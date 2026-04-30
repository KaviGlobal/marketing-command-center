"""Production logging configuration for the blob ingestion worker.

Provides:
  * Rotating human-readable log file (``ingestion.log``)
  * Rotating errors-only log file (``ingestion-errors.log``) for quick triage
  * Rotating machine-parseable JSON Lines file (``ingestion.jsonl``) suitable
    for log shippers such as Azure Log Analytics, Datadog, or Splunk
  * Optional console handler for interactive runs
  * Run-scoped correlation fields (``run_id``, ``blob_path``, ...) injected
    into every record via ``set_run_context`` / ``update_run_context``
  * ``install_excepthook`` to capture any uncaught exception with a full
    traceback before the process exits

All configuration is driven by environment variables so operators can tune
behavior without code changes:

    INGESTION_LOG_DIR              Directory for log files. Default: ./logs
    INGESTION_LOG_LEVEL            Root log level. Default: INFO
    INGESTION_LOG_CONSOLE_ENABLED  Write to stderr too. Default: true
    INGESTION_LOG_JSON_ENABLED     Emit ``ingestion.jsonl``. Default: true
    INGESTION_LOG_MAX_BYTES        Size per file before rotation. Default: 10485760
    INGESTION_LOG_BACKUP_COUNT     Rotated backups retained. Default: 7
    INGESTION_LOG_ERROR_LEVEL      Threshold for the errors file. Default: WARNING
    INGESTION_LOG_AUTO_SETUP       Auto-configure on first import. Default: true

The module is safe to import and call ``setup_logging`` multiple times; the
second and subsequent invocations are ignored unless ``force=True``.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
import traceback
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# Run-scoped context fields (e.g. run_id, blob_path). Using a ContextVar keeps
# the pattern compatible with any future async callers while remaining trivial
# to use from the current single-threaded CLI worker.
_run_context: ContextVar[Dict[str, Any]] = ContextVar("ingestion_run_context", default={})

# Reserved LogRecord attributes that must never be overwritten via `extra=`
# or by the RunContextFilter; the logging framework raises KeyError if we do.
_RESERVED_RECORD_ATTRS = frozenset({
    "args", "asctime", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelname", "levelno", "lineno", "message", "module",
    "msecs", "msg", "name", "pathname", "process", "processName",
    "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    "getMessage",
})


def get_run_context() -> Dict[str, Any]:
    """Return a snapshot of the current run context."""
    return dict(_run_context.get())


def set_run_context(**fields: Any) -> None:
    """Replace the current run context with the given fields."""
    _run_context.set(dict(fields))


def update_run_context(**fields: Any) -> None:
    """Merge fields into the current run context."""
    current = dict(_run_context.get())
    current.update(fields)
    _run_context.set(current)


def clear_run_context() -> None:
    """Reset the run context to empty."""
    _run_context.set({})


class RunContextFilter(logging.Filter):
    """Attach the current run context to every log record.

    Fields are added as attributes on the LogRecord (not via `extra=`) so the
    JSON formatter can pick them up and the text formatter can ignore them.
    We never overwrite an attribute the record already owns.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - trivial
        context = _run_context.get()
        if context:
            for key, value in context.items():
                if key in _RESERVED_RECORD_ATTRS:
                    continue
                if not hasattr(record, key):
                    setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    """Serialize each LogRecord as a single JSON line.

    Captures caller-provided extras, exception info, and run context so log
    aggregators can index by ``run_id``, ``blob_path``, ``stage``, etc.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }

        context = _run_context.get()
        if context:
            payload["context"] = dict(context)

        # Surface caller-provided extras attached to the record by `extra=` or
        # by the RunContextFilter. Skip private attributes and framework fields
        # we already handled above.
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_ATTRS or key in payload or key == "context":
                continue
            if key.startswith("_"):
                continue
            try:
                # Probe serializability so we don't blow up a whole line because
                # of one exotic object; fall back to repr if json cannot handle it.
                json.dumps(value, default=str)
                payload[key] = value
            except (TypeError, ValueError):
                payload[key] = repr(value)

        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            payload["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value) if exc_value else None,
                "traceback": "".join(traceback.format_exception(exc_type, exc_value, exc_tb)),
            }
        elif record.exc_text:
            payload["exception"] = {"traceback": record.exc_text}

        if record.stack_info:
            payload["stack"] = record.stack_info

        return json.dumps(payload, default=str, ensure_ascii=False)


def _resolve_level(name: Optional[str], default: int) -> int:
    """Translate a level name like ``INFO`` into its numeric logging level."""
    if not name:
        return default
    value = logging.getLevelName(name.strip().upper())
    return value if isinstance(value, int) else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


_logging_configured = False


def setup_logging(force: bool = False) -> Dict[str, Any]:
    """Configure the root logger with rotating file + console handlers.

    Idempotent. Returns a dict describing the effective configuration so
    callers can surface it to operators if desired.
    """
    global _logging_configured
    if _logging_configured and not force:
        return {"already_configured": True}

    requested_dir = (os.environ.get("INGESTION_LOG_DIR") or "logs").strip() or "logs"
    try:
        os.makedirs(requested_dir, exist_ok=True)
        log_dir = requested_dir
    except OSError:
        # Read-only filesystems (e.g. certain Function hosts) or permission
        # issues should not take down the process; degrade gracefully to the
        # current working directory and keep going.
        log_dir = "."

    root_level = _resolve_level(os.environ.get("INGESTION_LOG_LEVEL"), logging.INFO)
    error_level = _resolve_level(os.environ.get("INGESTION_LOG_ERROR_LEVEL"), logging.WARNING)
    console_enabled = _env_bool("INGESTION_LOG_CONSOLE_ENABLED", True)
    json_enabled = _env_bool("INGESTION_LOG_JSON_ENABLED", True)
    max_bytes = _env_int("INGESTION_LOG_MAX_BYTES", 10 * 1024 * 1024, minimum=1024)
    backup_count = _env_int("INGESTION_LOG_BACKUP_COUNT", 7, minimum=1)

    root_logger = logging.getLogger()
    # Keep the root logger permissive so handlers can filter independently.
    # Individual handlers enforce their own thresholds below.
    root_logger.setLevel(min(root_level, logging.DEBUG))

    # Remove any handlers from a previous basicConfig / setup call so log
    # lines don't duplicate across handlers.
    for existing in list(root_logger.handlers):
        root_logger.removeHandler(existing)

    context_filter = RunContextFilter()
    text_formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    json_formatter = JsonFormatter()

    installed: Dict[str, Any] = {
        "log_dir": os.path.abspath(log_dir),
        "level": logging.getLevelName(root_level),
        "error_level": logging.getLevelName(error_level),
        "console_enabled": console_enabled,
        "json_enabled": json_enabled,
        "max_bytes": max_bytes,
        "backup_count": backup_count,
        "handlers": [],
    }

    if console_enabled:
        console_handler = logging.StreamHandler(stream=sys.stderr)
        console_handler.setLevel(root_level)
        console_handler.setFormatter(text_formatter)
        console_handler.addFilter(context_filter)
        root_logger.addHandler(console_handler)
        installed["handlers"].append("console")

    main_path = os.path.join(log_dir, "ingestion.log")
    main_handler = logging.handlers.RotatingFileHandler(
        main_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
    )
    main_handler.setLevel(root_level)
    main_handler.setFormatter(text_formatter)
    main_handler.addFilter(context_filter)
    root_logger.addHandler(main_handler)
    installed["handlers"].append(main_path)

    error_path = os.path.join(log_dir, "ingestion-errors.log")
    error_handler = logging.handlers.RotatingFileHandler(
        error_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
    )
    error_handler.setLevel(error_level)
    error_handler.setFormatter(text_formatter)
    error_handler.addFilter(context_filter)
    root_logger.addHandler(error_handler)
    installed["handlers"].append(error_path)

    if json_enabled:
        json_path = os.path.join(log_dir, "ingestion.jsonl")
        json_handler = logging.handlers.RotatingFileHandler(
            json_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
        )
        # JSON file captures every record so downstream tooling can filter.
        json_handler.setLevel(logging.DEBUG)
        json_handler.setFormatter(json_formatter)
        json_handler.addFilter(context_filter)
        root_logger.addHandler(json_handler)
        installed["handlers"].append(json_path)

    # Azure SDK and HTTP libraries are extremely chatty at DEBUG/INFO. Quiet
    # them down so our logs focus on ingestion-level events.
    for noisy in ("azure", "azure.core", "azure.storage", "urllib3", "requests", "msal"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _logging_configured = True

    logging.getLogger("ingestion.logging").info(
        "Ingestion logging initialized",
        extra={
            "log_dir": installed["log_dir"],
            "level": installed["level"],
            "json_enabled": json_enabled,
            "console_enabled": console_enabled,
        },
    )
    return installed


def install_excepthook() -> None:
    """Route any uncaught exception through logging before the process exits.

    Preserves normal ``KeyboardInterrupt`` behavior so ``Ctrl+C`` still prints
    the usual traceback on the console.
    """
    previous_hook = sys.excepthook

    def _hook(exc_type, exc_value, tb) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            previous_hook(exc_type, exc_value, tb)
            return
        logging.getLogger("ingestion.excepthook").critical(
            "Uncaught exception terminated the process",
            exc_info=(exc_type, exc_value, tb),
        )

    sys.excepthook = _hook


def log_event(stage: str, level: int = logging.INFO, **context: Any) -> None:
    """Emit a structured ingestion event.

    The stage name is placed into the record's message AND captured as a
    structured field so human log viewers and JSON consumers both see it.
    Keys that would collide with reserved LogRecord attributes are dropped to
    avoid the ``Attempt to overwrite %r in LogRecord`` KeyError.
    """
    safe_context = {
        key: value
        for key, value in context.items()
        if key not in _RESERVED_RECORD_ATTRS and not key.startswith("_")
    }
    extra = {"stage": stage, **safe_context}
    logging.getLogger("ingestion.event").log(level, "ingestion_event stage=%s", stage, extra=extra)


# Auto-setup on import so any module-level logging call in the worker lands
# in our files, not a default stderr-only basicConfig. Can be disabled by
# setting INGESTION_LOG_AUTO_SETUP=false for test harnesses or library use.
if _env_bool("INGESTION_LOG_AUTO_SETUP", True):
    try:
        setup_logging()
    except Exception:  # pragma: no cover - defensive, never break import
        # Fall back to Python's default logging so the caller still gets *some*
        # output even if our setup failed for an unexpected reason.
        logging.basicConfig(level=logging.INFO)
        logging.getLogger(__name__).exception("Failed to initialize ingestion logging; using basicConfig fallback")
