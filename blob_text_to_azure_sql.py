import argparse
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

# This script converts blob text into data records and writes them to Azure SQL.
# Common validation rules are imported from shared_validation.py.
# The script also supports local.settings.json for local CLI execution.
#
# This file is used for batch ingestion from blob storage rather than the Azure Function HTTP endpoints.

try:
    import pyodbc  # type: ignore[import]
except Exception:  # pragma: no cover - local runtime dependency may be absent
    pyodbc = None  # type: ignore[assignment]
try:
    import pytds  # type: ignore[import]
except ImportError:  # pragma: no cover - optional local fallback
    pytds = None  # type: ignore[assignment]
try:
    import certifi  # type: ignore[import]
except ImportError:  # pragma: no cover - optional TLS helper
    certifi = None  # type: ignore[assignment]
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobClient, BlobServiceClient, ContentSettings  # type: ignore[import]

# Centralized production logging (rotating files, JSON lines, run-scoped context).
# Importing this module triggers setup_logging() unless
# INGESTION_LOG_AUTO_SETUP=false is set, so all logging calls below land in the
# configured files without needing any per-call boilerplate.
from ingestion_logging import (
    clear_run_context,
    install_excepthook,
    log_event as _structured_log_event,
    set_run_context,
    setup_logging,
    update_run_context,
)

from shared_validation import (
    ALLOWED_KPI_FIELDS,
    canonical_field_name,
    get_offering_category,
    normalize_bool,
    normalize_flow_type,
    normalize_kpi_field_value,
    normalize_offering_name,
    normalize_text,
    validate_field_name as shared_validate_field_name,
    validate_field_value as shared_validate_field_value,
    validate_session_id as shared_validate_session_id,
)

WORKER_ALLOWED_KPI_FIELDS = set(ALLOWED_KPI_FIELDS) | {
    "lead_phone",
    "lead_industry",
    "lead_job_title",
}

PROSPECT_DETAIL_FIELDS = [
    "lead_capture_started_flag", "lead_capture_completed_flag", "lead_name", "lead_email", "lead_company",
    "lead_phone", "lead_industry", "lead_job_title", "consultation_requested_flag",
    "scheduler_link_clicked_flag", "offering_primary", "offering_secondary", "intent_primary",
]
CAREER_DETAIL_FIELDS = [
    "application_intent_flag", "candidate_capture_started_flag", "candidate_capture_completed_flag",
    "candidate_name", "candidate_email", "job_interest_area", "job_interest_location",
]
PARTNERSHIP_DETAIL_FIELDS = [
    "partner_capture_started_flag", "partner_capture_completed_flag", "partner_name", "partner_org_name",
    "partner_email", "partner_type", "partner_consultation_requested_flag", "partner_consultation_booked_flag",
]


def load_local_settings_env_defaults() -> None:
    """Load local.settings.json Values into env if a key is currently unset.

    This keeps local CLI runs aligned with Function host settings while
    avoiding placeholder values like <your-password>.
    """
    # Look for a local.settings.json file in the current working directory first,
    # then in the script directory. This keeps local CLI behavior aligned with Azure Functions
    # and avoids duplicate config files.
    candidates = [
        os.path.join(os.getcwd(), "local.settings.json"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "local.settings.json"),
    ]
    settings_path = next((path for path in candidates if os.path.isfile(path)), None)
    if not settings_path:
        return

    try:
        with open(settings_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return

    values = payload.get("Values") if isinstance(payload, dict) else None
    if not isinstance(values, dict):
        return

    for key, value in values.items():
        # Only load defaults for environment variables that are not already set.
        if key in os.environ or value is None:
            continue
        text = str(value).strip()
        if not text or (text.startswith("<") and text.endswith(">")):
            continue
        # Avoid plugging placeholder values like <your-password> into env.
        os.environ[key] = text


load_local_settings_env_defaults()

# Support both Azure Function App default (AzureWebJobsStorage) and custom env var.
BLOB_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING") or os.environ.get("AzureWebJobsStorage")
if not BLOB_CONNECTION_STRING:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING or AzureWebJobsStorage environment variable is required")
# Container where session blobs are stored for batch ingestion (Power Automate writes to "session-logs").
BLOB_CONTAINER = os.environ.get("SESSION_LOG_CONTAINER", "session-logs")
BLOB_PREFIX = os.environ.get("SESSION_LOG_BLOB_PREFIX", "")
# Optional compatibility mode: also ingest legacy "one-blob-per-field" payloads
# written directly to blob storage by Power Automate (typically container "session-logs").
#
# If multiple sources are scanned in one run, the logical blob_path persisted to SQL
# will include the container name to avoid collisions across containers.
COMPAT_LEGACY_ENABLED = os.environ.get("SESSION_LOG_COMPAT_LEGACY_ENABLED", "true").lower() in {"true", "1", "yes"}
LEGACY_CONTAINER = os.environ.get("SESSION_LOG_LEGACY_CONTAINER", "session-logs")
LEGACY_PREFIX = os.environ.get("SESSION_LOG_LEGACY_PREFIX", "").lstrip("/")
if LEGACY_PREFIX and not LEGACY_PREFIX.endswith("/"):
    LEGACY_PREFIX += "/"
SOURCE_CONTAINERS_ENV = os.environ.get("SESSION_LOG_SOURCE_CONTAINERS", "")
SOURCE_PREFIXES_ENV = os.environ.get("SESSION_LOG_SOURCE_PREFIXES", "")
FAILED_FIELDNAMES_CONTAINER = os.environ.get("FAILED_FIELDNAMES_CONTAINER")
FAILED_FIELDNAMES_BLOB_PREFIX = os.environ.get("FAILED_FIELDNAMES_BLOB_PREFIX", "failed-fieldnames/")
# Support both Azure Function App default (SQL_CONNECTION_STRING) and custom env var.
AZURE_SQL_CONN_STR = os.environ.get("AZURE_SQL_CONN_STR") or os.environ.get("SQL_CONNECTION_STRING")
SQL_SERVER = os.environ.get("AZURE_SQL_SERVER")
SQL_DATABASE = os.environ.get("AZURE_SQL_DATABASE")
SQL_USER = os.environ.get("AZURE_SQL_USER")
SQL_PASSWORD = os.environ.get("AZURE_SQL_PASSWORD")
SQL_TRUST_SERVER_CERTIFICATE = os.environ.get("AZURE_SQL_TRUST_SERVER_CERTIFICATE", "no").lower() in {"yes", "true", "1"}
# Retry and backoff settings used for SQL connectivity and execution.
SQL_CONNECT_RETRY = int(os.environ.get("AZURE_SQL_CONNECT_RETRY", "3"))
SQL_CONNECT_BACKOFF = float(os.environ.get("AZURE_SQL_CONNECT_BACKOFF", "0.5"))
SQL_EXEC_RETRY = int(os.environ.get("AZURE_SQL_EXEC_RETRY", "3"))
DEAD_LETTER_CONTAINER = os.environ.get("DEAD_LETTER_CONTAINER")
INGESTION_FAILURE_ALERT_THRESHOLD = int(os.environ.get("INGESTION_FAILURE_ALERT_THRESHOLD", "5"))
DEAD_LETTER_DELETE_SOURCE = os.environ.get("DEAD_LETTER_DELETE_SOURCE", "false").lower() in {"yes", "true", "1"}
INGESTION_DELETE_AFTER_SUCCESS = os.environ.get("INGESTION_DELETE_AFTER_SUCCESS", "false").lower() in {"yes", "true", "1"}
INGEST_DEV_BLOBS = os.environ.get("INGEST_DEV_BLOBS", "false").lower() in {"yes", "true", "1"}
IGNORE_NORMALIZED_UPSERT_ERRORS = os.environ.get("IGNORE_NORMALIZED_UPSERT_ERRORS", "true").lower() in {"yes", "true", "1"}

# Ensure production logging is active and unhandled exceptions are captured.
# setup_logging() is also invoked at ingestion_logging import time; calling it
# here is idempotent and guarantees configuration even if auto-setup was disabled.
setup_logging()
install_excepthook()

RETRY_STATS = {
    "sql_connect_retries": 0,
    "sql_execute_retries": 0,
    "sql_executemany_retries": 0,
}
ACTIVE_SQL_BACKEND = "pyodbc"


def log_ingestion_event(stage: str, level: int = logging.INFO, **context: Any) -> None:
    """Emit a structured ingestion event through the production logging pipeline.

    The event is written to every configured handler: the human-readable log
    file, the errors file (when ``level >= INGESTION_LOG_ERROR_LEVEL``), and
    the machine-parseable ``ingestion.jsonl`` file. The current run context
    (run_id, blob_path, etc.) is injected automatically by the log filter.
    """
    _structured_log_event(stage, level=level, component="blob_ingestion", **context)


def get_int_env(name: str, default: int, minimum: int = 0) -> int:
    """Get int env."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        logging.warning("Invalid int for %s=%r. Using default %s", name, raw, default)
        return default
    if value < minimum:
        logging.warning("Out-of-range int for %s=%s (min %s). Using default %s", name, value, minimum, default)
        return default
    return value


def get_csv_env(name: str, default: List[str]) -> List[str]:
    """Get a comma-separated list environment variable."""
    raw = os.environ.get(name)
    if raw is None:
        return [item for item in default if item]
    return [item.strip() for item in raw.split(",") if item.strip()]


def get_bool_env(name: str, default: bool) -> bool:
    """Get bool env."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    logging.warning("Invalid bool for %s=%r. Using default %s", name, raw, default)
    return default


INGESTION_LIST_PAGE_SIZE = get_int_env("INGESTION_LIST_PAGE_SIZE", 500, minimum=1)
INGESTION_EXCLUDED_PREFIXES = get_csv_env(
    "SESSION_LOG_EXCLUDED_PREFIXES",
    [FAILED_FIELDNAMES_BLOB_PREFIX, "session-path-index/", "deadletter/"],
)
FACT_ROW_DEDUPE_ENABLED = get_bool_env("FACT_ROW_DEDUPE_ENABLED", True)
KPI_AGGREGATE_REFRESH_ENABLED = get_bool_env("KPI_AGGREGATE_REFRESH_ENABLED", True)
KPI_AGGREGATE_REFRESH_FAIL_ON_ERROR = get_bool_env("KPI_AGGREGATE_REFRESH_FAIL_ON_ERROR", False)
KPI_AGGREGATE_REFRESH_FULL = get_bool_env("KPI_AGGREGATE_REFRESH_FULL", False)
KPI_AGGREGATE_REFRESH_LOOKBACK_DAYS = get_int_env("KPI_AGGREGATE_REFRESH_LOOKBACK_DAYS", 30, minimum=0)


def parse_connection_string(raw: str) -> Dict[str, str]:
    """Parse a semicolon-delimited connection string into lowercase keys."""
    pairs: Dict[str, str] = {}
    for segment in raw.split(";"):
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        pairs[key.strip().lower()] = value.strip()
    return pairs


def strip_placeholder_braces(value: Optional[str]) -> Optional[str]:
    """Strip Azure portal placeholder braces from credential values."""
    if value is None:
        return None
    text = value.strip()
    if len(text) >= 2 and text.startswith("{") and text.endswith("}"):
        return text[1:-1]
    return text


def split_server_and_port(server: str) -> Tuple[str, int]:
    """Split a SQL Server host string into host and port."""
    text = server.strip()
    if text.lower().startswith("tcp:"):
        text = text[4:]
    port = 1433
    if "," in text:
        host, port_text = text.rsplit(",", 1)
        if port_text.isdigit():
            return host, int(port_text)
        return text, port
    if ":" in text and text.count(":") == 1:
        host, port_text = text.rsplit(":", 1)
        if port_text.isdigit():
            return host, int(port_text)
    return text, port


def get_sql_connection_settings() -> Dict[str, Any]:
    """Resolve SQL connection settings from env vars or the embedded ADO.NET string."""
    raw_connection_string = AZURE_SQL_CONN_STR or None
    if raw_connection_string:
        pairs = parse_connection_string(raw_connection_string)
        server_value = pairs.get("server") or pairs.get("data source") or ""
        database = pairs.get("initial catalog") or pairs.get("database")
        user = pairs.get("user id") or pairs.get("uid")
        password = strip_placeholder_braces(pairs.get("password") or pairs.get("pwd"))
        encrypt = pairs.get("encrypt", "True").lower() in {"true", "yes", "1"}
        trust_server_certificate = pairs.get("trustservercertificate", "False").lower() in {"true", "yes", "1"}
        timeout = int(float(pairs.get("connection timeout") or pairs.get("timeout") or "30"))
        host, port = split_server_and_port(server_value)
        if host and database and user and password:
            return {
                "server": host,
                "port": port,
                "database": database,
                "user": user,
                "password": password,
                "encrypt": encrypt,
                "trust_server_certificate": trust_server_certificate,
                "timeout": timeout,
                "raw_connection_string": raw_connection_string,
            }

    missing = [name for name, value in [
        ("AZURE_SQL_SERVER", SQL_SERVER),
        ("AZURE_SQL_DATABASE", SQL_DATABASE),
        ("AZURE_SQL_USER", SQL_USER),
        ("AZURE_SQL_PASSWORD", SQL_PASSWORD),
    ] if not value]
    if missing:
        raise ValueError(f"Missing SQL connection environment variables: {', '.join(missing)}")

    host, port = split_server_and_port(str(SQL_SERVER))
    return {
        "server": host,
        "port": port,
        "database": SQL_DATABASE,
        "user": SQL_USER,
        "password": strip_placeholder_braces(SQL_PASSWORD),
        "encrypt": True,
        "trust_server_certificate": SQL_TRUST_SERVER_CERTIFICATE,
        "timeout": 30,
        "raw_connection_string": None,
    }


def build_sql_connection_string() -> str:
    """Build an Azure SQL ODBC connection string from environment values."""
    settings = get_sql_connection_settings()
    raw_connection_string = settings.get("raw_connection_string")
    if raw_connection_string and "driver=" in raw_connection_string.lower():
        return str(raw_connection_string)

    trust_value = "yes" if settings["trust_server_certificate"] else "no"
    return (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{settings['server']},{settings['port']};"
        f"Database={settings['database']};"
        f"Uid={settings['user']};"
        f"Pwd={settings['password']};"
        f"Encrypt={'yes' if settings['encrypt'] else 'no'};"
        f"TrustServerCertificate={trust_value};"
        f"Connection Timeout={settings['timeout']};"
    )


def adapt_query_for_backend(query: str) -> str:
    """Convert qmark SQL parameters when using a non-ODBC backend."""
    if ACTIVE_SQL_BACKEND == "pytds":
        return query.replace("?", "%s")
    return query

# Module-level caches to avoid re-creating clients and re-issuing create_container
# calls for every blob processed. Clients are reused across the entire run, which
# amortizes HTTP connection pools and removes redundant network round-trips.
_BLOB_SERVICE_CLIENT: Optional[BlobServiceClient] = None
_CONTAINER_CLIENTS: Dict[str, Any] = {}
_CONTAINERS_ENSURED: set = set()


def reset_blob_client_cache() -> None:
    """Drop cached Azure blob clients. Intended for tests that swap connection strings."""
    global _BLOB_SERVICE_CLIENT
    _BLOB_SERVICE_CLIENT = None
    _CONTAINER_CLIENTS.clear()
    _CONTAINERS_ENSURED.clear()


def get_blob_service_client() -> BlobServiceClient:
    """Return a cached Azure BlobServiceClient for the configured connection string."""
    global _BLOB_SERVICE_CLIENT
    if not BLOB_CONNECTION_STRING:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is required")
    if _BLOB_SERVICE_CLIENT is None:
        _BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    return _BLOB_SERVICE_CLIENT


def get_cached_container_client(container_name: str):
    """Return a cached ContainerClient for the given container name."""
    client = _CONTAINER_CLIENTS.get(container_name)
    if client is None:
        client = get_blob_service_client().get_container_client(container_name)
        _CONTAINER_CLIENTS[container_name] = client
    return client


def ensure_container_exists(container_name: str) -> None:
    """Attempt to create the container at most once per process."""
    if container_name in _CONTAINERS_ENSURED:
        return
    container = get_cached_container_client(container_name)
    try:
        container.create_container()
    except AzureError:
        # The container already exists or cannot be created with current credentials;
        # either way, subsequent writes will surface a clearer error if needed.
        pass
    _CONTAINERS_ENSURED.add(container_name)


def get_dead_letter_container_client():
    """Get dead letter container client."""
    if not DEAD_LETTER_CONTAINER:
        return None
    ensure_container_exists(DEAD_LETTER_CONTAINER)
    return get_cached_container_client(DEAD_LETTER_CONTAINER)


def load_blob_text(blob_client: BlobClient) -> str:
    """Download blob content and return it as UTF-8 text."""
    try:
        downloader = blob_client.download_blob()
        data = downloader.readall()
        # Convert blob payload bytes into UTF-8 text for ingestion.
        if isinstance(data, bytes):
            # Be resilient to occasional invalid UTF-8 bytes; preserve as much text as possible.
            return data.decode("utf-8", errors="replace")
        return str(data)
    except AzureError as exc:
        logging.error("Failed to download blob text for %s: %s", blob_client.blob_name, exc)
        raise


def get_failed_fieldnames_container_client():
    """Get or create the container used to persist field name rejection metadata."""
    container_name = FAILED_FIELDNAMES_CONTAINER or BLOB_CONTAINER
    ensure_container_exists(container_name)
    return get_cached_container_client(container_name)


def get_failed_fieldnames_blob_client(source_blob_path: str) -> BlobClient:
    """Get failed fieldnames blob client."""
    container = get_failed_fieldnames_container_client()
    # Use a separate blob prefix to store rejection metadata for a source blob.
    return container.get_blob_client(f"{FAILED_FIELDNAMES_BLOB_PREFIX}{source_blob_path}")


def get_failed_field_names_from_rejections(rejected_rows: List[Tuple[Any, ...]]) -> List[str]:
    """Extract unique failed field names from rejection records."""
    # Each rejection row stores the field name in the third tuple position.
    return sorted({str(row[2]) for row in rejected_rows if row[2]})


def write_failed_fieldnames_blob(source_blob_path: str, field_names: List[str]) -> None:
    """Write failed fieldnames blob."""
    blob_client = get_failed_fieldnames_blob_client(source_blob_path)
    if not field_names:
        # If there are no failed fields, delete any existing failure marker blob.
        try:
            blob_client.delete_blob()
        except ResourceNotFoundError:
            pass
        return

    payload = json.dumps({
        "sourceBlobPath": source_blob_path,
        "failedFieldNames": field_names,
        "recordedAtUtc": datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False)
    blob_client.upload_blob(
        payload.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )


def parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse an ISO formatted timestamp string into a datetime object."""
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            # Support UTC timestamps with Z suffix by converting to +00:00 offset.
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def connect_with_retry() -> Any:
    """Retry SQL connection attempts according to configured retry settings."""
    global ACTIVE_SQL_BACKEND
    last_exc: Optional[Exception] = None
    for attempt in range(1, SQL_CONNECT_RETRY + 1):
        try:
            if pyodbc is not None:
                drivers = pyodbc.drivers()
                if drivers:
                    ACTIVE_SQL_BACKEND = "pyodbc"
                    return pyodbc.connect(build_sql_connection_string(), autocommit=False)
            if pytds is not None:
                settings = get_sql_connection_settings()
                ACTIVE_SQL_BACKEND = "pytds"
                cafile = certifi.where() if certifi is not None and settings["encrypt"] else None
                return pytds.connect(
                    server=settings["server"],
                    port=settings["port"],
                    database=settings["database"],
                    user=settings["user"],
                    password=settings["password"],
                    timeout=float(settings["timeout"]),
                    login_timeout=float(settings["timeout"]),
                    autocommit=False,
                    cafile=cafile,
                    validate_host=not settings["trust_server_certificate"],
                    enc_login_only=not settings["encrypt"],
                    disable_connect_retry=True,
                )
            raise RuntimeError("No usable SQL client is available. Install an ODBC driver or python-tds.")
        except Exception as exc:
            last_exc = exc
            if attempt > 1:
                RETRY_STATS["sql_connect_retries"] += 1
            logging.warning("SQL connect attempt %d failed: %s", attempt, exc)
            if attempt < SQL_CONNECT_RETRY:
                # Exponential backoff between retry attempts.
                time.sleep(SQL_CONNECT_BACKOFF * attempt)
    raise last_exc if last_exc is not None else RuntimeError("SQL connection retry failed")


def execute_with_retry(cursor: Any, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
    """Execute a single SQL statement with retry on transient ODBC errors."""
    last_exc: Optional[Exception] = None
    sql = adapt_query_for_backend(query)
    for attempt in range(1, SQL_EXEC_RETRY + 1):
        try:
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            return
        except Exception as exc:
            last_exc = exc
            if attempt > 1:
                RETRY_STATS["sql_execute_retries"] += 1
            logging.warning("SQL execute attempt %d failed: %s", attempt, exc)
            if attempt < SQL_EXEC_RETRY:
                # Retry transient SQL execution failures after backoff.
                time.sleep(SQL_CONNECT_BACKOFF * attempt)
    raise last_exc if last_exc is not None else RuntimeError("SQL execution retry failed")


def executemany_with_retry(cursor: Any, query: str, rows: List[Tuple[Any, ...]]) -> None:
    """Execute a multi-row insert/update SQL statement with retry on transient errors."""
    if not rows:
        return
    last_exc: Optional[Exception] = None
    sql = adapt_query_for_backend(query)
    for attempt in range(1, SQL_EXEC_RETRY + 1):
        try:
            cursor.executemany(sql, rows)
            return
        except Exception as exc:
            last_exc = exc
            if attempt > 1:
                RETRY_STATS["sql_executemany_retries"] += 1
            logging.warning("SQL executemany attempt %d failed: %s", attempt, exc)
            if attempt < SQL_EXEC_RETRY:
                # Retry bulk inserts when the database is temporarily unavailable.
                time.sleep(SQL_CONNECT_BACKOFF * attempt)
    raise last_exc if last_exc is not None else RuntimeError("SQL executemany retry failed")


def validate_session_id(session_id: Any) -> Tuple[bool, str]:
    """Validate that the provided session ID is normalized and valid."""
    return shared_validate_session_id(normalize_text(session_id))


def validate_field_name(field_name: Any) -> Tuple[bool, str]:
    """Validate field name."""
    return shared_validate_field_name(normalize_text(field_name), is_kpi=False)


def validate_kpi_field_name(field_name: Any) -> Tuple[bool, str]:
    """Validate KPI field name (enforces KPI whitelist)."""
    return shared_validate_field_name(normalize_text(field_name), is_kpi=True)


def validate_field_value(field_name: str, field_value: Any, is_kpi: bool) -> Tuple[bool, str]:
    """Validate a fieldValue after normalizing text and applying KPI rules."""
    return shared_validate_field_value(field_name, normalize_text(field_value), is_kpi)


def validate_session_payload(payload: Any) -> Tuple[Optional[str], List[str]]:
    """Validate session payload."""
    errors: List[str] = []
    if not isinstance(payload, dict):
        return None, ["Top-level blob JSON must be a JSON object"]

    # Validate required session metadata before any table-specific ingestion.
    session_id = normalize_text(payload.get("sessionId"))
    ok, reason = validate_session_id(session_id)
    if not ok:
        errors.append(reason)

    if payload.get("responses") is not None and not isinstance(payload["responses"], dict):
        errors.append("responses must be an object")
    if payload.get("kpis") is not None and not isinstance(payload["kpis"], dict):
        errors.append("kpis must be an object")
    if payload.get("events") is not None and not isinstance(payload["events"], list):
        errors.append("events must be an array")

    for key in ("timestamp", "createdAtUtc", "lastUpdatedUtc"):
        if payload.get(key) is not None and parse_timestamp(payload.get(key)) is None:
            errors.append(f"{key} must be a valid datetime string")

    # Validate top-level KPI fields against the known KPI whitelist.
    kpis = payload.get("kpis")
    if isinstance(kpis, dict):
        for field_name in kpis.keys():
            if field_name not in WORKER_ALLOWED_KPI_FIELDS:
                errors.append(f"Unsupported KPI fieldName: {field_name}")

    return session_id if session_id else None, errors


def get_dev_flag(payload: Dict[str, Any]) -> str:
    """Return the normalized devFlag value."""
    return normalize_text(payload.get("devFlag")).lower()


def normalize_int(value: Any) -> Optional[int]:
    """Normalize int."""
    if value is None:
        return None
    text = normalize_text(value)
    return int(text) if re.fullmatch(r"-?\d+", text) else None


def normalize_time(value: Any) -> Optional[datetime]:
    """Normalize time."""
    return parse_timestamp(value)


def normalize_bool_with_default(value: Any, default: int = 0) -> int:
    """Normalize a boolean-like value, falling back to a numeric default when absent."""
    normalized = normalize_bool(value)
    return default if normalized is None else normalized


def normalize_storage_value(field_name: str, field_value: Any, is_kpi: bool) -> str:
    """Normalize a field value into the canonical text stored in raw fact rows."""
    if is_kpi:
        normalized = normalize_kpi_field_value(field_name, field_value)
        return normalized or ""
    return normalize_text(field_value)


def get_kpi_value(payload: Dict[str, Any], field_name: str) -> Any:
    """Get normalized KPI value from payload aliases or canonical fields."""
    # Resolve both aliased and canonical representations for campaign fields.
    if field_name == "flow_type":
        if payload.get("flowType") is not None:
            return normalize_flow_type(payload.get("flowType"))
        if payload.get("kpiFlowType") is not None:
            return normalize_flow_type(payload.get("kpiFlowType"))
    if payload.get("fieldName") is not None:
        canonical = canonical_field_name(normalize_text(payload.get("fieldName")))
        if canonical == field_name:
            return normalize_kpi_field_value(field_name, payload.get("fieldValue"))
    # Legacy/alternate spellings for time keys.
    if field_name == "session_start_time":
        if payload.get("session_start_time_utc") is not None:
            return payload.get("session_start_time_utc")
        if payload.get("sessionStartTimeUtc") is not None:
            return payload.get("sessionStartTimeUtc")
    if field_name == "session_end_time_utc":
        if payload.get("sessionEndTimeUtc") is not None:
            return payload.get("sessionEndTimeUtc")
    kpis = payload.get("kpis")
    if isinstance(kpis, dict):
        return normalize_kpi_field_value(field_name, kpis.get(field_name))
    return None


def has_any_kpi_fields(payload: Dict[str, Any], keys: List[str]) -> bool:
    """Has any kpi fields."""
    return any(get_kpi_value(payload, key) is not None for key in keys)


def normalize_session_metadata(payload: Dict[str, Any]) -> Tuple[Any, ...]:
    """Normalize session metadata."""
    session = normalize_text(payload.get("sessionId"))
    user_value = payload.get("user")
    user: Dict[str, Any] = user_value if isinstance(user_value, dict) else {}
    timestamp = parse_timestamp(payload.get("createdAtUtc")) or parse_timestamp(payload.get("timestamp"))
    last_updated = parse_timestamp(payload.get("lastUpdatedUtc")) or parse_timestamp(payload.get("timestamp"))
    return (
        session,
        normalize_text(payload.get("botId")) or None,
        normalize_text(user.get("id")) or None,
        normalize_text(user.get("email")) or None,
        normalize_text(user.get("displayName")) or None,
        timestamp,
        last_updated,
    )


def upsert_session_record(cursor: Any, metadata: Tuple[Any, ...]) -> None:
    """Upsert session record."""
    # Use MERGE to insert a session row or update existing session metadata atomically.
    execute_with_retry(cursor,
        """
        MERGE session_blob_session AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS source (
            session_id, bot_id, user_id, user_email, user_display_name, created_at_utc, last_updated_utc
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                bot_id = COALESCE(source.bot_id, target.bot_id),
                user_id = COALESCE(source.user_id, target.user_id),
                user_email = COALESCE(source.user_email, target.user_email),
                user_display_name = COALESCE(source.user_display_name, target.user_display_name),
                created_at_utc = COALESCE(source.created_at_utc, target.created_at_utc),
                last_updated_utc = COALESCE(source.last_updated_utc, target.last_updated_utc)
        WHEN NOT MATCHED THEN
            INSERT (session_id, bot_id, user_id, user_email, user_display_name, created_at_utc, last_updated_utc)
            VALUES (source.session_id, source.bot_id, source.user_id, source.user_email, source.user_display_name, source.created_at_utc, source.last_updated_utc);
        """,
        metadata,
    )


def extract_approved_rows(blob_path: str, payload: Dict[str, Any]) -> Tuple[List[Tuple[Any, ...]], List[Tuple[Any, ...]]]:
    """Extract approved fact rows and rejection rows from the session payload."""
    session_id = normalize_text(payload.get("sessionId"))
    rows: List[Tuple[Any, ...]] = []
    rejections: List[Tuple[Any, ...]] = []
    # Lazily serialize the payload at most once; most blobs have zero rejections and
    # will skip the serialization entirely. Large payloads avoid repeated json.dumps calls.
    payload_json_cache: List[Optional[str]] = [None]

    def payload_json() -> str:
        if payload_json_cache[0] is None:
            payload_json_cache[0] = json.dumps(payload)
        return payload_json_cache[0]

    def append_rejection(field_name: Optional[str], reason: str, raw_text: Optional[str] = None) -> None:
        # Capture rejection details with the blob path and optional raw text payload.
        rejections.append((
            blob_path,
            session_id or None,
            normalize_text(field_name) or None,
            datetime.now(timezone.utc),
            reason,
            raw_text[:4000] if raw_text else None,
        ))

    def append_row(field_name: str, field_value: Any, is_kpi: bool, event_type: str, captured_at: Optional[datetime]) -> None:
        # Append a validated row for later insertion into SQL.
        rows.append((
            blob_path,
            session_id,
            field_name,
            normalize_storage_value(field_name, field_value, is_kpi),
            1 if is_kpi else 0,
            event_type,
            captured_at,
        ))

    def best_payload_timestamp() -> Optional[datetime]:
        # Prefer canonical writer timestamps; fall back to legacy 'timestamp' if present.
        return (
            parse_timestamp(payload.get("lastUpdatedUtc"))
            or parse_timestamp(payload.get("createdAtUtc"))
            or parse_timestamp(payload.get("timestamp"))
        )

    # If flowType/kpiFlowType is present separately from a fieldName record,
    # treat it as a kpi field with the canonical flow_type name.
    flow_type_value = payload.get("flowType") if payload.get("flowType") is not None else payload.get("kpiFlowType")
    if flow_type_value is not None and canonical_field_name(normalize_text(payload.get("fieldName"))) != "flow_type":
        field_name = "flow_type"
        field_value = flow_type_value
        is_kpi = True
        captured_at = best_payload_timestamp()
        ok, reason = validate_kpi_field_name(field_name)
        if not ok:
            append_rejection(field_name, reason, payload_json())
        else:
            ok, reason = validate_field_value(field_name, field_value, is_kpi)
            if not ok:
                append_rejection(field_name, reason, payload_json())
            else:
                append_row(field_name, field_value, is_kpi, "kpi", captured_at)

    # Promote a top-level fieldName/fieldValue entry into an event row.
    if payload.get("fieldName") is not None:
        raw_field_name = normalize_text(payload.get("fieldName"))
        field_name = canonical_field_name(raw_field_name)
        field_value = payload.get("fieldValue")
        is_kpi = field_name in WORKER_ALLOWED_KPI_FIELDS
        captured_at = parse_timestamp(payload.get("capturedAtUtc")) or best_payload_timestamp()
        ok, reason = validate_kpi_field_name(field_name) if is_kpi else validate_field_name(field_name)
        if not ok:
            append_rejection(field_name, reason, payload_json())
        else:
            ok, reason = validate_field_value(field_name, field_value, is_kpi)
            if not ok:
                append_rejection(field_name, reason, payload_json())
            else:
                append_row(field_name, field_value, is_kpi, "event", captured_at)

    for section, is_kpi in [("responses", False), ("kpis", True)]:
        value = payload.get(section)
        if isinstance(value, dict):
            for field_name, field_value in value.items():
                ok, reason = validate_kpi_field_name(field_name) if is_kpi else validate_field_name(field_name)
                if not ok:
                    append_rejection(field_name, reason)
                    continue
                if is_kpi and field_name not in WORKER_ALLOWED_KPI_FIELDS:
                    append_rejection(field_name, f"Unsupported KPI fieldName: {field_name}")
                    continue
                ok, reason = validate_field_value(field_name, field_value, is_kpi)
                if not ok:
                    append_rejection(field_name, reason)
                    continue
                append_row(field_name, field_value, is_kpi, section, best_payload_timestamp())

    # Process any explicit events list, validating each event object independently.
    events = payload.get("events")
    if isinstance(events, list):
        for index, event in enumerate(events):
            if not isinstance(event, dict):
                append_rejection(None, f"events[{index}] must be an object", json.dumps(event) if event is not None else None)
                continue
            field_name = normalize_text(event.get("fieldName"))
            field_value = event.get("fieldValue")
            is_kpi_value = normalize_bool(event.get("isKpi", False))
            if is_kpi_value is None and normalize_text(event.get("isKpi")):
                append_rejection(field_name or None, "Invalid boolean value for isKpi", json.dumps(event))
                continue
            is_kpi = is_kpi_value == 1
            captured_at = parse_timestamp(event.get("capturedAtUtc"))
            ok, reason = validate_kpi_field_name(field_name) if is_kpi else validate_field_name(field_name)
            if not ok:
                append_rejection(field_name, reason, json.dumps(event))
                continue
            if is_kpi and field_name not in WORKER_ALLOWED_KPI_FIELDS:
                append_rejection(field_name, f"Unsupported KPI fieldName: {field_name}", json.dumps(event))
                continue
            ok, reason = validate_field_value(field_name, field_value, is_kpi)
            if not ok:
                append_rejection(field_name, reason, json.dumps(event))
                continue
            append_row(field_name, field_value, is_kpi, "event", captured_at)

    if rows and FACT_ROW_DEDUPE_ENABLED:
        # Lightweight dedupe to avoid double-counting when producers write the same logical value
        # into multiple sections (e.g., both kpis and events). Disable via FACT_ROW_DEDUPE_ENABLED=false
        # if repeated identical events are meaningful for your analysis.
        rows = list(dict.fromkeys(rows))

    if not rows and not rejections:
        append_rejection(None, "No session fields available for ingestion", payload_json())

    return rows, rejections


def upsert_sessions_table(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert sessions table."""
    # Use MERGE to insert or update the session-level KPI record in one atomic operation.
    # The source row values are normalized from payload KPIs before merge.
    execute_with_retry(cursor,
        """
        MERGE dbo.sessions AS target
        USING (VALUES (TRY_CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            session_id, session_start_time_utc, session_end_time_utc, engaged_flag,
            resolved_flag, escalated_flag, abandoned_flag, flow_type,
            satisfaction_score, response_latency_ms_avg, error_flag, fallback_flag, created_at
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                session_start_time_utc = COALESCE(source.session_start_time_utc, target.session_start_time_utc),
                session_end_time_utc = COALESCE(source.session_end_time_utc, target.session_end_time_utc),
                engaged_flag = COALESCE(source.engaged_flag, target.engaged_flag),
                resolved_flag = COALESCE(source.resolved_flag, target.resolved_flag),
                escalated_flag = COALESCE(source.escalated_flag, target.escalated_flag),
                abandoned_flag = COALESCE(source.abandoned_flag, target.abandoned_flag),
                flow_type = COALESCE(source.flow_type, target.flow_type),
                satisfaction_score = COALESCE(source.satisfaction_score, target.satisfaction_score),
                response_latency_ms_avg = COALESCE(source.response_latency_ms_avg, target.response_latency_ms_avg),
                error_flag = COALESCE(source.error_flag, target.error_flag),
                fallback_flag = COALESCE(source.fallback_flag, target.fallback_flag)
        WHEN NOT MATCHED THEN
            INSERT (session_id, session_start_time_utc, session_end_time_utc, engaged_flag,
                resolved_flag, escalated_flag, abandoned_flag, flow_type,
                satisfaction_score, response_latency_ms_avg, error_flag, fallback_flag, created_at)
            VALUES (source.session_id, source.session_start_time_utc, source.session_end_time_utc, source.engaged_flag,
                source.resolved_flag, source.escalated_flag, source.abandoned_flag, source.flow_type,
                source.satisfaction_score, source.response_latency_ms_avg, source.error_flag, source.fallback_flag, source.created_at);
        """,
        (
            session_id,
            normalize_time(get_kpi_value(payload, "session_start_time")),
            normalize_time(get_kpi_value(payload, "session_end_time_utc")),
            normalize_bool(get_kpi_value(payload, "engaged_flag")),
            normalize_bool(get_kpi_value(payload, "resolved_flag")),
            normalize_bool(get_kpi_value(payload, "escalated_flag")),
            normalize_bool(get_kpi_value(payload, "abandoned_flag")),
            normalize_flow_type(get_kpi_value(payload, "flow_type")),
            normalize_int(get_kpi_value(payload, "satisfaction_score")),
            normalize_int(get_kpi_value(payload, "response_latency_ms_avg")),
            normalize_bool(get_kpi_value(payload, "error_flag")),
            normalize_bool(get_kpi_value(payload, "fallback_flag")),
            normalize_time(payload.get("createdAtUtc")) or datetime.now(timezone.utc),
        ),
    )


def clear_conflicting_inquiry_rows(cursor: Any, session_id: str, flow_type: Optional[str]) -> None:
    """Remove stale inquiry rows from tables that do not match the session's canonical flow."""
    if flow_type == "Prospect":
        execute_with_retry(cursor, "DELETE FROM dbo.career_inquiries WHERE session_id = TRY_CAST(? AS UNIQUEIDENTIFIER)", (session_id,))
        execute_with_retry(cursor, "DELETE FROM dbo.partner_inquiries WHERE session_id = TRY_CAST(? AS UNIQUEIDENTIFIER)", (session_id,))
    elif flow_type == "Career":
        execute_with_retry(cursor, "DELETE FROM dbo.prospect_inquiries WHERE session_id = TRY_CAST(? AS UNIQUEIDENTIFIER)", (session_id,))
        execute_with_retry(cursor, "DELETE FROM dbo.partner_inquiries WHERE session_id = TRY_CAST(? AS UNIQUEIDENTIFIER)", (session_id,))
    elif flow_type == "Partnership":
        execute_with_retry(cursor, "DELETE FROM dbo.prospect_inquiries WHERE session_id = TRY_CAST(? AS UNIQUEIDENTIFIER)", (session_id,))
        execute_with_retry(cursor, "DELETE FROM dbo.career_inquiries WHERE session_id = TRY_CAST(? AS UNIQUEIDENTIFIER)", (session_id,))


def upsert_drop_off_nodes(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert drop off nodes."""
    # Persist the final node and drop-off details for the session.
    # This table is only updated when drop-off-related KPIs are present.
    execute_with_retry(cursor,
        """
        MERGE dbo.drop_off_nodes AS target
        USING (VALUES (TRY_CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?)) AS source (
            session_id, last_node_id, last_node_name, last_node_time_utc,
            goal_completed_flag, exit_reason, created_at
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                last_node_id = COALESCE(source.last_node_id, target.last_node_id),
                last_node_name = COALESCE(source.last_node_name, target.last_node_name),
                last_node_time_utc = COALESCE(source.last_node_time_utc, target.last_node_time_utc),
                goal_completed_flag = COALESCE(source.goal_completed_flag, target.goal_completed_flag),
                exit_reason = COALESCE(source.exit_reason, target.exit_reason)
        WHEN NOT MATCHED THEN
            INSERT (id, session_id, last_node_id, last_node_name, last_node_time_utc,
                goal_completed_flag, exit_reason, created_at)
            VALUES (NEWID(), source.session_id, source.last_node_id, source.last_node_name, source.last_node_time_utc,
                source.goal_completed_flag, source.exit_reason, source.created_at);
        """,
        (
            session_id,
            normalize_text(get_kpi_value(payload, "last_node_id")) or None,
            normalize_text(get_kpi_value(payload, "last_node_name")) or None,
            normalize_time(get_kpi_value(payload, "last_node_time")),
            normalize_bool(get_kpi_value(payload, "goal_completed_flag")),
            normalize_text(get_kpi_value(payload, "exit_reason")) or None,
            datetime.now(timezone.utc),
        ),
    )


def upsert_satisfaction_feedback(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert satisfaction feedback."""
    execute_with_retry(cursor,
        """
        MERGE dbo.satisfaction_feedback AS target
        USING (VALUES (TRY_CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?)) AS source (
            session_id, satisfaction_score, feedback_submitted_flag, satisfaction_submitted_flag, feedback_comment, created_at
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                satisfaction_score = COALESCE(source.satisfaction_score, target.satisfaction_score),
                feedback_submitted_flag = COALESCE(source.feedback_submitted_flag, target.feedback_submitted_flag),
                satisfaction_submitted_flag = COALESCE(source.satisfaction_submitted_flag, target.satisfaction_submitted_flag),
                feedback_comment = COALESCE(source.feedback_comment, target.feedback_comment)
        WHEN NOT MATCHED THEN
            INSERT (id, session_id, satisfaction_score, feedback_submitted_flag, satisfaction_submitted_flag, feedback_comment, created_at)
            VALUES (NEWID(), source.session_id, source.satisfaction_score, source.feedback_submitted_flag, source.satisfaction_submitted_flag, source.feedback_comment, source.created_at);
        """,
        (
            session_id,
            normalize_int(get_kpi_value(payload, "satisfaction_score")),
            normalize_bool_with_default(get_kpi_value(payload, "feedback_submitted_flag")),
            normalize_bool_with_default(get_kpi_value(payload, "satisfaction_submitted_flag")),
            normalize_text(get_kpi_value(payload, "feedback_comment")) or None,
            datetime.now(timezone.utc),
        ),
    )


def upsert_prospect_inquiries(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert prospect inquiries."""
    # Persist prospect inquiry KPI fields into the normalized prospect_inquiries table.
    # This writes lead capture and qualification metadata for the session.
    offering_primary = normalize_offering_name(get_kpi_value(payload, "offering_primary"))
    offering_secondary = normalize_offering_name(get_kpi_value(payload, "offering_secondary"))
    execute_with_retry(cursor,
        """
        MERGE dbo.prospect_inquiries AS target
        USING (VALUES (TRY_CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            session_id, flow_type, lead_capture_started_flag, lead_capture_completed_flag,
            lead_name, lead_email, lead_company, lead_phone, lead_industry, lead_job_title,
            consultation_requested_flag, scheduler_link_clicked_flag, offering_primary,
            offering_secondary, offering_primary_category, offering_secondary_category, intent_primary, created_at
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                flow_type = COALESCE(source.flow_type, target.flow_type),
                lead_capture_started_flag = COALESCE(source.lead_capture_started_flag, target.lead_capture_started_flag),
                lead_capture_completed_flag = COALESCE(source.lead_capture_completed_flag, target.lead_capture_completed_flag),
                lead_name = COALESCE(source.lead_name, target.lead_name),
                lead_email = COALESCE(source.lead_email, target.lead_email),
                lead_company = COALESCE(source.lead_company, target.lead_company),
                lead_phone = COALESCE(source.lead_phone, target.lead_phone),
                lead_industry = COALESCE(source.lead_industry, target.lead_industry),
                lead_job_title = COALESCE(source.lead_job_title, target.lead_job_title),
                consultation_requested_flag = COALESCE(source.consultation_requested_flag, target.consultation_requested_flag),
                scheduler_link_clicked_flag = COALESCE(source.scheduler_link_clicked_flag, target.scheduler_link_clicked_flag),
                offering_primary = COALESCE(source.offering_primary, target.offering_primary),
                offering_secondary = COALESCE(source.offering_secondary, target.offering_secondary),
                offering_primary_category = COALESCE(source.offering_primary_category, target.offering_primary_category),
                offering_secondary_category = COALESCE(source.offering_secondary_category, target.offering_secondary_category),
                intent_primary = COALESCE(source.intent_primary, target.intent_primary)
        WHEN NOT MATCHED THEN
            INSERT (id, session_id, flow_type, lead_capture_started_flag, lead_capture_completed_flag,
                lead_name, lead_email, lead_company, lead_phone, lead_industry, lead_job_title,
                consultation_requested_flag,
                scheduler_link_clicked_flag, offering_primary, offering_secondary,
                offering_primary_category, offering_secondary_category, intent_primary, created_at)
            VALUES (NEWID(), source.session_id, source.flow_type, source.lead_capture_started_flag, source.lead_capture_completed_flag,
                source.lead_name, source.lead_email, source.lead_company, source.lead_phone, source.lead_industry,
                source.lead_job_title, source.consultation_requested_flag,
                source.scheduler_link_clicked_flag, source.offering_primary, source.offering_secondary,
                source.offering_primary_category, source.offering_secondary_category, source.intent_primary, source.created_at);
        """,
        (
            session_id,
            normalize_flow_type(get_kpi_value(payload, "flow_type")),
            normalize_bool(get_kpi_value(payload, "lead_capture_started_flag")),
            normalize_bool(get_kpi_value(payload, "lead_capture_completed_flag")),
            normalize_text(get_kpi_value(payload, "lead_name")) or None,
            normalize_text(get_kpi_value(payload, "lead_email")) or None,
            normalize_text(get_kpi_value(payload, "lead_company")) or None,
            normalize_text(get_kpi_value(payload, "lead_phone")) or None,
            normalize_text(get_kpi_value(payload, "lead_industry")) or None,
            normalize_text(get_kpi_value(payload, "lead_job_title")) or None,
            normalize_bool(get_kpi_value(payload, "consultation_requested_flag")),
            normalize_bool(get_kpi_value(payload, "scheduler_link_clicked_flag")),
            offering_primary,
            offering_secondary,
            get_offering_category(offering_primary),
            get_offering_category(offering_secondary),
            normalize_text(get_kpi_value(payload, "intent_primary")) or None,
            datetime.now(timezone.utc),
        ),
    )


def upsert_career_inquiries(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert career inquiries."""
    # Persist career inquiry KPI fields into the normalized career_inquiries table.
    # This includes intent, candidate, and job preference metadata.
    execute_with_retry(cursor,
        """
        MERGE dbo.career_inquiries AS target
        USING (VALUES (TRY_CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            session_id, flow_type, application_intent_flag,
            candidate_capture_started_flag, candidate_capture_completed_flag,
            candidate_name, candidate_email, job_interest_area, job_interest_location, created_at
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                flow_type = COALESCE(source.flow_type, target.flow_type),
                application_intent_flag = COALESCE(source.application_intent_flag, target.application_intent_flag),
                candidate_capture_started_flag = COALESCE(source.candidate_capture_started_flag, target.candidate_capture_started_flag),
                candidate_capture_completed_flag = COALESCE(source.candidate_capture_completed_flag, target.candidate_capture_completed_flag),
                candidate_name = COALESCE(source.candidate_name, target.candidate_name),
                candidate_email = COALESCE(source.candidate_email, target.candidate_email),
                job_interest_area = COALESCE(source.job_interest_area, target.job_interest_area),
                job_interest_location = COALESCE(source.job_interest_location, target.job_interest_location)
        WHEN NOT MATCHED THEN
            INSERT (id, session_id, flow_type, application_intent_flag,
                candidate_capture_started_flag, candidate_capture_completed_flag,
                candidate_name, candidate_email, job_interest_area, job_interest_location, created_at)
            VALUES (NEWID(), source.session_id, source.flow_type, source.application_intent_flag,
                source.candidate_capture_started_flag, source.candidate_capture_completed_flag,
                source.candidate_name, source.candidate_email, source.job_interest_area, source.job_interest_location, source.created_at);
        """,
        (
            session_id,
            normalize_flow_type(get_kpi_value(payload, "flow_type")),
            normalize_bool(get_kpi_value(payload, "application_intent_flag")),
            normalize_bool(get_kpi_value(payload, "candidate_capture_started_flag")),
            normalize_bool(get_kpi_value(payload, "candidate_capture_completed_flag")),
            normalize_text(get_kpi_value(payload, "candidate_name")) or None,
            normalize_text(get_kpi_value(payload, "candidate_email")) or None,
            normalize_text(get_kpi_value(payload, "job_interest_area")) or None,
            normalize_text(get_kpi_value(payload, "job_interest_location")) or None,
            datetime.now(timezone.utc),
        ),
    )


def upsert_partner_inquiries(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert partner inquiries."""
    # Persist partner inquiry KPI fields into the normalized partner_inquiries table.
    # This captures partner lead generation and booking activity.
    execute_with_retry(cursor,
        """
        MERGE dbo.partner_inquiries AS target
        USING (VALUES (TRY_CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            session_id, flow_type,
            partner_capture_started_flag, partner_capture_completed_flag,
            partner_name, partner_org_name, partner_email, partner_type,
            partner_consultation_requested_flag, partner_consultation_booked_flag, created_at
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                flow_type = COALESCE(source.flow_type, target.flow_type),
                partner_capture_started_flag = COALESCE(source.partner_capture_started_flag, target.partner_capture_started_flag),
                partner_capture_completed_flag = COALESCE(source.partner_capture_completed_flag, target.partner_capture_completed_flag),
                partner_name = COALESCE(source.partner_name, target.partner_name),
                partner_org_name = COALESCE(source.partner_org_name, target.partner_org_name),
                partner_email = COALESCE(source.partner_email, target.partner_email),
                partner_type = COALESCE(source.partner_type, target.partner_type),
                partner_consultation_requested_flag = COALESCE(source.partner_consultation_requested_flag, target.partner_consultation_requested_flag),
                partner_consultation_booked_flag = COALESCE(source.partner_consultation_booked_flag, target.partner_consultation_booked_flag)
        WHEN NOT MATCHED THEN
            INSERT (id, session_id, flow_type,
                partner_capture_started_flag, partner_capture_completed_flag,
                partner_name, partner_org_name, partner_email, partner_type,
                partner_consultation_requested_flag, partner_consultation_booked_flag, created_at)
            VALUES (NEWID(), source.session_id, source.flow_type,
                source.partner_capture_started_flag, source.partner_capture_completed_flag,
                source.partner_name, source.partner_org_name, source.partner_email, source.partner_type,
                source.partner_consultation_requested_flag, source.partner_consultation_booked_flag, source.created_at);
        """,
        (
            session_id,
            normalize_flow_type(get_kpi_value(payload, "flow_type")),
            normalize_bool(get_kpi_value(payload, "partner_capture_started_flag")),
            normalize_bool(get_kpi_value(payload, "partner_capture_completed_flag")),
            normalize_text(get_kpi_value(payload, "partner_name")) or None,
            normalize_text(get_kpi_value(payload, "partner_org_name")) or None,
            normalize_text(get_kpi_value(payload, "partner_email")) or None,
            normalize_text(get_kpi_value(payload, "partner_type")) or None,
            normalize_bool(get_kpi_value(payload, "partner_consultation_requested_flag")),
            normalize_bool(get_kpi_value(payload, "partner_consultation_booked_flag")),
            datetime.now(timezone.utc),
        ),
    )


def upsert_vendor_inquiries(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert vendor inquiries."""
    # Persist vendor inquiry KPI fields into the normalized vendor_inquiries table.
    # This includes vendor contact and service interest data.
    execute_with_retry(cursor,
        """
        MERGE dbo.vendor_inquiries AS target
        USING (VALUES (TRY_CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            session_id, vendor_name, vendor_company, vendor_email, vendor_phone,
            service_category, service_details, partner_status, previous_experience, created_at
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                vendor_name = COALESCE(source.vendor_name, target.vendor_name),
                vendor_company = COALESCE(source.vendor_company, target.vendor_company),
                vendor_email = COALESCE(source.vendor_email, target.vendor_email),
                vendor_phone = COALESCE(source.vendor_phone, target.vendor_phone),
                service_category = COALESCE(source.service_category, target.service_category),
                service_details = COALESCE(source.service_details, target.service_details),
                partner_status = COALESCE(source.partner_status, target.partner_status),
                previous_experience = COALESCE(source.previous_experience, target.previous_experience)
        WHEN NOT MATCHED THEN
            INSERT (id, session_id, vendor_name, vendor_company, vendor_email, vendor_phone,
                service_category, service_details, partner_status, previous_experience, created_at)
            VALUES (NEWID(), source.session_id, source.vendor_name, source.vendor_company, source.vendor_email, source.vendor_phone,
                source.service_category, source.service_details, source.partner_status, source.previous_experience, source.created_at);
        """,
        (
            session_id,
            normalize_text(get_kpi_value(payload, "vendor_name")) or None,
            normalize_text(get_kpi_value(payload, "vendor_company")) or None,
            normalize_text(get_kpi_value(payload, "vendor_email")) or None,
            normalize_text(get_kpi_value(payload, "vendor_phone")) or None,
            normalize_text(get_kpi_value(payload, "service_category")) or None,
            normalize_text(get_kpi_value(payload, "service_details")) or None,
            normalize_text(get_kpi_value(payload, "partner_status")) or None,
            normalize_text(get_kpi_value(payload, "previous_experience")) or None,
            datetime.now(timezone.utc),
        ),
    )


def upsert_bot_optimization_metrics(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert bot optimization metrics."""
    # Persist bot optimization KPI fields into the normalized bot_optimization_metrics table.
    # This includes fallback, latency, and error diagnostics.
    execute_with_retry(cursor,
        """
        MERGE dbo.bot_optimization_metrics AS target
        USING (VALUES (TRY_CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            session_id, fallback_flag, fallback_count, response_latency_ms,
            error_flag, error_node_id, error_code, error_count, created_at
        )
        ON target.session_id = source.session_id
        WHEN MATCHED THEN
            UPDATE SET
                fallback_flag = COALESCE(source.fallback_flag, target.fallback_flag),
                fallback_count = COALESCE(source.fallback_count, target.fallback_count),
                response_latency_ms = COALESCE(source.response_latency_ms, target.response_latency_ms),
                error_flag = COALESCE(source.error_flag, target.error_flag),
                error_node_id = COALESCE(source.error_node_id, target.error_node_id),
                error_code = COALESCE(source.error_code, target.error_code),
                error_count = COALESCE(source.error_count, target.error_count)
        WHEN NOT MATCHED THEN
            INSERT (id, session_id, fallback_flag, fallback_count, response_latency_ms,
                error_flag, error_node_id, error_code, error_count, created_at)
            VALUES (NEWID(), source.session_id, source.fallback_flag, source.fallback_count, source.response_latency_ms,
                source.error_flag, source.error_node_id, source.error_code, source.error_count, source.created_at);
        """,
        (
            session_id,
            normalize_bool(get_kpi_value(payload, "fallback_flag")),
            normalize_int(get_kpi_value(payload, "fallback_count")),
            normalize_int(get_kpi_value(payload, "response_latency_ms")),
            normalize_bool(get_kpi_value(payload, "error_flag")),
            normalize_int(get_kpi_value(payload, "error_node_id")),
            normalize_text(get_kpi_value(payload, "error_code")) or None,
            normalize_int(get_kpi_value(payload, "error_count")),
            datetime.now(timezone.utc),
        ),
    )


def upsert_normalized_tables(cursor: Any, payload: Dict[str, Any], session_id: str) -> None:
    """Upsert normalized tables."""
    # Only write detail tables when the payload contains the corresponding KPI fields.
    flow_type = normalize_flow_type(get_kpi_value(payload, "flow_type"))
    has_session_fields = has_any_kpi_fields(payload, [
        "session_start_time", "session_end_time_utc", "engaged_flag", "resolved_flag",
        "escalated_flag", "abandoned_flag", "flow_type", "satisfaction_score",
        "response_latency_ms_avg", "error_flag", "fallback_flag",
    ])
    has_drop_off_fields = has_any_kpi_fields(payload, [
        "last_node_id", "last_node_name", "last_node_time", "goal_completed_flag", "exit_reason",
    ])
    has_satisfaction_fields = has_any_kpi_fields(payload, [
        "satisfaction_score", "feedback_submitted_flag", "satisfaction_submitted_flag", "feedback_comment",
    ])
    has_prospect_fields = has_any_kpi_fields(payload, PROSPECT_DETAIL_FIELDS)
    has_career_fields = has_any_kpi_fields(payload, CAREER_DETAIL_FIELDS)
    has_partner_fields = has_any_kpi_fields(payload, PARTNERSHIP_DETAIL_FIELDS)
    has_vendor_fields = has_any_kpi_fields(payload, [
        "vendor_name", "vendor_company", "vendor_email", "vendor_phone",
        "service_category", "service_details", "partner_status", "previous_experience"
    ])
    has_bot_optimization_fields = has_any_kpi_fields(payload, [
        "fallback_flag", "fallback_count", "response_latency_ms", "error_flag", "error_node_id", "error_code", "error_count",
    ])

    upsert_sessions_table(cursor, payload, session_id)
    clear_conflicting_inquiry_rows(cursor, session_id, flow_type)

    should_upsert_detail_tables = any([
        has_drop_off_fields,
        has_satisfaction_fields,
        has_prospect_fields,
        has_career_fields,
        has_partner_fields,
        has_vendor_fields,
        has_bot_optimization_fields,
    ])

    if not should_upsert_detail_tables:
        # Skip additional normalized tables when no detail KPI fields are present.
        if has_session_fields:
            logging.info("Skipping normalized detail table upserts for session %s: only session-level KPI fields present", session_id)
        else:
            logging.info("Skipping normalized detail table upserts for session %s: no detail KPI fields present", session_id)
        return

    def run_detail_upsert(table_name: str, func: Any) -> None:
        try:
            func(cursor, payload, session_id)
        except Exception:
            if not IGNORE_NORMALIZED_UPSERT_ERRORS:
                raise
            logging.exception("Ignoring normalized detail table upsert failure for %s in session %s", table_name, session_id)

    if has_drop_off_fields:
        run_detail_upsert("drop_off_nodes", upsert_drop_off_nodes)
    if has_satisfaction_fields:
        run_detail_upsert("satisfaction_feedback", upsert_satisfaction_feedback)
    if has_prospect_fields and flow_type == "Prospect":
        run_detail_upsert("prospect_inquiries", upsert_prospect_inquiries)
    if has_career_fields and flow_type == "Career":
        run_detail_upsert("career_inquiries", upsert_career_inquiries)
    if has_partner_fields and flow_type == "Partnership":
        run_detail_upsert("partner_inquiries", upsert_partner_inquiries)
    if has_vendor_fields:
        run_detail_upsert("vendor_inquiries", upsert_vendor_inquiries)
    if has_bot_optimization_fields:
        run_detail_upsert("bot_optimization_metrics", upsert_bot_optimization_metrics)


def get_ingestion_state(cursor: Any, blob_path: str) -> Optional[Dict[str, Any]]:
    """Get ingestion state."""
    execute_with_retry(cursor,
        "SELECT blob_etag, ingestion_status FROM session_blob_ingestion WHERE blob_path = ?",
        (blob_path,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {"blob_etag": row[0], "ingestion_status": row[1]}


def delete_existing_blob_rows(cursor: Any, blob_path: str) -> None:
    """Delete existing blob rows."""
    execute_with_retry(cursor, "DELETE FROM session_blob_fact WHERE blob_path = ?", (blob_path,))
    execute_with_retry(cursor, "DELETE FROM session_blob_rejection WHERE blob_path = ?", (blob_path,))


def insert_fact_rows(cursor: Any, rows: List[Tuple[Any, ...]]) -> None:
    """Insert fact rows."""
    executemany_with_retry(
        cursor,
        "INSERT INTO session_blob_fact (blob_path, session_id, field_name, field_value, is_kpi, event_type, event_timestamp_utc) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def insert_rejection_rows(cursor: Any, rows: List[Tuple[Any, ...]]) -> None:
    """Insert rejection rows."""
    executemany_with_retry(
        cursor,
        "INSERT INTO session_blob_rejection (blob_path, session_id, field_name, rejected_at_utc, reason, raw_text) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def upsert_ingestion_state(
    cursor: Any,
    blob_path: str,
    last_modified: datetime,
    etag: str,
    status: str,
    row_count: int,
    rejection_count: int,
    error_message: Optional[str] = None,
) -> None:
    """Upsert ingestion state."""
    # Maintain a single canonical ingestion state row for each processed blob.
    # Use MERGE to update or insert ingestion metadata on every pass.
    now = datetime.now(timezone.utc)
    execute_with_retry(cursor,
        """
        MERGE session_blob_ingestion AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            blob_path, last_modified_utc, blob_etag, ingestion_status,
            ingested_at_utc, row_count, rejection_count, error_message
        )
        ON target.blob_path = source.blob_path
        WHEN MATCHED THEN
            UPDATE SET
                last_modified_utc = source.last_modified_utc,
                blob_etag = source.blob_etag,
                ingestion_status = source.ingestion_status,
                ingested_at_utc = source.ingested_at_utc,
                row_count = source.row_count,
                rejection_count = source.rejection_count,
                error_message = source.error_message
        WHEN NOT MATCHED THEN
            INSERT (blob_path, last_modified_utc, blob_etag, ingestion_status, ingested_at_utc, row_count, rejection_count, error_message)
            VALUES (source.blob_path, source.last_modified_utc, source.blob_etag, source.ingestion_status, source.ingested_at_utc, source.row_count, source.rejection_count, source.error_message);
        """,
        (
            blob_path,
            last_modified,
            etag,
            status,
            now,
            row_count,
            rejection_count,
            error_message,
        ),
    )


def upsert_ingestion_run_history(
    cursor: Any,
    run_id: str,
    started_at_utc: datetime,
    completed_at_utc: datetime,
    selected_blob_path: Optional[str],
    status: str,
    blobs_processed: int,
    blobs_succeeded: int,
    blobs_rejected: int,
    blobs_failed: int,
    blobs_skipped: int,
    sql_connect_retries: int,
    sql_execute_retries: int,
    sql_executemany_retries: int,
    error_message: Optional[str] = None,
) -> None:
    """Write or update ingestion run summary for reliability monitoring."""
    execute_with_retry(
        cursor,
        """
        MERGE dbo.session_blob_ingestion_run AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            run_id, started_at_utc, completed_at_utc, run_status, selected_blob_path,
            blobs_processed, blobs_succeeded, blobs_rejected, blobs_failed, blobs_skipped,
            sql_connect_retries, sql_execute_retries, sql_executemany_retries, error_message
        )
        ON target.run_id = source.run_id
        WHEN MATCHED THEN
            UPDATE SET
                completed_at_utc = source.completed_at_utc,
                run_status = source.run_status,
                selected_blob_path = source.selected_blob_path,
                blobs_processed = source.blobs_processed,
                blobs_succeeded = source.blobs_succeeded,
                blobs_rejected = source.blobs_rejected,
                blobs_failed = source.blobs_failed,
                blobs_skipped = source.blobs_skipped,
                sql_connect_retries = source.sql_connect_retries,
                sql_execute_retries = source.sql_execute_retries,
                sql_executemany_retries = source.sql_executemany_retries,
                error_message = source.error_message
        WHEN NOT MATCHED THEN
            INSERT (run_id, started_at_utc, completed_at_utc, run_status, selected_blob_path,
                blobs_processed, blobs_succeeded, blobs_rejected, blobs_failed, blobs_skipped,
                sql_connect_retries, sql_execute_retries, sql_executemany_retries, error_message)
            VALUES (source.run_id, source.started_at_utc, source.completed_at_utc, source.run_status, source.selected_blob_path,
                source.blobs_processed, source.blobs_succeeded, source.blobs_rejected, source.blobs_failed, source.blobs_skipped,
                source.sql_connect_retries, source.sql_execute_retries, source.sql_executemany_retries, source.error_message);
        """,
        (
            run_id,
            started_at_utc,
            completed_at_utc,
            status,
            selected_blob_path,
            blobs_processed,
            blobs_succeeded,
            blobs_rejected,
            blobs_failed,
            blobs_skipped,
            sql_connect_retries,
            sql_execute_retries,
            sql_executemany_retries,
            error_message,
        ),
    )


def refresh_kpi_aggregates(
    cursor: Any,
    lookback_days: int = KPI_AGGREGATE_REFRESH_LOOKBACK_DAYS,
    full_refresh: bool = KPI_AGGREGATE_REFRESH_FULL,
) -> Optional[Dict[str, Any]]:
    """Refresh aggregate KPI table from normalized reporting tables."""
    execute_with_retry(
        cursor,
        """
        EXEC dbo.usp_refresh_kpi_aggregates
            @LookbackDays = ?,
            @FullRefresh = ?;
        """,
        (
            lookback_days,
            1 if full_refresh else 0,
        ),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "rows_inserted": row[0],
        "refresh_run_id": str(row[1]) if row[1] is not None else None,
    }


def resolve_refresh_lookback_days(
    window_end_utc: datetime,
    earliest_session_created_at: Optional[datetime],
    configured_lookback_days: int,
    full_refresh: bool,
) -> int:
    """Expand the refresh window when a run ingests older historical sessions."""
    if full_refresh or earliest_session_created_at is None:
        return configured_lookback_days
    earliest_utc = earliest_session_created_at.astimezone(timezone.utc)
    age_days = (window_end_utc.date() - earliest_utc.date()).days + 1
    return max(configured_lookback_days, age_days, 0)


def process_blob(
    cursor: Any,
    blob_client: Any,
    run_id: str,
    delete_after: bool = False,
    logical_blob_path: Optional[str] = None,
    blob_properties: Any = None,
) -> Tuple[Optional[str], Optional[datetime]]:
    """Process blob."""
    blob_path = logical_blob_path or blob_client.blob_name
    log_ingestion_event("ingestion_started", run_id=run_id, blob_path=blob_path)
    # Use blob properties and ingestion state to avoid reprocessing the same content.
    # Reuse the properties returned by list_blobs when available to save a round-trip.
    properties = blob_properties if blob_properties is not None else blob_client.get_blob_properties()
    state = get_ingestion_state(cursor, blob_path)
    # Skip blobs already processed successfully with unchanged ETag.
    # Rejected/Failed blobs are allowed to reprocess so validation/ingestion fixes can be applied
    # without requiring content changes.
    if state and state["blob_etag"] == properties.etag and state["ingestion_status"] in {"Succeeded"}:
        log_ingestion_event("duplicate_blob_skipped", run_id=run_id, blob_path=blob_path, status=state["ingestion_status"])
        return "Skipped", None

    raw_text = load_blob_text(blob_client)
    try:
        payload = json.loads(raw_text)
        log_ingestion_event("json_parsed", run_id=run_id, blob_path=blob_path)
    except json.JSONDecodeError as exc:
        # Invalid JSON is treated as a failed ingestion and sent to dead letter handling.
        logging.warning(
            "Blob JSON parse failed for %s: %s",
            blob_path,
            exc,
            extra={"blob_path": blob_path, "error": exc.msg, "line": exc.lineno, "column": exc.colno},
        )
        delete_existing_blob_rows(cursor, blob_path)
        insert_rejection_rows(cursor, [
            (blob_path, None, None, datetime.now(timezone.utc), f"JSON parse failure: {exc.msg}", raw_text[:4000])
        ])
        upsert_ingestion_state(cursor, blob_path, properties.last_modified, properties.etag, "Failed", 0, 1, f"JSON parse failure: {exc.msg}")
        dead_letter_blob(blob_client, "JSON parse failure", logical_blob_path=blob_path)
        log_ingestion_event(
            "json_parse_failed",
            level=logging.ERROR,
            run_id=run_id,
            blob_path=blob_path,
            error=exc.msg,
        )
        return "Failed", None

    dev_flag = get_dev_flag(payload)
    if dev_flag == "dev" and not INGEST_DEV_BLOBS:
        try:
            blob_client.delete_blob()
            logging.info("Deleted dev-mode blob without ingestion: %s", blob_path, extra={"blob_path": blob_path})
        except AzureError as exc:
            logging.warning(
                "Failed to delete dev-mode blob %s: %s",
                blob_path,
                exc,
                extra={"blob_path": blob_path, "azure_error": str(exc)},
            )
        upsert_ingestion_state(cursor, blob_path, properties.last_modified, properties.etag, "Skipped", 0, 0, "Dev mode blob deleted")
        log_ingestion_event("dev_blob_skipped", run_id=run_id, blob_path=blob_path)
        return "Skipped", None

    session_id, metadata_errors = validate_session_payload(payload)
    if metadata_errors:
        # Metadata validation failures do not ingest any rows and are rejected.
        # Rejections are stored so users can inspect why the blob was not ingested.
        delete_existing_blob_rows(cursor, blob_path)
        for reason in metadata_errors:
            insert_rejection_rows(cursor, [
                (blob_path, session_id or None, None, datetime.now(timezone.utc), reason, raw_text[:4000])
            ])
        upsert_ingestion_state(cursor, blob_path, properties.last_modified, properties.etag, "Rejected", 0, len(metadata_errors), "; ".join(metadata_errors))
        dead_letter_blob(blob_client, "Metadata validation failure", logical_blob_path=blob_path)
        log_ingestion_event(
            "validation_failed",
            level=logging.WARNING,
            run_id=run_id,
            blob_path=blob_path,
            session_id=session_id,
            error_count=len(metadata_errors),
            errors=metadata_errors,
        )
        return "Rejected", None

    if session_id is None:
        raise RuntimeError("sessionId missing after validation")
    log_ingestion_event("validation_passed", run_id=run_id, blob_path=blob_path, session_id=session_id)

    session_metadata = normalize_session_metadata(payload)
    upsert_session_record(cursor, session_metadata)
    upsert_normalized_tables(cursor, payload, session_id)

    # Extract row-level data for SQL ingestion, then replace any prior rows for this blob.
    approved_rows, rejected_rows = extract_approved_rows(blob_path, payload)
    delete_existing_blob_rows(cursor, blob_path)
    insert_fact_rows(cursor, approved_rows)
    insert_rejection_rows(cursor, rejected_rows)

    failed_field_names = get_failed_field_names_from_rejections(rejected_rows)
    # Write a blob with failed field names so downstream systems can inspect rejection patterns.
    write_failed_fieldnames_blob(blob_path, failed_field_names)

    status = "Succeeded" if approved_rows else "Rejected"
    error_message = None if status == "Succeeded" else "No approved rows"
    upsert_ingestion_state(
        cursor,
        blob_path,
        properties.last_modified,
        properties.etag,
        status,
        len(approved_rows),
        len(rejected_rows),
        error_message,
    )
    log_ingestion_event(
        "sql_write_succeeded",
        run_id=run_id,
        blob_path=blob_path,
        session_id=session_id,
        status=status,
        rows_accepted=len(approved_rows),
        rows_rejected=len(rejected_rows),
    )

    if delete_after and status == "Succeeded":
        # Remove source blob after successful extraction when configured.
        try:
            blob_client.delete_blob()
            logging.info("Deleted blob after extraction: %s", blob_path)
        except AzureError as exc:
            logging.warning("Failed to delete blob %s: %s", blob_path, exc)

    return status, normalize_time(payload.get("createdAtUtc")) or datetime.now(timezone.utc)


def iter_sources() -> List[Tuple[str, str]]:
    """Return a deduplicated list of (container, prefix) sources to scan for ingestion."""
    raw_sources: List[Tuple[str, str]] = []

    if SOURCE_CONTAINERS_ENV.strip():
        containers = [c.strip() for c in SOURCE_CONTAINERS_ENV.split(",") if c.strip()]
        prefixes = [p.strip().lstrip("/") for p in SOURCE_PREFIXES_ENV.split(",")] if SOURCE_PREFIXES_ENV.strip() else []
        if prefixes and len(prefixes) != len(containers):
            raise ValueError("SESSION_LOG_SOURCE_PREFIXES must have same number of entries as SESSION_LOG_SOURCE_CONTAINERS")
        for idx, container in enumerate(containers):
            prefix = prefixes[idx] if prefixes else (BLOB_PREFIX if idx == 0 else "")
            if prefix and not prefix.endswith("/"):
                prefix += "/"
            raw_sources.append((container, prefix))
    else:
        # Default source.
        raw_sources.append((BLOB_CONTAINER, BLOB_PREFIX))
        # Legacy compatibility source (typically the Power Automate "session-logs" container).
        if COMPAT_LEGACY_ENABLED and LEGACY_CONTAINER:
            raw_sources.append((LEGACY_CONTAINER, LEGACY_PREFIX))

    # Deduplicate while preserving scan order.
    seen: set = set()
    sources: List[Tuple[str, str]] = []
    for container, prefix in raw_sources:
        key = (container, prefix)
        if container and key not in seen:
            seen.add(key)
            sources.append(key)
    return sources


def list_blobs_to_process_from_source(container_name: str, prefix: str) -> Iterable[Any]:
    """List blobs to process from a specific container/prefix."""
    container = get_cached_container_client(container_name)
    try:
        pager = container.list_blobs(name_starts_with=prefix, results_per_page=INGESTION_LIST_PAGE_SIZE).by_page()
        for page in pager:
            for blob in page:
                yield blob
    except ResourceNotFoundError:
        # Be resilient when compatibility mode is enabled but the legacy container
        # hasn't been created (or is not accessible) in the current environment.
        logging.warning("Blob container not found (skipping): %s", container_name)
        return


def should_skip_blob_name(blob_name: str) -> bool:
    """Return True when a blob path belongs to an internal helper prefix."""
    return any(blob_name.startswith(prefix) for prefix in INGESTION_EXCLUDED_PREFIXES)


def sanitize_dead_letter_component(value: str) -> str:
    """Sanitize dead letter component."""
    text = normalize_text(value)
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-")
    return text or "unknown"


def build_dead_letter_path(blob_name: str, etag: str) -> str:
    """Build dead letter path."""
    safe_etag = sanitize_dead_letter_component(etag)
    return f"deadletter/{blob_name}--etag-{safe_etag}"


def dead_letter_blob(blob_client: BlobClient, reason: str, logical_blob_path: Optional[str] = None) -> None:
    """Dead letter blob."""
    # Copy failed or rejected blobs to a dead-letter container for later inspection.
    # This preserves payloads that could not be ingested for debugging.
    if not DEAD_LETTER_CONTAINER:
        return
    try:
        target_container = get_dead_letter_container_client()
        if not target_container:
            return
        source_props = blob_client.get_blob_properties()
        source_etag = normalize_text(source_props.etag).strip('"')
        # Use the logical path (container-qualified when multi-source) to avoid
        # dead-letter path collisions across containers with same-named blobs.
        dead_letter_source_name = logical_blob_path or blob_client.blob_name
        dead_letter_path = build_dead_letter_path(dead_letter_source_name, source_etag)
        target_blob = target_container.get_blob_client(dead_letter_path)

        source_payload = blob_client.download_blob().readall()
        if isinstance(source_payload, str):
            source_payload = source_payload.encode("utf-8")

        content_type = "application/json"
        if source_props.content_settings and source_props.content_settings.content_type:
            content_type = source_props.content_settings.content_type

        # Preserve source metadata and failure reason for dead-lettered blobs.
        metadata = {
            "dead_letter_reason": sanitize_dead_letter_component(reason)[:128],
            "source_blob": sanitize_dead_letter_component(blob_client.blob_name)[:256],
            "source_etag": sanitize_dead_letter_component(source_etag)[:128],
            "dead_lettered_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        try:
            target_blob.upload_blob(
                source_payload,
                overwrite=False,
                content_settings=ContentSettings(content_type=content_type),
                metadata=metadata,
            )
            logging.info(
                "Dead-lettered blob %s to container %s at %s because %s",
                blob_client.blob_name,
                DEAD_LETTER_CONTAINER,
                dead_letter_path,
                reason,
            )
        except ResourceExistsError:
            logging.info("Dead-letter blob already exists for source %s at %s", blob_client.blob_name, dead_letter_path)

        if DEAD_LETTER_DELETE_SOURCE:
            try:
                blob_client.delete_blob(delete_snapshots="include")
                logging.info("Deleted source blob after dead-lettering: %s", blob_client.blob_name)
            except AzureError as exc:
                logging.warning("Failed to delete source blob %s after dead-lettering: %s", blob_client.blob_name, exc)
    except AzureError as exc:
        logging.warning("Failed to dead-letter blob %s: %s", blob_client.blob_name, exc)


def run_ingestion(selected_blob: Optional[str] = None, delete_after: bool = False) -> None:
    """Run ingestion for blobs matching the configured prefix."""
    run_id = str(uuid.uuid4())
    run_started_at = datetime.now(timezone.utc)
    processed = 0
    succeeded = 0
    rejected = 0
    failed = 0
    skipped = 0
    earliest_success_created_at: Optional[datetime] = None
    conn = None
    run_error: Optional[str] = None
    # Install run-scoped context so every log line emitted during this run is
    # correlated by run_id (and, once set below, by blob_path).
    set_run_context(run_id=run_id, selected_blob_path=selected_blob)
    try:
        conn = connect_with_retry()
        cursor = conn.cursor()
        log_ingestion_event("run_started", run_id=run_id, selected_blob_path=selected_blob, delete_after=delete_after)
        sources = iter_sources()
        multi_source = len(sources) > 1
        for source_container, source_prefix in sources:
            # Resolve the container client once per source instead of once per blob.
            container_client = get_cached_container_client(source_container)
            for blob in list_blobs_to_process_from_source(source_container, source_prefix):
                logical_path = f"{source_container}/{blob.name}" if multi_source else blob.name
                # Accept either the raw blob name or the container-qualified logical path when filtering.
                if selected_blob and selected_blob not in (blob.name, logical_path):
                    continue
                if not selected_blob and should_skip_blob_name(blob.name):
                    logging.info("Skipping internal helper blob outside ingestion scope: %s", logical_path)
                    continue
                blob_client = container_client.get_blob_client(blob.name)
                processed += 1
                # Scope log context to this blob so failures below carry the
                # identifier even when the failure originates inside process_blob.
                update_run_context(blob_path=logical_path, source_container=source_container)
                try:
                    status, session_created_at = process_blob(
                        cursor,
                        blob_client,
                        run_id=run_id,
                        delete_after=delete_after,
                        logical_blob_path=logical_path,
                        blob_properties=blob,
                    )
                    # Commit the transaction for each blob processed successfully or rejected logically.
                    conn.commit()
                    if status == "Succeeded":
                        succeeded += 1
                        if session_created_at is not None and (
                            earliest_success_created_at is None or session_created_at < earliest_success_created_at
                        ):
                            earliest_success_created_at = session_created_at
                    elif status == "Rejected":
                        rejected += 1
                    elif status == "Failed":
                        failed += 1
                    elif status == "Skipped":
                        skipped += 1
                        continue
                    else:
                        rejected += 1
                except Exception:
                    # Roll back the transaction to avoid partial writes for this blob.
                    conn.rollback()
                    failed += 1
                    error_message = f"Unhandled ingestion error for blob {logical_path}"
                    # logging.exception captures the full traceback, which our
                    # JSON formatter + errors file then persist for later review.
                    logging.exception(
                        "Unhandled ingestion error for blob %s",
                        logical_path,
                        extra={"blob_path": logical_path, "source_container": source_container},
                    )
                    try:
                        props = blob_client.get_blob_properties()
                        upsert_ingestion_state(
                            cursor,
                            logical_path,
                            props.last_modified,
                            props.etag,
                            "Failed",
                            0,
                            1,
                            error_message,
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        logging.exception(
                            "Failed persisting failure ingestion state for blob %s",
                            logical_path,
                            extra={"blob_path": logical_path},
                        )
                    log_ingestion_event(
                        "sql_write_failed",
                        level=logging.ERROR,
                        run_id=run_id,
                        blob_path=logical_path,
                        error=error_message,
                    )
                    continue
        if KPI_AGGREGATE_REFRESH_ENABLED and succeeded > 0:
            refresh_window_end_utc = datetime.now(timezone.utc)
            refresh_lookback_days = resolve_refresh_lookback_days(
                refresh_window_end_utc,
                earliest_success_created_at,
                KPI_AGGREGATE_REFRESH_LOOKBACK_DAYS,
                KPI_AGGREGATE_REFRESH_FULL,
            )
            try:
                refresh_result = refresh_kpi_aggregates(
                    cursor,
                    lookback_days=refresh_lookback_days,
                    full_refresh=KPI_AGGREGATE_REFRESH_FULL,
                )
                conn.commit()
                log_ingestion_event(
                    "kpi_aggregate_refresh_succeeded",
                    run_id=run_id,
                    rows_inserted=refresh_result["rows_inserted"] if refresh_result else None,
                    refresh_run_id=refresh_result["refresh_run_id"] if refresh_result else None,
                    lookback_days=refresh_lookback_days,
                    full_refresh=KPI_AGGREGATE_REFRESH_FULL,
                )
            except Exception as exc:
                conn.rollback()
                log_ingestion_event(
                    "kpi_aggregate_refresh_failed",
                    level=logging.ERROR,
                    run_id=run_id,
                    error=str(exc),
                    lookback_days=refresh_lookback_days,
                    full_refresh=KPI_AGGREGATE_REFRESH_FULL,
                )
                if KPI_AGGREGATE_REFRESH_FAIL_ON_ERROR:
                    raise
        try:
            upsert_ingestion_run_history(
                cursor,
                run_id=run_id,
                started_at_utc=run_started_at,
                completed_at_utc=datetime.now(timezone.utc),
                selected_blob_path=selected_blob,
                status="Succeeded",
                blobs_processed=processed,
                blobs_succeeded=succeeded,
                blobs_rejected=rejected,
                blobs_failed=failed,
                blobs_skipped=skipped,
                sql_connect_retries=RETRY_STATS["sql_connect_retries"],
                sql_execute_retries=RETRY_STATS["sql_execute_retries"],
                sql_executemany_retries=RETRY_STATS["sql_executemany_retries"],
                error_message=None,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            logging.exception("Failed to persist ingestion run history for run_id=%s", run_id)
            raise
    except Exception as exc:
        run_error = str(exc)
        raise
    finally:
        if conn is not None and run_error is not None:
            try:
                cursor = conn.cursor()
                upsert_ingestion_run_history(
                    cursor,
                    run_id=run_id,
                    started_at_utc=run_started_at,
                    completed_at_utc=datetime.now(timezone.utc),
                    selected_blob_path=selected_blob,
                    status="Failed",
                    blobs_processed=processed,
                    blobs_succeeded=succeeded,
                    blobs_rejected=rejected,
                    blobs_failed=failed,
                    blobs_skipped=skipped,
                    sql_connect_retries=RETRY_STATS["sql_connect_retries"],
                    sql_execute_retries=RETRY_STATS["sql_execute_retries"],
                    sql_executemany_retries=RETRY_STATS["sql_executemany_retries"],
                    error_message=run_error,
                )
                conn.commit()
            except Exception:
                if conn is not None:
                    conn.rollback()
                logging.exception("Failed to update failed ingestion run history for run_id=%s", run_id)
        if conn is not None:
            conn.close()
        log_ingestion_event(
            "run_completed",
            level=logging.ERROR if (failed or run_error) else logging.INFO,
            run_id=run_id,
            processed=processed,
            succeeded=succeeded,
            rejected=rejected,
            failed=failed,
            skipped=skipped,
            sql_connect_retries=RETRY_STATS["sql_connect_retries"],
            sql_execute_retries=RETRY_STATS["sql_execute_retries"],
            sql_executemany_retries=RETRY_STATS["sql_executemany_retries"],
            started_at_utc=run_started_at.isoformat(),
            completed_at_utc=datetime.now(timezone.utc).isoformat(),
        )
        if failed >= INGESTION_FAILURE_ALERT_THRESHOLD:
            # Elevate the alert to the errors log and the JSON stream so
            # downstream monitors (e.g. a simple tail on ingestion-errors.log)
            # can page an operator without tailing the full noisy log.
            log_ingestion_event(
                "ingestion_failure_alert",
                level=logging.ERROR,
                run_id=run_id,
                failed_count=failed,
                threshold=INGESTION_FAILURE_ALERT_THRESHOLD,
                processed=processed,
            )
        # Release run-scoped log context now that the run is finished; any
        # subsequent log calls in the same process will not inherit run_id.
        clear_run_context()


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments for blob ingestion."""
    parser = argparse.ArgumentParser(description="Ingest session JSON blobs from Azure Blob Storage into Azure SQL.")
    parser.add_argument("--blob", dest="blob_path", help="Optional single blob path to ingest")
    parser.set_defaults(delete_after=None)
    parser.add_argument("--delete-after", dest="delete_after", action="store_true", help="Delete blob after successful extraction")
    parser.add_argument("--no-delete-after", dest="delete_after", action="store_false", help="Do not delete blob after successful extraction")
    return parser.parse_args()


def main() -> int:
    """Main entrypoint for CLI execution."""
    args = parse_arguments()
    try:
        delete_after = args.delete_after if args.delete_after is not None else INGESTION_DELETE_AFTER_SUCCESS
        # Use explicit CLI flag if provided, otherwise fall back to configured environment default.
        run_ingestion(args.blob_path, delete_after=delete_after)
        logging.info("Blob ingestion completed")
        return 0
    except Exception:
        # logging.exception records the full traceback to every handler, so
        # ingestion-errors.log and ingestion.jsonl both capture the root cause.
        logging.exception("Blob ingestion failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
