# Ingestion Logging

The blob ingestion worker (`blob_text_to_azure_sql.py`) ships with a production
logging layer implemented in `ingestion_logging.py`. Every ingestion run emits
to a set of rotating log files so that operators always have enough history to
investigate failures, without needing the console attached.

## What gets written where

All files live in the directory set by `INGESTION_LOG_DIR` (default
`./logs/`). Each file is rotated by Python's `RotatingFileHandler` once it
exceeds `INGESTION_LOG_MAX_BYTES` (default 10 MB), keeping
`INGESTION_LOG_BACKUP_COUNT` (default 7) compressed backups such as
`ingestion.log.1`, `ingestion.log.2`, ...

| File | Level | Purpose |
| --- | --- | --- |
| `ingestion.log` | `INGESTION_LOG_LEVEL` (default `INFO`) | Human-readable run narrative. First stop when watching a run in progress. |
| `ingestion-errors.log` | `INGESTION_LOG_ERROR_LEVEL` (default `WARNING`) | Triage-only view. Tail this file to see just warnings, errors, validation failures, dead-letter events, and unhandled exceptions. |
| `ingestion.jsonl` | All records (from `DEBUG` up) | Machine-parseable JSON Lines suitable for Azure Log Analytics, Datadog, Splunk, `jq`, `pandas.read_json`, etc. Source of truth for automated monitoring. |
| stderr (console) | `INGESTION_LOG_LEVEL` | Same content as `ingestion.log`, for interactive CLI runs. Disable by setting `INGESTION_LOG_CONSOLE_ENABLED=false`. |

In addition, the existing Azure SQL tables continue to persist structured
ingestion state:

- `dbo.ingestion_state` - one row per blob, with latest status, rows
  accepted/rejected, and the last error message.
- `dbo.ingestion_run_history` - one row per run, with totals and retry
  counters.

And the dead-letter container (`DEAD_LETTER_CONTAINER`) still stores the raw
payloads of blobs that failed validation or could not be parsed, with
metadata such as `dead_letter_reason`, `source_blob`, `source_etag`, and
`dead_lettered_at_utc` for post-mortem analysis.

## Correlating logs with runs and blobs

Every record emitted during an ingestion run carries a run-scoped context
injected by `RunContextFilter`. Specifically:

- `run_id` - unique UUID per `run_ingestion` call.
- `selected_blob_path` - CLI `--blob` filter, if any.
- `blob_path` - container-qualified logical path once the per-blob loop
  starts working on a blob.
- `source_container` - whichever container the current blob was listed
  from.

In `ingestion.jsonl` these appear under the top-level `context` object plus
as individual keys, so you can filter a 10 000-line log to a single run with
one command:

```bash
jq 'select(.context.run_id == "11111111-2222-3333-4444-555555555555")' logs/ingestion.jsonl
```

Or filter to a single blob across runs:

```bash
jq 'select(.blob_path == "session-logs/abc.json")' logs/ingestion.jsonl
```

## Event stages

`log_ingestion_event(stage, level=..., **context)` emits events with a stable
`stage` field. The full list of stages currently emitted:

| Stage | Level | When |
| --- | --- | --- |
| `run_started` | INFO | Start of `run_ingestion`. |
| `ingestion_started` | INFO | Per-blob processing begins. |
| `duplicate_blob_skipped` | INFO | Blob etag matches last successful ingestion. |
| `dev_blob_skipped` | INFO | `devFlag == "dev"` - blob deleted without ingestion. |
| `json_parsed` | INFO | Blob payload parsed as JSON successfully. |
| `json_parse_failed` | ERROR | JSON parse error; blob dead-lettered. |
| `validation_passed` | INFO | Session metadata validation succeeded. |
| `validation_failed` | WARNING | Session metadata validation failed. `errors` list included. |
| `sql_write_succeeded` | INFO | Rows successfully written for the blob. |
| `sql_write_failed` | ERROR | Unhandled exception during SQL write. |
| `ingestion_failure_alert` | ERROR | `failed` count for the run exceeded `INGESTION_FAILURE_ALERT_THRESHOLD`. |
| `run_completed` | INFO (ERROR if any failures) | End of `run_ingestion`. |

Uncaught exceptions that escape `main()` are captured twice:

1. Via `logging.exception(...)` in the `main()` try/except with a full
   traceback.
2. Via `install_excepthook()` which routes any top-level uncaught exception
   through logging before the Python interpreter exits.

## Configuration reference

All of these may be set in the environment, in `local.settings.json`, or via
the Azure Functions App settings (if the worker runs in a Function host).

| Variable | Default | Description |
| --- | --- | --- |
| `INGESTION_LOG_DIR` | `logs` | Directory for log files. If not writable, logs fall back to the current working directory. |
| `INGESTION_LOG_LEVEL` | `INFO` | Minimum level for `ingestion.log` and the console handler. |
| `INGESTION_LOG_ERROR_LEVEL` | `WARNING` | Minimum level for `ingestion-errors.log`. Set to `ERROR` if warnings are too noisy. |
| `INGESTION_LOG_CONSOLE_ENABLED` | `true` | Write to stderr in addition to files. Disable for batch jobs without a terminal. |
| `INGESTION_LOG_JSON_ENABLED` | `true` | Emit `ingestion.jsonl`. Disable only if disk space is extremely tight. |
| `INGESTION_LOG_MAX_BYTES` | `10485760` (10 MB) | Rotate after a file reaches this size. |
| `INGESTION_LOG_BACKUP_COUNT` | `7` | Number of rotated backups kept per file. Tune for retention vs disk budget. |
| `INGESTION_LOG_AUTO_SETUP` | `true` | If `false`, importing `ingestion_logging` does not auto-configure logging; callers must invoke `setup_logging()` themselves. |

## Recipes

### Triage a failed run quickly
```bash
tail -n 200 logs/ingestion-errors.log
```

### See everything that happened for a specific blob
```bash
jq 'select(.blob_path == "session-logs/sessions/abc.json")' logs/ingestion.jsonl
```

### Summarize failures by stage
```bash
jq -r 'select(.level=="ERROR") | .stage' logs/ingestion.jsonl | sort | uniq -c
```

### Ship logs to Azure Log Analytics
Point your log collector at `logs/ingestion.jsonl`. Each line is a
self-contained JSON document with a UTC `timestamp`, `level`, `logger`,
`stage`, and run/blob context.

### Disable file logging for a one-off run
```bash
INGESTION_LOG_JSON_ENABLED=false INGESTION_LOG_DIR=. python blob_text_to_azure_sql.py --blob foo.json
```

### Enable debug-level detail temporarily
```bash
INGESTION_LOG_LEVEL=DEBUG python blob_text_to_azure_sql.py
```
Note that Azure SDK and `urllib3` loggers are explicitly pinned to `WARNING`
inside `setup_logging` to avoid flooding your logs with HTTP noise even when
the root level is `DEBUG`.
