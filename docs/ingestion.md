# Ingestion

## Purpose

The ingestion worker processes session blobs and writes validated data to Azure SQL.

## Workflow

1. List blobs from one or more configured sources:
   - Default: `SESSION_LOG_CONTAINER` under prefix `SESSION_LOG_BLOB_PREFIX`
   - Optional compatibility mode: also scan `SESSION_LOG_LEGACY_CONTAINER` / `SESSION_LOG_LEGACY_PREFIX`
   - Advanced: `SESSION_LOG_SOURCE_CONTAINERS` / `SESSION_LOG_SOURCE_PREFIXES` (comma-separated lists)
2. Download each blob as UTF-8 text.
3. Parse JSON payload.
4. Validate session ID, field names, and field values.
5. Write approved rows to `dbo.session_blob_fact`.
6. Write rejected rows to `dbo.session_blob_rejection`.
7. Record ingestion state in `dbo.session_blob_ingestion`.
8. Record run details in `dbo.session_blob_ingestion_run`.
9. Optionally write dead-letter metadata to `DEAD_LETTER_CONTAINER`.

## Ingestion CLI

Run the ingestion worker with:

```bash
python blob_text_to_azure_sql.py
```

Available flags:
- `--blob <path>`: ingest a specific blob instead of scanning the prefix.
- `--delete-after`: delete successfully ingested blobs.
- `--no-delete-after`: keep blobs after ingestion.

The CLI flag overrides `INGESTION_DELETE_AFTER_SUCCESS` for that run.

## Important behavior

- `INGESTION_DELETE_AFTER_SUCCESS=false` is recommended until ingestion behavior is confirmed.
- `DEAD_LETTER_DELETE_SOURCE=false` is recommended when using dead-lettering.
- `FACT_ROW_DEDUPE_ENABLED=true` reduces duplicate inserts in SQL.
- If a blob payload contains `devFlag: "dev"`, the ingestion worker deletes it and marks it as skipped rather than ingesting it.
- When ingesting from multiple blob sources in a single run, the ingestion worker records `blob_path` in SQL as `<container>/<blobName>` to avoid collisions across containers.

## Failure handling

- Failed blobs are counted and logged.
- If `DEAD_LETTER_CONTAINER` is configured, bad blob metadata may be written there.
- The ingestion worker logs structured stages including `ingestion_started`, `json_parsed`, `validation_passed`, `sql_write_succeeded`, `sql_write_failed`, `json_parse_failed`, `validation_failed`, `ingestion_failure_alert`, and `run_completed`.

## Logging

Every ingestion run writes to rotating log files under `INGESTION_LOG_DIR`
(default `logs/`):

- `ingestion.log` - human-readable run narrative.
- `ingestion-errors.log` - warnings and errors only, for quick triage.
- `ingestion.jsonl` - machine-parseable JSON Lines with full per-record
  context (run_id, blob_path, stage, exception tracebacks). Source of truth
  for automated monitoring.

All records emitted during a run carry a `run_id` (and, once the per-blob
loop starts, a `blob_path`) so you can correlate events for a single run or
a single blob across files. Uncaught exceptions are captured by an
`install_excepthook` handler so nothing is lost even if the process
terminates unexpectedly.

See [`docs/logging.md`](logging.md) for the full reference, including
environment variables (`INGESTION_LOG_LEVEL`, `INGESTION_LOG_MAX_BYTES`,
etc.), the event-stage table, and example `jq` recipes.

## Local settings integration

When run locally, the ingestion worker loads defaults from `local.settings.json` if present.
