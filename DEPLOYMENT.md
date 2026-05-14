# Deployment Runbook

## Scope

This runbook is for deploying:

- Azure Functions HTTP app from `function_app.py`
- Ingestion worker from `blob_text_to_azure_sql.py`
- Azure SQL schema from `chatbot-sessions-data-export-schema.sql`

Current flow:

Azure Function HTTP API -> Azure Blob Storage -> Ingestion worker -> Azure SQL

## Prerequisites

- Azure subscription and resource group
- Azure Storage Account (Blob)
- Azure Function App (Python)
- Azure SQL Server + Azure SQL Database
- Network path from runtime(s) to Storage and SQL
- Function App and ingestion runtime configured with required app settings
- Local and CI hosts for ingestion must have `ODBC Driver 18 for SQL Server` installed

## SQL Deployment

Run these in order against target Azure SQL database:

1. `chatbot-sessions-data-export-schema.sql` (canonical schema + normalized reporting tables + reporting views + KPI refresh procedure)

## Function App Deployment

### Runtime assumptions

- Python worker runtime
- Tested matrix: Python `3.13.x`, `azure-functions>=1.21.0`, `azure-storage-blob>=12.19.0`, `azure-core>=1.30.0`, `pyodbc>=5.1.0`
- HTTP route prefix from `host.json` is `api`
- Function timeout from `host.json` is `00:05:00`

### Endpoints and auth

- `GET /api/health` -> Anonymous
- `POST /api/session-log/upsert-field` -> Function auth key required
- `POST /api/session-log/upsert-batch` -> Function auth key required
- `GET /api/session-log/get-session?sessionId=...` -> Function auth key required

### Function App settings

Required:

- `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`

Optional:

- `SESSION_LOG_CONTAINER` (default `session-logs`)
- `SESSION_LOG_DATE_PARTITION` (default `false`)
- `SESSION_LOG_PARTITION_LOOKUP_FALLBACK` (default `true`)
- `SESSION_LOG_PARTITION_LOOKUP_RECENT_DAYS` (default `7`)
- `SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS` (default `2000`)
- `SESSION_LOG_PARTITION_LOOKUP_SCAN_PAGE_SIZE` (default `500`)
- `SESSION_LOG_PARTITION_LOOKUP_CACHE_TTL_SECONDS` (default `300`)
- `SESSION_LOG_PARTITION_LOOKUP_CACHE_MAX_ENTRIES` (default `5000`)
- `SESSION_LOG_PATH_INDEX_ENABLED` (default `true`)
- `SESSION_LOG_PATH_INDEX_PREFIX` (default `session-path-index/`)
- `SESSION_LOG_MAX_RETRIES` (default `3`)

### Function behavior notes

- Request body limit is 1 MB.
- Batch endpoint max is 100 fields.
- Incompatible field names are tracked in `incompatibleFieldNames`.
- Session lookup/write uses bounded partition fallback plus in-memory cache to avoid full scans and reduce repeated lookup latency.
- Session lookup/write persists a lightweight sessionId->blobPath index blob for faster cold-start resolution.
- If bounded scan caps are reached during write-path resolution, writes fail safe rather than selecting an ambiguous partial-match path.
- `SESSION_LOG_MAX_RETRIES` controls ETag retry attempts for concurrent blob writes.
- When `SESSION_LOG_DATE_PARTITION=true`, session blobs are stored under `sessions/YYYY/MM/DD/<sessionId>.json`.
- `SESSION_LOG_PATH_INDEX_PREFIX` is normalized to end with `/` automatically by the function.

## Ingestion Worker Deployment

The ingestion worker can run as:

- Scheduled job (recommended)
- WebJob
- Container App Job
- Any scheduled host able to run Python and reach Storage + SQL

This repository also includes container build assets for the ingestion worker:

- `Dockerfile` - builds a runnable ingestion image with Python 3.13, `pyodbc`,
  and Microsoft ODBC Driver 18 for SQL Server
- `.dockerignore` - excludes local secrets and transient files from the image
  build context

### Ingestion environment settings

Required:

- `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`
- SQL connection via either:
  - `AZURE_SQL_CONN_STR`
  - or all of: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USER`, `AZURE_SQL_PASSWORD`

Optional:

- `SESSION_LOG_CONTAINER` (default `session-logs`)
- `SESSION_LOG_BLOB_PREFIX` (default empty; set `sessions/` only when blob names actually start with `sessions/`)
- `FAILED_FIELDNAMES_CONTAINER` (default falls back to `SESSION_LOG_CONTAINER`)
- `FAILED_FIELDNAMES_BLOB_PREFIX` (default `failed-fieldnames/`)
- `AZURE_SQL_TRUST_SERVER_CERTIFICATE` (default `no`)
- `AZURE_SQL_CONNECT_RETRY` (default `3`)
- `AZURE_SQL_CONNECT_BACKOFF` (default `0.5`)
- `AZURE_SQL_EXEC_RETRY` (default `3`)
- `KPI_AGGREGATE_REFRESH_ENABLED` (default `true`)
- `KPI_AGGREGATE_REFRESH_LOOKBACK_DAYS` (default `30`)
- `KPI_AGGREGATE_REFRESH_FULL` (default `false`)
- `KPI_AGGREGATE_REFRESH_FAIL_ON_ERROR` (default `false`)
- `INGESTION_LIST_PAGE_SIZE` (default `500`)
- `FACT_ROW_DEDUPE_ENABLED` (default `true`)
- `DEAD_LETTER_CONTAINER` (optional)
- `DEAD_LETTER_DELETE_SOURCE` (default `false`)
- `INGESTION_DELETE_AFTER_SUCCESS` (default `false`)
- `INGESTION_FAILURE_ALERT_THRESHOLD` (default `5`)

Logging (all optional, see [`docs/logging.md`](docs/logging.md) for the full
reference and example `jq` recipes):

- `INGESTION_LOG_DIR` (default `logs`)
- `INGESTION_LOG_LEVEL` (default `INFO`)
- `INGESTION_LOG_ERROR_LEVEL` (default `WARNING`) - minimum level for `ingestion-errors.log`
- `INGESTION_LOG_CONSOLE_ENABLED` (default `true`)
- `INGESTION_LOG_JSON_ENABLED` (default `true`) - emits `ingestion.jsonl`
- `INGESTION_LOG_MAX_BYTES` (default `10485760` / 10 MB)
- `INGESTION_LOG_BACKUP_COUNT` (default `7`)
- `INGESTION_LOG_AUTO_SETUP` (default `true`)

Production hosts should ensure `INGESTION_LOG_DIR` points at a writable
directory that is either persisted (for local VMs) or mounted (for
containers). Uncaught exceptions are captured automatically by an installed
`sys.excepthook` and written to both `ingestion-errors.log` and
`ingestion.jsonl` before the process exits.

### Ingestion CLI

- `python blob_text_to_azure_sql.py`
- `python blob_text_to_azure_sql.py --blob <path>`
- `python blob_text_to_azure_sql.py --delete-after`
- `python blob_text_to_azure_sql.py --no-delete-after`

CLI flags override env behavior for that run.

### Ingestion behavior notes

- `INGESTION_LIST_PAGE_SIZE` controls blob listing page size during ingestion.
- `FACT_ROW_DEDUPE_ENABLED=true` deduplicates identical fact rows during extraction.
- `KPI_AGGREGATE_REFRESH_ENABLED=true` runs `dbo.usp_refresh_kpi_aggregates` once after any successful ingestion run so `dbo.kpi_aggregates` stays populated.
- `KPI_AGGREGATE_REFRESH_FAIL_ON_ERROR=true` makes the ingestion run fail if the aggregate refresh step fails; otherwise the worker logs the refresh error and keeps the successfully ingested detail rows.
- If a blob payload contains `devFlag: "dev"`, the ingestion worker skips it and deletes the source blob.
- Ingestion skips blobs already successfully processed with the same ETag.

### Local runtime fallback

When run directly, the worker loads `local.settings.json` `Values` as defaults for unset env vars.

- Empty values are ignored.
- Placeholder values like `<your-password>` are ignored.
- `local.settings.json` is local-only and gitignored; `local.settings.example.json` is the tracked template.
- `AzureWebJobsStorage` and `FUNCTIONS_WORKER_RUNTIME=python` are required for Azure Functions local emulation.

## Data and Validation Constraints

- `sessionId` is required and must match the supported session ID character set.
- Non-GUID `sessionId` values are accepted for raw ingestion and deterministically mapped to GUIDs for normalized reporting tables.
- Raw ingestion tables preserve the original source `sessionId` value for traceability.
- Invalid rows are written to `session_blob_rejection`.
- Ingestion state is tracked in `session_blob_ingestion`.
- Dead-letter writes are idempotent by source blob etag pathing.
- Canonical `flow_type` is treated as a session-level value.
- Generic wrapper hints such as `System` and `ConversationEvaluation` are not treated as canonical business flows.
- Once a canonical session flow is established in normalized tables, later blobs do not overwrite it with a conflicting wrapper flow.

## Security and Operations

- Use app settings or Key Vault references for secrets.
- Do not store production secrets in repository files.
- Keep `DEAD_LETTER_DELETE_SOURCE=false` initially.
- Keep `INGESTION_DELETE_AFTER_SUCCESS=false` initially.
- Enable destructive deletes only after monitoring confirms expected behavior.

## Smoke Test Checklist

1. Call `GET /api/health` and expect HTTP 200.
2. Call `POST /api/session-log/upsert-field` with function key and a valid payload.
3. Call `GET /api/session-log/get-session?sessionId=<id>` and verify JSON persisted.
4. Run ingestion once.
5. Verify SQL rows in:
   - `session_blob_session`
   - `session_blob_fact`
   - `session_blob_ingestion`
6. Verify reporting objects expected by Power BI exist and return rows:
   - `vw_kpi_aggregates_power_bi`
   - `vw_session_reporting_detail`
   - `vw_kpi_card_base_power_bi`
   - `vw_session_heatmap_power_bi`
7. If testing invalid input, verify `session_blob_rejection` and optional dead-letter container behavior.

## Handoff Notes For Platform Team

- Placeholders in `local.settings.json` are expected for source control safety.
- `local.settings.example.json` is the committed baseline for local configuration shape.
- Deployment should set real values through environment configuration.
- No code edits are required for environment-specific wiring.
- Power BI source guidance lives in [`docs/powerbi.md`](docs/powerbi.md).
