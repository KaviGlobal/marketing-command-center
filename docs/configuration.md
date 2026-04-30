# Configuration

## Requirements

- Python `3.13.x` for local CLI and ingestion worker execution.
- `pip install -r requirements.txt` to install the runtime dependencies.
- `ODBC Driver 18 for SQL Server` installed on the host running the ingestion worker.
- Azure Storage and Azure SQL reachable from both the Function App and ingestion runtime.

## Local development configuration

Use `local.settings.example.json` as the template.

Copy it to `local.settings.json` and fill in:

- `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`
- `AZURE_SQL_SERVER`
- `AZURE_SQL_DATABASE`
- `AZURE_SQL_USER`
- `AZURE_SQL_PASSWORD`

### Function host settings

The Azure Functions host is configured in `host.json`.
- `routePrefix: api` determines the API root path.
- `functionTimeout: 00:05:00` limits execution time for each function.
- `maxOutstandingRequests` and `maxConcurrentRequests` control runtime request concurrency.

Notes:
- `local.settings.json` is local-only and should not be committed.
- Placeholder values like `<your-password>` are ignored by local CLI execution.
- Local Azure Functions requires `AzureWebJobsStorage` and `FUNCTIONS_WORKER_RUNTIME=python`.

## Azure Function App environment variables

Required:
- `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`

Optional:
- `SESSION_LOG_CONTAINER` = `session-logs`
- `SESSION_LOG_DATE_PARTITION` = `false`
- `SESSION_LOG_MAX_RETRIES` = `3`
- `SESSION_LOG_PARTITION_LOOKUP_FALLBACK` = `true`
- `SESSION_LOG_PARTITION_LOOKUP_RECENT_DAYS` = `7`
- `SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS` = `2000`
- `SESSION_LOG_PARTITION_LOOKUP_SCAN_PAGE_SIZE` = `500`
- `SESSION_LOG_PARTITION_LOOKUP_CACHE_TTL_SECONDS` = `300`
- `SESSION_LOG_PARTITION_LOOKUP_CACHE_MAX_ENTRIES` = `5000`
- `SESSION_LOG_PATH_INDEX_ENABLED` = `true`
- `SESSION_LOG_PATH_INDEX_PREFIX` = `session-path-index/`

## Ingestion worker environment variables

Required:
- `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`
- Either `AZURE_SQL_CONN_STR` or all of:
  - `AZURE_SQL_SERVER`
  - `AZURE_SQL_DATABASE`
  - `AZURE_SQL_USER`
  - `AZURE_SQL_PASSWORD`

Optional:
- `SESSION_LOG_CONTAINER` = `session-logs`
- `SESSION_LOG_BLOB_PREFIX` = `sessions/`
- `FAILED_FIELDNAMES_CONTAINER` = defaults to `SESSION_LOG_CONTAINER`
- `FAILED_FIELDNAMES_BLOB_PREFIX` = `failed-fieldnames/`
- `AZURE_SQL_TRUST_SERVER_CERTIFICATE` = `no`
- `AZURE_SQL_CONNECT_RETRY` = `3`
- `AZURE_SQL_CONNECT_BACKOFF` = `0.5`
- `AZURE_SQL_EXEC_RETRY` = `3`
- `DEAD_LETTER_CONTAINER`
- `DEAD_LETTER_DELETE_SOURCE` = `false`
- `INGESTION_DELETE_AFTER_SUCCESS` = `false`
- `INGESTION_FAILURE_ALERT_THRESHOLD` = `5`
- `INGESTION_LIST_PAGE_SIZE` = `500`
- `FACT_ROW_DEDUPE_ENABLED` = `true`

## Notes

- If `AZURE_SQL_CONN_STR` is provided, it overrides the individual SQL env vars.
- Use app settings or Key Vault references for production secrets.
- Keep destructive delete settings disabled until behavior is verified.
