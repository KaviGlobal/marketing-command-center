# Handoff and Deployment

## What to change and where

### `function_app.py`

Change this file when you need to:
- modify HTTP validation or request/response behavior
- change blob path resolution or container behavior
- add or remove endpoints
- adjust session lookup, caching, or ETag retry behavior

### `blob_text_to_azure_sql.py`

Change this file when you need to:
- adjust blob listing, filtering, or deletion logic
- change SQL connection behavior
- update ingestion failure handling or dead-letter behavior
- modify how normalized reporting tables are populated

### `shared_validation.py`

Change this file when you need to:
- add or remove allowed KPI field names
- update field alias normalization
- change email, boolean, integer, or noise validation rules

### `chatbot-sessions-data-export-schema.sql`

Change this file when you need to:
- deploy new tables or alter existing schema
- update indexes and reporting views
- change ingestion or KPI refresh bookkeeping structures

## Deployment checklist

1. Create Azure Storage Account and container.
2. Create Azure SQL Server and database.
3. Run `chatbot-sessions-data-export-schema.sql` in the target database.
4. Deploy `function_app.py` to Azure Functions.
5. Configure app settings in the Function App.
6. Deploy or schedule the ingestion worker runtime.
7. Verify `GET /api/health` returns `200`.
8. Send a test ingest request to `POST /api/session-log/upsert-field` or `POST /api/session-log/upsert-batch`.
9. Confirm session blob storage write.
10. Run the ingestion worker and verify SQL tables.
11. Confirm `session_blob_ingestion_run` and `session_blob_fact` / `session_blob_rejection` records.

## KT talking points

Explain:
- The function app writes JSON to blobs, not SQL.
- The ingestion worker validates and writes SQL rows separately.
- `local.settings.example.json` is the local template.
- `chatbot-sessions-data-export-schema.sql` is the production schema.
- `DEPLOYMENT.md` is the deployment runbook.
- The main client inputs are storage and SQL credentials.
