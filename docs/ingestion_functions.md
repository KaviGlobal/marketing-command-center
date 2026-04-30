# blob_text_to_azure_sql.py Reference

This page documents every major helper and processing function in `blob_text_to_azure_sql.py`.

## Local settings and configuration

- `load_local_settings_env_defaults()`
  - Loads environment variables from `local.settings.json` if they are unset.
  - Skips placeholders like `<your-password>`.

- `build_sql_connection_string()`
  - Builds the ODBC connection string from `AZURE_SQL_CONN_STR` or individual SQL env vars.
  - Requires either `AZURE_SQL_CONN_STR` or `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USER`, and `AZURE_SQL_PASSWORD`.
- `get_int_env()` / `get_bool_env()`
  - Parse integer and boolean environment settings with safe defaults and warning logs for invalid values.

- `get_blob_service_client()`
  - Creates a new `BlobServiceClient` for each run.

- `get_dead_letter_container_client()`
  - Ensures the configured dead letter container exists and returns its client.

## Blob helpers

- `load_blob_text(blob_client)`
  - Downloads blob content and decodes it as UTF-8.
  - Replaces invalid UTF-8 bytes rather than failing.

- `get_failed_fieldnames_container_client()`
  - Creates or reuses the container for failed fieldname metadata.

- `get_failed_fieldnames_blob_client(source_blob_path)`
  - Returns a blob client under `FAILED_FIELDNAMES_BLOB_PREFIX` for rejection metadata.

- `get_failed_field_names_from_rejections(rejected_rows)`
  - Extracts unique failed field names from rejection rows.

- `write_failed_fieldnames_blob(source_blob_path, field_names)`
  - Writes a JSON marker blob with failed field names.
  - Deletes the marker if `field_names` is empty.

## Retry and SQL helpers

- `connect_with_retry()`
  - Tries to connect to Azure SQL using `pyodbc.connect`.
  - Retries up to `AZURE_SQL_CONNECT_RETRY` times with exponential backoff.

- `execute_with_retry(cursor, query, params=None)`
  - Executes a SQL statement with retry on transient errors.

- `executemany_with_retry(cursor, query, rows)`
  - Executes bulk inserts/updates with retry.

## Validation helpers

- `validate_session_id(session_id)`
  - Normalizes and validates session ID using shared validation.

- `validate_field_name(field_name)`
  - Normalizes and validates non-KPI field names.

- `validate_kpi_field_name(field_name)`
  - Validates KPI field names against the shared KPI whitelist.

- `validate_field_value(field_name, field_value, is_kpi)`
  - Validates a field value after normalization.

- `validate_session_payload(payload)`
  - Validates top-level session JSON structure, metadata, timestamps, and KPI field names.
  - Returns `session_id` and a list of validation errors.

## Normalization helpers

- `get_dev_flag(payload)`
  - Returns the normalized `devFlag` value from the payload.

- `normalize_int(value)`
  - Converts a normalized string to an integer if valid.

- `normalize_time(value)`
  - Converts a value to a `datetime` with `parse_timestamp()`.

- `get_kpi_value(payload, field_name)`
  - Resolves current or aliased KPI values from the payload.
  - Supports canonical and legacy names such as `flowType`, `sessionStartTimeUtc`, and `sessionEndTimeUtc`.

- `has_any_kpi_fields(payload, keys)`
  - Returns whether any of the requested KPI keys are present in the payload.

- `normalize_session_metadata(payload)`
  - Extracts and normalizes session metadata for the `session_blob_session` table.

## SQL upsert helpers

- `upsert_session_record(cursor, metadata)`
  - Inserts or updates the `session_blob_session` row via `MERGE`.

- `extract_approved_rows(blob_path, payload)`
  - Transforms payload fields into approved fact rows and rejection rows.
  - Validates events, responses, KPI sections, and top-level `fieldName` / `fieldValue` entries.
  - Deduplicates rows if `FACT_ROW_DEDUPE_ENABLED=true`.

- `upsert_sessions_table(cursor, payload, session_id)`
  - Inserts or updates the normalized `dbo.sessions` record.

- `upsert_drop_off_nodes(cursor, payload, session_id)`
  - Inserts or updates the normalized `dbo.drop_off_nodes` record.

- `upsert_satisfaction_feedback(cursor, payload, session_id)`
  - Inserts or updates the normalized `dbo.satisfaction_feedback` record.

- `upsert_prospect_inquiries(cursor, payload, session_id)`
  - Inserts or updates the normalized `dbo.prospect_inquiries` record.

- `upsert_career_inquiries(cursor, payload, session_id)`
  - Inserts or updates the normalized `dbo.career_inquiries` record.

- `upsert_partner_inquiries(cursor, payload, session_id)`
  - Inserts or updates the normalized `dbo.partner_inquiries` record.

- `upsert_vendor_inquiries(cursor, payload, session_id)`
  - Inserts or updates the normalized `dbo.vendor_inquiries` record.

- `upsert_bot_optimization_metrics(cursor, payload, session_id)`
  - Inserts or updates the normalized `dbo.bot_optimization_metrics` record.

- `upsert_normalized_tables(cursor, payload, session_id)`
  - Determines which normalized tables should be updated based on payload fields.
  - Writes session-level and detail-level KPI tables only when relevant fields are present.

## Ingestion state helpers

- `get_ingestion_state(cursor, blob_path)`
  - Loads existing ingestion status for a blob.

- `delete_existing_blob_rows(cursor, blob_path)`
  - Removes prior `session_blob_fact` and `session_blob_rejection` rows for a blob.

- `insert_fact_rows(cursor, rows)`
  - Inserts approved fact rows in bulk.

- `insert_rejection_rows(cursor, rows)`
  - Inserts rejection rows in bulk.

- `upsert_ingestion_state(cursor, blob_path, last_modified, etag, status, row_count, rejection_count, error_message=None)`
  - Upserts the blob-level ingestion result row.

- `upsert_ingestion_run_history(cursor, run_id, started_at_utc, completed_at_utc, selected_blob_path, status, blobs_processed, blobs_succeeded, blobs_rejected, blobs_failed, blobs_skipped, sql_connect_retries, sql_execute_retries, sql_executemany_retries, error_message=None)`
  - Writes the ingestion run summary for monitoring.
- `refresh_kpi_aggregates(cursor, lookback_days=30, full_refresh=False)`
  - Executes `dbo.usp_refresh_kpi_aggregates` and returns the reported inserted row count and refresh run ID.

## Blob processing logic

- `process_blob(cursor, blob_client, run_id, delete_after=False)`
  - Main per-blob ingestion workflow.
  - Skips duplicates when the blob has already succeeded with the same ETag.
  - Parses JSON and logs `json_parsed` or `json_parse_failed` events.
  - Rejects invalid blobs and dead-letters them when needed.
  - If the payload contains `devFlag: "dev"`, deletes the blob and marks it as skipped.
  - Writes session metadata, normalized tables, approved facts, and rejection rows.
  - Writes `FAILED_FIELDNAMES` metadata blobs for rejected field names.
  - Deletes the source blob when `delete_after` is enabled and ingestion succeeded.

- `list_blobs_to_process(prefix)`
  - Enumerates blobs under `BLOB_PREFIX` using paging.

- `sanitize_dead_letter_component(value)`
  - Normalizes values for safe dead-letter path components.

- `build_dead_letter_path(blob_name, etag)`
  - Builds a deterministic dead-letter path from the source blob name and ETag.

- `dead_letter_blob(blob_client, reason)`
  - Copies failed or rejected blobs to `DEAD_LETTER_CONTAINER`.
  - Preserves source payload, content type, and failure metadata.
  - Optionally deletes the source blob if `DEAD_LETTER_DELETE_SOURCE=true`.

## Run entrypoints

- `run_ingestion(selected_blob=None, delete_after=False)`
  - Main ingestion driver.
  - Opens a SQL connection, iterates blobs, and processes each one.
  - Refreshes `dbo.kpi_aggregates` once after a run when `KPI_AGGREGATE_REFRESH_ENABLED=true` and at least one blob succeeded.
  - Commits per-blob transactions and updates run history.
  - Logs `run_started`, `run_completed`, and failure alerts.

- `parse_arguments()`
  - Parses CLI flags:
    - `--blob <path>` to ingest a single blob.
    - `--delete-after` to delete blobs after successful ingestion.
    - `--no-delete-after` to retain blobs.

- `main()`
  - CLI entrypoint that runs ingestion and returns process exit status.
