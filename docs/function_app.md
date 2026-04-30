# function_app.py Reference

This page documents every major helper and endpoint in `function_app.py`.

## Configuration helpers

- `get_int_env(name, default, minimum=0)`
  - Reads an integer environment variable.
  - Applies a default if the value is missing or invalid.
  - Enforces a minimum bound.

- `get_blob_service_client()`
  - Lazily creates a shared `BlobServiceClient` from `AZURE_STORAGE_CONNECTION_STRING` or `AzureWebJobsStorage`.
  - Throws if the connection string is missing.

- `get_container_client()`
  - Returns a shared `ContainerClient` for `SESSION_LOG_CONTAINER`.
  - Reuses the same client across function executions.

## Logging and request helpers

- `utcnow()`
  - Returns timezone-aware current UTC time.

- `get_request_context(req, session_id=None)`
  - Builds a consistent request context for logs and responses.
  - Uses `x-request-id`, `x-ms-request-id`, or generates a UUID.
  - Uses `x-correlation-id`, `x-ms-correlation-id`, or request ID.

- `log_api_event(stage, **context)`
  - Emits structured JSON logs for lifecycle events.
  - Includes `component`, `stage`, timestamp, and request context.

- `ensure_session_container_initialized()`
  - Creates the blob container if it does not exist.
  - Uses `create_container(timeout=5)` and tolerates `ResourceExistsError`.

## Timestamp helpers

- `parse_timestamp(value)`
  - Parses an ISO timestamp string, including `Z` suffix.
  - Returns `utcnow()` on invalid or missing values.

- `try_parse_timestamp(value)`
  - Attempts to parse a timestamp and returns `None` on failure.
  - Used for validation checks where invalid timestamps should be rejected.

## Blob path and lookup helpers

- `get_blob_path(session_id, captured_at=None)`
  - Computes session blob path based on `SESSION_LOG_DATE_PARTITION`.
  - Defaults to `sessions/<sessionId>.json`.
  - When partitioning is enabled, uses `sessions/YYYY/MM/DD/<sessionId>.json`.

- `get_partitioned_blob_path(session_id, captured_at)`
  - Returns the same partitioned path when a timestamp is known.

- `is_valid_session_blob_path(session_id, blob_path)`
  - Ensures a blob path belongs to the session under `sessions/`.

- `get_session_path_index_blob_path(session_id)`
  - Returns the optional index blob path under `SESSION_LOG_PATH_INDEX_PREFIX`.

- `read_session_path_index(session_id)`
  - Reads a cached path from the optional index blob.
  - Validates the path before returning it.

- `write_session_path_index(session_id, blob_path)`
  - Writes the resolved path to the index blob for faster future lookups.
  - Best-effort only; errors are logged and ignored.

- `cache_session_blob_path(session_id, blob_path)`
  - Stores a session blob path in memory with TTL.
  - Enforces max cache entries via LRU eviction.

- `remember_session_blob_path(session_id, blob_path)`
  - Updates both the in-memory cache and the optional index blob.

- `get_cached_session_blob_client(session_id)`
  - Returns a cached `BlobClient` if the path is still valid and the blob exists.

- `find_partition_blob_by_recent_dates(session_id, captured_at)`
  - Attempts date-partitioned path resolution over recent days.
  - Checks the captured date first, then recent partitions.

- `find_partition_blob_by_bounded_scan(session_id, legacy_path)`
  - Scans up to `SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS` for a matching blob.
  - Returns the newest match and whether the scan was truncated.

- `blob_exists(blob_client)`
  - Checks whether a blob exists via `get_blob_properties()`.

- `get_session_blob_client(session_id, captured_at=None, for_write=False)`
  - Resolves the best blob path for reads or writes.
  - Uses preferred path, legacy path, cache, index, recent partitions, and bounded scans.
  - For writes, it may return the preferred path even if the blob does not exist yet.
  - For writes, if the bounded scan is ambiguous, it fails safe and returns `None`.

## Payload validation helpers

- `generate_event_id(session_id, field_name, field_value, timestamp)`
  - Creates a deterministic 32-character hash for event de-duplication.

- `parse_is_kpi(value)`
  - Normalizes boolean-like values for `isKpi`.
  - Returns `True`, `False`, or `None` if the value is absent.

- `validate_payload(data, require_field_value=False)`
  - Validates session payloads for both single-field and batch ingestion.
  - Checks `sessionId`, canonical `fieldName`, field value length, email format, and KPI rule compliance.
  - If `isKpi=true`, the field name must be an allowed KPI field.

- `is_field_name_error(reason)`
  - Detects whether a validation failure is due to `fieldName` issues.

## Document operations

- `build_new_document(data, captured_at)`
  - Constructs a fresh JSON session document skeleton.
  - Includes `sessionId`, `botId`, `user`, `createdAtUtc`, `lastUpdatedUtc`, `responses`, `kpis`, `events`, and `incompatibleFieldNames`.

- `append_incompatible_field_names(doc, field_names)`
  - Adds invalid field names to the session document metadata.

- `merge_into_document(doc, data, captured_at, incompatible_field_names=None, include_field_values=True)`
  - Merges a new payload into an existing session document.
  - Updates session metadata, normalizes field names, and appends events.
  - Prevents duplicate event IDs.

- `read_session_document(blob_client)`
  - Reads and parses JSON from an existing blob.
  - Returns `None` if the blob does not exist.
  - If the blob content is invalid JSON, returns an empty document so the next write can self-heal.

- `upload_document(blob_client, doc, etag)`
  - Uploads the JSON document with `IfNotModified` optimistic concurrency when an ETag is provided.

- `upsert_blob_document(data, captured_at, incompatible_field_names=None)`
  - Creates or updates a session blob document.
  - Reads the current document, merges the payload, and writes with ETag retries.
  - Returns success state, blob path, and error message.

- `upsert_blob_document_batch(session_id, payloads, session_common, now, incompatible_field_names=None, include_field_values=True)`
  - Reads the session document once, applies multiple payloads, and writes back a single merged document.
  - Supports batch-level field validation and incompatible field name preservation.

## Response and status helpers

- `reject_response(message, status_code=400)`
  - Returns a standardized JSON error response.

- `batch_failure_status(rejected)`
  - Chooses `409` for ETag or path-resolution conflicts; otherwise returns `400`.

## HTTP endpoints

### `health(req)`

- Route: `GET /api/health`
- Auth: anonymous
- Returns `200` with `{"status":"healthy"}`.

### `upsert_field(req)`

- Route: `POST /api/session-log/upsert-field`
- Auth: function key required
- Validates request body size, JSON syntax, `sessionId`, `fieldName`, `fieldValue`, `userEmail`, and `capturedAtUtc`.
- On invalid `fieldName`, may still write the session document with `incompatibleFieldNames`.
- Writes a merged session blob and returns `blobPath`, `sessionId`, `updatedAtUtc`, `requestId`, and `correlationId`.
- Returns `413` when body exceeds 1 MB.

### `upsert_batch(req)`

- Route: `POST /api/session-log/upsert-batch`
- Auth: function key required
- Validates up to 100 fields.
- Supports optional `botId`, `userId`, `userEmail`, `userDisplayName`, and per-field `eventId` and `capturedAtUtc`.
- Preserves invalid field names in `incompatibleFieldNames` when possible.
- Returns a summary including `fieldsProcessed`, `fieldsRejected`, `incompatibleFieldNames`, and rejected field reasons.

### `get_session(req)`

- Route: `GET /api/session-log/get-session`
- Auth: function key required
- Query parameter: `sessionId`.
- Resolves the blob path using cache/index/partition/fallback logic.
- Returns the raw stored JSON document or `404` if not found.
