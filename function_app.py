import hashlib
import json
import logging
import os
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import azure.functions as func
from azure.core import MatchConditions
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceModifiedError, ResourceNotFoundError
from azure.storage.blob import BlobClient, BlobServiceClient, ContentSettings, ContainerClient

from shared_validation import (
    ALLOWED_KPI_FIELDS,
    canonical_field_name,
    normalize_bool,
    normalize_text,
    validate_email,
    validate_field_name,
    validate_field_value,
    validate_session_id,
)

# This Azure Functions app stores session updates as JSON documents in blob storage.
# Each request may append a single field or multiple fields to a session document.
# The blob lookup layer supports partitioned sessions, legacy paths, path index entries,
# in-memory caching, and bounded scans to resolve a sessionId to its persistent blob path.
# Document updates use optimistic concurrency with ETag retries to prevent lost writes
# when multiple concurrent requests target the same session.
app = func.FunctionApp()

# Function app configuration and environment-driven constants.
# These values control blob storage behavior, cache size, retry limits, and request limits.
# They are intentionally kept in module scope so the function app can reuse shared clients and settings.

# --- Configuration -----------------------------------------------------------


def get_int_env(name: str, default: int, minimum: int = 0) -> int:
    """Load an integer environment variable with default and minimum bounds."""
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

# Support both Azure Function App default (AzureWebJobsStorage) and custom env var.
BLOB_CONN_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING") or os.environ.get("AzureWebJobsStorage")
# Required connection string used by all blob storage operations.

# Default blob container name for session documents (Power Automate writes to "session-logs").
SESSION_CONTAINER = os.environ.get("SESSION_LOG_CONTAINER", "session-logs")
MAX_RETRIES = get_int_env("SESSION_LOG_MAX_RETRIES", 3, minimum=1)
PARTITION_LOOKUP_FALLBACK = os.environ.get("SESSION_LOG_PARTITION_LOOKUP_FALLBACK", "true").lower() in {"true", "1", "yes"}
PARTITION_LOOKUP_RECENT_DAYS = get_int_env("SESSION_LOG_PARTITION_LOOKUP_RECENT_DAYS", 7, minimum=0)
PARTITION_LOOKUP_SCAN_MAX_BLOBS = get_int_env("SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS", 2000, minimum=1)
PARTITION_LOOKUP_SCAN_PAGE_SIZE = get_int_env("SESSION_LOG_PARTITION_LOOKUP_SCAN_PAGE_SIZE", 500, minimum=1)
PARTITION_LOOKUP_CACHE_TTL_SECONDS = get_int_env("SESSION_LOG_PARTITION_LOOKUP_CACHE_TTL_SECONDS", 300, minimum=1)
PARTITION_LOOKUP_CACHE_MAX_ENTRIES = get_int_env("SESSION_LOG_PARTITION_LOOKUP_CACHE_MAX_ENTRIES", 5000, minimum=1)
SESSION_PATH_INDEX_ENABLED = os.environ.get("SESSION_LOG_PATH_INDEX_ENABLED", "true").lower() in {"true", "1", "yes"}
SESSION_PATH_INDEX_PREFIX = os.environ.get("SESSION_LOG_PATH_INDEX_PREFIX", "session-path-index/").strip()
if SESSION_PATH_INDEX_PREFIX and not SESSION_PATH_INDEX_PREFIX.endswith("/"):
    SESSION_PATH_INDEX_PREFIX += "/"

MAX_REQUEST_BODY_SIZE = 1_048_576  # 1 MB
MAX_BATCH_FIELDS = 100

blob_service_client: Optional[BlobServiceClient] = None
container_client: Optional[ContainerClient] = None
# In-memory LRU cache mapping sessionId to resolved blob path and expiration timestamp.
session_blob_path_cache: "OrderedDict[str, Tuple[str, float]]" = OrderedDict()
session_blob_path_cache_lock = Lock()
container_init_attempted = False
container_init_lock = Lock()


def get_blob_service_client() -> BlobServiceClient:
    """Create or return a shared BlobServiceClient for the configured storage account."""
    global blob_service_client
    if blob_service_client is None:
        if not BLOB_CONN_STR:
            raise EnvironmentError("AZURE_STORAGE_CONNECTION_STRING or AzureWebJobsStorage environment variable is required")
        # Lazily initialize the client so the module is import-safe in Azure Functions.
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    return blob_service_client


def get_container_client() -> ContainerClient:
    """Create or return a shared container client for session blob storage."""
    global container_client
    if container_client is None:
        # Reuse a single container client across function executions.
        container_client = get_blob_service_client().get_container_client(SESSION_CONTAINER)
    return container_client


# --- Helpers ------------------------------------------------------------------

def utcnow() -> datetime:
    """Return the current UTC datetime with timezone awareness."""
    return datetime.now(timezone.utc)


def get_request_context(req: func.HttpRequest, session_id: Optional[str] = None) -> Dict[str, str]:
    """Build stable request context identifiers for structured logs and responses."""
    request_id = (
        normalize_text(req.headers.get("x-request-id"))
        or normalize_text(req.headers.get("x-ms-request-id"))
        or str(uuid.uuid4())
    )
    correlation_id = (
        normalize_text(req.headers.get("x-correlation-id"))
        or normalize_text(req.headers.get("x-ms-correlation-id"))
        or request_id
    )
    return {
        "request_id": request_id,
        "correlation_id": correlation_id,
        "session_id": session_id or "",
    }


def log_api_event(stage: str, **context: Any) -> None:
    """Emit structured API logs for observability across request lifecycle stages."""
    payload = {"component": "http_api", "stage": stage, "at_utc": utcnow().isoformat(), **context}
    logging.info(json.dumps(payload, default=str))


def ensure_session_container_initialized() -> None:
    """Create the session container if it does not already exist."""
    global container_init_attempted
    if container_init_attempted:
        return
    with container_init_lock:
        if container_init_attempted:
            return
        try:
            # Create the container if it does not already exist. A ResourceExistsError is fine.
            get_container_client().create_container(timeout=5)
            container_init_attempted = True
        except ResourceExistsError:
            # Container already exists; mark initialization complete.
            container_init_attempted = True
        except AzureError as exc:
            # Preserve the last known failed init state for retry logic.
            logging.warning("Session container init check failed for %s: %s", SESSION_CONTAINER, exc)
            container_init_attempted = False


def parse_timestamp(value: Optional[str]) -> datetime:
    """Parse an ISO timestamp string into a timezone-aware datetime, defaulting to now on failure."""
    if value:
        try:
            # Normalize UTC Z suffix into a parseable ISO offset.
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return utcnow()
    return utcnow()


def try_parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse an ISO timestamp string into a timezone-aware datetime, returning None on failure."""
    if value is None:
        return None
    try:
        text = normalize_text(value)
        if not text:
            return None
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def get_blob_path(session_id: str, captured_at: Optional[datetime] = None) -> str:
    """Return the blob path for a session, partitioned by date if configured."""
    use_partition = os.environ.get("SESSION_LOG_DATE_PARTITION", "false").lower() == "true"
    if use_partition:
        # Use a date-partitioned path when configured, to spread session blobs across folders.
        dt = captured_at or utcnow()
        return f"sessions/{dt:%Y/%m/%d}/{session_id}.json"
    return f"sessions/{session_id}.json"


def get_partitioned_blob_path(session_id: str, captured_at: datetime) -> str:
    """Return a date-partitioned blob path for a session blob."""
    return f"sessions/{captured_at:%Y/%m/%d}/{session_id}.json"


def is_valid_session_blob_path(session_id: str, blob_path: str) -> bool:
    """Check whether the blob path is a valid session document path for the given session."""
    if not blob_path or not blob_path.startswith("sessions/"):
        return False
    if blob_path == f"sessions/{session_id}.json":
        return True
    return blob_path.endswith(f"/{session_id}.json")


def get_session_path_index_blob_path(session_id: str) -> str:
    """Return the blob path for the optional session path index entry."""
    return f"{SESSION_PATH_INDEX_PREFIX}{session_id}.txt"


def read_session_path_index(session_id: str) -> Optional[str]:
    """Read the optional session path index entry for a sessionId, if enabled."""
    if not SESSION_PATH_INDEX_ENABLED:
        return None
    # Session path index stores the resolved blob path for a session id in a separate text blob.
    # If a session document has already been resolved, the index can avoid expensive blob scans.
    index_blob_client = get_container_client().get_blob_client(get_session_path_index_blob_path(session_id))
    try:
        payload = index_blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return None
    except AzureError as exc:
        logging.warning("Failed reading session path index for %s: %s", session_id, exc)
        return None

    text = payload.decode("utf-8", errors="ignore") if isinstance(payload, bytes) else str(payload)
    blob_path = normalize_text(text)
    if not is_valid_session_blob_path(session_id, blob_path):
        logging.warning("Ignoring invalid session path index for %s: %s", session_id, blob_path)
        return None
    return blob_path


def write_session_path_index(session_id: str, blob_path: str) -> None:
    """Persist a resolved session blob path into the optional index for faster lookup."""
    if not SESSION_PATH_INDEX_ENABLED or not is_valid_session_blob_path(session_id, blob_path):
        return
    # Persist the resolved session blob path to the index so future lookups can avoid scans.
    # This index is best-effort and can be rebuilt if stale, because blob existence is revalidated.
    index_blob_client = get_container_client().get_blob_client(get_session_path_index_blob_path(session_id))
    try:
        index_blob_client.upload_blob(
            blob_path.encode("utf-8"),
            overwrite=True,
            content_settings=ContentSettings(content_type="text/plain"),
        )
    except AzureError as exc:
        logging.warning("Failed writing session path index for %s to %s: %s", session_id, blob_path, exc)


def cache_session_blob_path(session_id: str, blob_path: str) -> None:
    """Store a session blob path in the in-memory cache with a TTL."""
    expires_at = time.time() + PARTITION_LOOKUP_CACHE_TTL_SECONDS
    with session_blob_path_cache_lock:
        session_blob_path_cache[session_id] = (blob_path, expires_at)
        session_blob_path_cache.move_to_end(session_id)
        # Evict oldest cache entries when exceeding the configured maximum.
        while len(session_blob_path_cache) > PARTITION_LOOKUP_CACHE_MAX_ENTRIES:
            session_blob_path_cache.popitem(last=False)


def remember_session_blob_path(session_id: str, blob_path: str) -> None:
    """Update both the cache and the optional path index with the resolved blob path."""
    cache_session_blob_path(session_id, blob_path)
    write_session_path_index(session_id, blob_path)


def get_cached_session_blob_client(session_id: str) -> Optional[BlobClient]:
    """Return a cached blob client if the session path is still valid and exists."""
    with session_blob_path_cache_lock:
        entry = session_blob_path_cache.get(session_id)
        if not entry:
            return None
        blob_path, expires_at = entry
        if expires_at <= time.time():
            session_blob_path_cache.pop(session_id, None)
            return None
        session_blob_path_cache.move_to_end(session_id)

    # Confirm the cached blob still exists before returning the client.
    # The cache stores a candidate path, but the blob may have been deleted or moved.
    blob_client = get_container_client().get_blob_client(blob_path)
    if blob_exists(blob_client):
        return blob_client

    # Remove stale cache entry when the blob no longer exists.
    with session_blob_path_cache_lock:
        session_blob_path_cache.pop(session_id, None)
    return None


def find_partition_blob_by_recent_dates(session_id: str, captured_at: Optional[datetime]) -> Optional[BlobClient]:
    """Attempt to find a partitioned session blob by checking recent dates first."""
    checked_paths = set()
    candidates: List[str] = []

    if captured_at is not None:
        captured_utc = captured_at if captured_at.tzinfo else captured_at.replace(tzinfo=timezone.utc)
        candidates.append(get_partitioned_blob_path(session_id, captured_utc.astimezone(timezone.utc)))

    # Search recent date partitions in descending order so the newest matching blob is found quickly.
    now = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    for day_offset in range(PARTITION_LOOKUP_RECENT_DAYS + 1):
        dt = now - timedelta(days=day_offset)
        candidates.append(get_partitioned_blob_path(session_id, dt))

    for path in candidates:
        if path in checked_paths:
            continue
        checked_paths.add(path)
        blob_client = get_container_client().get_blob_client(path)
        if blob_exists(blob_client):
            return blob_client
    return None


def find_partition_blob_by_bounded_scan(session_id: str, legacy_path: str) -> Tuple[Optional[BlobClient], bool]:
    """Scan a limited number of blobs to find a matching session path when no direct path is available."""
    # Bounded scan is a fallback for partitioned storage when other lookups fail.
    suffix = f"/{session_id}.json"
    latest_match = None
    scanned = 0
    # Page through the blob list to enforce the configured scan limit.
    pager = get_container_client().list_blobs(
        name_starts_with="sessions/",
        results_per_page=PARTITION_LOOKUP_SCAN_PAGE_SIZE,
    ).by_page()
    for page in pager:
        for blob in page:
            scanned += 1
            # Match either the legacy unpartitioned path or a partitioned path ending in the sessionId.
            if blob.name == legacy_path or blob.name.endswith(suffix):
                # Keep the newest matching blob when multiple candidates exist.
                if latest_match is None or blob.last_modified > latest_match.last_modified:
                    latest_match = blob
            if scanned >= PARTITION_LOOKUP_SCAN_MAX_BLOBS:
                if latest_match is None:
                    logging.info(
                        "Partition lookup scan limit reached (%s blobs) for session %s with no match",
                        PARTITION_LOOKUP_SCAN_MAX_BLOBS,
                        session_id,
                    )
                    return None, True
                logging.warning(
                    "Partition lookup scan limit reached (%s blobs) for session %s; found match %s but result is partial",
                    PARTITION_LOOKUP_SCAN_MAX_BLOBS,
                    session_id,
                    latest_match.name,
                )
                return get_container_client().get_blob_client(latest_match.name), True

    if latest_match is not None:
        return get_container_client().get_blob_client(latest_match.name), False
    return None, False


def blob_exists(blob_client: BlobClient) -> bool:
    """Return True if the specified blob exists in Azure Blob Storage."""
    try:
        blob_client.get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False


def get_session_blob_client(
    session_id: str,
    captured_at: Optional[datetime] = None,
    for_write: bool = False,
) -> Optional[BlobClient]:
    """Resolve the best session blob client for reads or writes, using partitioning, cache, and index lookups."""
    preferred_path = get_blob_path(session_id, captured_at)
    preferred_client = get_container_client().get_blob_client(preferred_path)
    if blob_exists(preferred_client):
        remember_session_blob_path(session_id, preferred_path)
        return preferred_client

    # Fall back to legacy unpartitioned path if configured partitioning does not find a document.
    legacy_path = f"sessions/{session_id}.json"
    if legacy_path != preferred_path:
        legacy_client = get_container_client().get_blob_client(legacy_path)
        if blob_exists(legacy_client):
            remember_session_blob_path(session_id, legacy_path)
            return legacy_client

    # Check an in-memory cache before doing any more expensive lookups.
    cached_client = get_cached_session_blob_client(session_id)
    if cached_client:
        return cached_client

    indexed_path = read_session_path_index(session_id)
    if indexed_path:
        indexed_client = get_container_client().get_blob_client(indexed_path)
        if blob_exists(indexed_client):
            remember_session_blob_path(session_id, indexed_path)
            return indexed_client

    allow_scan = PARTITION_LOOKUP_FALLBACK
    if allow_scan:
        recent_partition_client = find_partition_blob_by_recent_dates(session_id, captured_at)
        if recent_partition_client:
            remember_session_blob_path(session_id, recent_partition_client.blob_name)
            return recent_partition_client

        # If the date-based search misses, fall back to a bounded scan of session blobs.

        scanned_client, scan_truncated = find_partition_blob_by_bounded_scan(session_id, legacy_path)
        if scanned_client:
            if scan_truncated:
                if for_write:
                    logging.warning(
                        "Ambiguous bounded-scan match for session %s; refusing write path reuse. Increase SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS.",
                        session_id,
                    )
                    return None
                # Do not persist partial-scan matches into cache/index; keep write path fail-safe.
                logging.warning(
                    "Returning partial bounded-scan read match for session %s without caching/indexing.",
                    session_id,
                )
                return scanned_client
            remember_session_blob_path(session_id, scanned_client.blob_name)
            return scanned_client

        if for_write and scan_truncated:
            logging.warning(
                "Bounded scan truncated without reliable match for session %s; refusing write path selection. Increase SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS.",
                session_id,
            )
            return None

    if for_write:
        # If no document exists yet, allow writes to the preferred path so a new session document can be created.
        return preferred_client
    return None


def generate_event_id(session_id: str, field_name: str, field_value: str, timestamp: str) -> str:
    """Generate a deterministic eventId hash for session event deduplication."""
    # The generated eventId is stable across retries and client resubmissions, so
    # the same field update is not appended twice when already present.
    raw = f"{session_id}|{field_name}|{field_value}|{timestamp}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def parse_is_kpi(value: Any) -> Optional[bool]:
    """Normalize an isKpi value into a boolean or None if unset."""
    normalized = normalize_bool(value)
    if normalized is not None:
        return normalized == 1
    if normalize_text(value) == "":
        return False
    return None


# --- Validation ---------------------------------------------------------------

def validate_payload(data: Dict[str, Any], require_field_value: bool = False) -> Tuple[bool, str]:
    """Validate an incoming payload dictionary for required session and field information."""
    # Normalize the incoming payload fields before validating them.
    session_id = normalize_text(data.get("sessionId"))
    field_name = canonical_field_name(normalize_text(data.get("fieldName")))
    field_value = normalize_text(data.get("fieldValue"))
    is_kpi_value = parse_is_kpi(data.get("isKpi", False))
    if is_kpi_value is None:
        return False, "isKpi must be true/false (or 1/0/yes/no)"
    is_kpi = is_kpi_value
    user_email = normalize_text(data.get("userEmail"))

    ok, reason = validate_session_id(session_id)
    if not ok:
        return False, reason

    ok, reason = validate_field_name(field_name, is_kpi)
    if not ok:
        return False, reason

    if field_name in ALLOWED_KPI_FIELDS and not is_kpi:
        return False, f"isKpi must be true for KPI fieldName: {field_name}"

    # Ensure a required fieldValue is present for ingest operations.
    if require_field_value and not field_value:
        return False, "fieldValue is required"

    if user_email and not validate_email(user_email):
        return False, "Invalid userEmail"

    ok, reason = validate_field_value(field_name, field_value, is_kpi)
    if not ok:
        return False, reason

    # A payload is valid only when all required session and field rules pass.
    return True, ""


def is_field_name_error(reason: str) -> bool:
    """Detect whether a validation failure was caused by an invalid field name."""
    return reason.startswith("fieldName") or reason.startswith("Unsupported KPI fieldName")


# --- Document operations ------------------------------------------------------

def build_new_document(data: Dict[str, Any], captured_at: datetime) -> Dict[str, Any]:
    """Build a fresh session JSON document skeleton for a new session blob."""
    # The document is initialized with separate containers for responses, KPIs,
    # events, and incompatible field names to allow later retrieval and diagnosis.
    return {
        "sessionId": normalize_text(data.get("sessionId")),
        "botId": normalize_text(data.get("botId")) or None,
        "user": {
            "id": normalize_text(data.get("userId")) or None,
            "email": normalize_text(data.get("userEmail")) or None,
            "displayName": normalize_text(data.get("userDisplayName")) or None,
        },
        "createdAtUtc": captured_at.isoformat(),
        "lastUpdatedUtc": captured_at.isoformat(),
        "responses": {},
        "kpis": {},
        "events": [],
        "incompatibleFieldNames": [],
    }


def append_incompatible_field_names(doc: Dict[str, Any], field_names: Optional[List[str]]) -> None:
    """Append field names that were invalid for normal ingestion to the document metadata."""
    if not field_names:
        return
    incompatible = doc.setdefault("incompatibleFieldNames", [])
    for field_name in field_names:
        normalized = normalize_text(field_name)
        if normalized and normalized not in incompatible:
            incompatible.append(normalized)


def merge_into_document(
    doc: Dict[str, Any],
    data: Dict[str, Any],
    captured_at: datetime,
    incompatible_field_names: Optional[List[str]] = None,
    include_field_values: bool = True,
) -> Dict[str, Any]:
    """Merge a single validated payload into the existing session document."""
    session_id = normalize_text(data.get("sessionId"))
    field_name = canonical_field_name(normalize_text(data.get("fieldName")))
    field_value = normalize_text(data.get("fieldValue"))
    is_kpi_value = parse_is_kpi(data.get("isKpi", False))
    is_kpi = is_kpi_value if is_kpi_value is not None else False
    bot_id = normalize_text(data.get("botId")) or None
    user_id = normalize_text(data.get("userId")) or None
    user_email = normalize_text(data.get("userEmail")) or None
    user_display_name = normalize_text(data.get("userDisplayName")) or None
    flow_type_value = normalize_text(data.get("flowType") or data.get("kpiFlowType"))

    # Keep session metadata up to date while preserving existing values.
    doc["sessionId"] = session_id
    if not doc.get("botId") and bot_id:
        doc["botId"] = bot_id

    user = doc.setdefault("user", {})
    if not user.get("id") and user_id:
        user["id"] = user_id
    if not user.get("email") and user_email:
        user["email"] = user_email
    if not user.get("displayName") and user_display_name:
        user["displayName"] = user_display_name

    doc.setdefault("createdAtUtc", captured_at.isoformat())
    doc["lastUpdatedUtc"] = captured_at.isoformat()
    doc.setdefault("responses", {})
    doc.setdefault("kpis", {})
    doc.setdefault("events", [])
    doc.setdefault("incompatibleFieldNames", [])

    if include_field_values:
        # Store the field value in responses or KPIs depending on the isKpi flag.
        if is_kpi:
            doc["kpis"][field_name] = field_value
        else:
            doc["responses"][field_name] = field_value

        if flow_type_value:
            # Preserve the top-level provided flowType or kpiFlowType into the canonical KPI field.
            doc["kpis"]["flow_type"] = flow_type_value

        event_id = normalize_text(data.get("eventId")) or generate_event_id(
            session_id, field_name, field_value, captured_at.isoformat()
        )
        existing_ids = {e.get("eventId") for e in doc["events"] if isinstance(e, dict)}
        # Avoid duplicate events by checking whether this eventId already exists.
        # This keeps the event list stable across retries and prevents logical duplicates.
        if event_id not in existing_ids:
            doc["events"].append({
                "eventId": event_id,
                "fieldName": field_name,
                "fieldValue": field_value,
                "isKpi": is_kpi,
                "capturedAtUtc": captured_at.isoformat(),
            })

    append_incompatible_field_names(doc, incompatible_field_names)
    return doc


def read_session_document(blob_client) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Read the JSON document and ETag from a session blob, or return None if absent."""
    try:
        downloader = blob_client.download_blob()
        content = downloader.readall()
        etag = downloader.properties.etag
        if not content:
            return {}, etag
        try:
            doc = json.loads(content)
        except json.JSONDecodeError:
            # If the existing blob is corrupted/non-JSON, treat it as empty so the next write self-heals.
            logging.warning("Session blob contains invalid JSON at %s; overwriting with a fresh document", blob_client.blob_name)
            doc = {}
        return doc, etag
    except ResourceNotFoundError:
        # Missing blob means no document yet exists for this session.
        return None, None


def upload_document(blob_client, doc: Dict[str, Any], etag: Optional[str]) -> None:
    """Upload a session document to blob storage with optional ETag optimistic concurrency."""
    payload = json.dumps(doc, ensure_ascii=False).encode("utf-8")
    kwargs = {
        "overwrite": True,
        "content_settings": ContentSettings(content_type="application/json"),
    }
    if etag:
        kwargs["etag"] = etag
        kwargs["match_condition"] = MatchConditions.IfNotModified
    # Upload the serialized document with optional ETag safety for optimistic concurrency.
    blob_client.upload_blob(payload, **kwargs)


def upsert_blob_document(
    data: Dict[str, Any],
    captured_at: datetime,
    incompatible_field_names: Optional[List[str]] = None,
) -> Tuple[bool, str, str]:
    """Create or update a single session document blob with optimistic concurrency."""
    ensure_session_container_initialized()
    session_id = normalize_text(data.get("sessionId"))
    # Resolve a blob client for writing, using partitioned paths, legacy path, cache/index, or fallback heuristics.
    blob_client = get_session_blob_client(session_id, captured_at=captured_at, for_write=True)
    if not blob_client:
        return (
            False,
            get_blob_path(session_id, captured_at),
            "Failed to resolve session blob path. Increase SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS or SESSION_LOG_PARTITION_LOOKUP_RECENT_DAYS.",
        )
    blob_path = blob_client.blob_name

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Read current document and ETag for optimistic concurrency updates.
            doc, etag = read_session_document(blob_client)
            if doc is None:
                doc = build_new_document(data, captured_at)
            doc = merge_into_document(
                doc,
                data,
                captured_at,
                incompatible_field_names=incompatible_field_names,
                include_field_values=not bool(incompatible_field_names),
            )
            # If incompatible field names are present, do not persist fieldValue data
            # in the document. Instead, record the invalid field name for later inspection.
            upload_document(blob_client, doc, etag)
            remember_session_blob_path(session_id, blob_path)
            return True, blob_path, ""
        except ResourceModifiedError:
            # Retry on ETag mismatch when another writer updated the document concurrently.
            # The document may have changed between read and upload due to parallel requests.
            if attempt == MAX_RETRIES:
                return False, blob_path, "Failed to update blob after ETag retries"
            time.sleep(0.1 * attempt)
        except AzureError as exc:
            logging.error("Blob upsert error for session %s: %s", session_id, exc, exc_info=True)
            return False, blob_path, "Internal error processing session"

    return False, blob_path, "Unknown blob upsert failure"


def upsert_blob_document_batch(
    session_id: str,
    payloads: list,
    session_common: Dict[str, Any],
    now: datetime,
    incompatible_field_names: Optional[List[str]] = None,
    include_field_values: bool = True,
) -> Tuple[int, str, list]:
    """Read session document once, apply a batch of payloads, and then write the blob."""
    # Batch mode reduces the number of blob writes by merging multiple field updates
    # into a single document update. Only valid payloads are included in the merge.
    ensure_session_container_initialized()
    # Resolve the write path using the same session blob lookup strategy as single-item upserts.
    blob_client = get_session_blob_client(session_id, captured_at=now, for_write=True)
    if not blob_client:
        return (
            0,
            get_blob_path(session_id, now),
            [{
                "fieldName": "batch",
                "reason": "Failed to resolve session blob path. Increase SESSION_LOG_PARTITION_LOOKUP_SCAN_MAX_BLOBS or SESSION_LOG_PARTITION_LOOKUP_RECENT_DAYS.",
            }],
        )
    blob_path = blob_client.blob_name
    accepted = 0
    rejected = []

    for attempt in range(1, MAX_RETRIES + 1):
        # Retry on optimistic concurrency failures and other transient blob conflicts.
        try:
            doc, etag = read_session_document(blob_client)
            if doc is None:
                doc = build_new_document(session_common, now)

            accepted = 0
            rejected = []
            # Merge each validated payload into a single document to minimize blob writes.
            # This keeps batch operations atomic as a single blob upload after all merges.
            for payload in payloads:
                doc = merge_into_document(
                    doc,
                    payload,
                    parse_timestamp(payload.get("capturedAtUtc")),
                    include_field_values=include_field_values,
                )
                accepted += 1

            append_incompatible_field_names(doc, incompatible_field_names)
            upload_document(blob_client, doc, etag)
            remember_session_blob_path(session_id, blob_path)
            return accepted, blob_path, rejected
        except ResourceModifiedError:
            # If another client changed the blob, retry up to MAX_RETRIES.
            # This is important in batch mode because multiple concurrent batch writes
            # can target the same session document.
            if attempt == MAX_RETRIES:
                return 0, blob_path, [{"fieldName": "batch", "reason": "Failed to update blob after ETag retries"}]
            time.sleep(0.1 * attempt)
        except AzureError as exc:
            logging.error("Batch blob upsert error for session %s: %s", session_id, exc, exc_info=True)
            return 0, blob_path, [{"fieldName": "batch", "reason": "Internal error processing session"}]

    return 0, blob_path, [{"fieldName": "batch", "reason": "Unknown blob upsert failure"}]


def reject_response(message: str, status_code: int = 400) -> func.HttpResponse:
    """Return a standardized JSON error response for HTTP endpoints."""
    # All error responses share a consistent JSON payload shape.
    return func.HttpResponse(
        json.dumps({"success": False, "message": message}),
        status_code=status_code,
        mimetype="application/json",
    )


def batch_failure_status(rejected: List[Dict[str, Any]]) -> int:
    """Choose an appropriate HTTP status code for batch write failures."""
    reasons = " ".join(str(item.get("reason", "")) for item in rejected if isinstance(item, dict))
    # Contentions and path resolution failures are client-visible conflicts.
    if "ETag" in reasons or "resolve session blob path" in reasons:
        return 409
    return 400


# --- HTTP endpoints -----------------------------------------------------------

@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health(req: func.HttpRequest) -> func.HttpResponse:
    """Health endpoint used by monitoring to verify the function app is alive."""
    return func.HttpResponse(
        json.dumps({"status": "healthy"}),
        status_code=200,
        mimetype="application/json",
    )


@app.route(route="session-log/upsert-field", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def upsert_field(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint to upsert a single session field into blob storage."""
    # Validate request size before consuming the payload.
    body = req.get_body()
    request_ctx = get_request_context(req)
    log_api_event("request_received", route="session-log/upsert-field", body_size=len(body), **request_ctx)
    if len(body) > MAX_REQUEST_BODY_SIZE:
        log_api_event("request_rejected", route="session-log/upsert-field", reason="request_body_too_large", **request_ctx)
        return reject_response("Request body too large", 413)

    # Parse JSON payload and reject invalid JSON immediately.
    try:
        data = req.get_json()
    except ValueError:
        log_api_event("request_rejected", route="session-log/upsert-field", reason="invalid_json", **request_ctx)
        return reject_response("Invalid JSON", 400)

    request_ctx = get_request_context(req, normalize_text(data.get("sessionId")))
    is_valid, reason = validate_payload(data, require_field_value=True)
    captured_at_raw = data.get("capturedAtUtc")
    if captured_at_raw is not None and try_parse_timestamp(captured_at_raw) is None:
        log_api_event("request_validation_failed", route="session-log/upsert-field", reason="Invalid capturedAtUtc timestamp", **request_ctx)
        return reject_response("capturedAtUtc must be a valid datetime string", 400)
    captured_at = parse_timestamp(captured_at_raw)
    if not is_valid:
        log_api_event("request_validation_failed", route="session-log/upsert-field", reason=reason, **request_ctx)
        raw_field_name = data.get("fieldName")
        # Allow invalid field names to be stored under incompatibleFieldNames for analysis.
        if raw_field_name is not None and is_field_name_error(reason):
            ok, blob_path, error = upsert_blob_document(
                data,
                captured_at,
                incompatible_field_names=[raw_field_name],
            )
            if not ok:
                status = 409 if "ETag" in error or "resolve session blob path" in error else 500
                log_api_event("raw_payload_write_failed", route="session-log/upsert-field", error=error, status_code=status, **request_ctx)
                return reject_response(error, status)
            log_api_event("raw_payload_written_with_warning", route="session-log/upsert-field", blob_path=blob_path, warning=reason, **request_ctx)
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "warning": reason,
                    "blobPath": blob_path,
                    "sessionId": normalize_text(data.get("sessionId")),
                    "updatedAtUtc": captured_at.isoformat(),
                    "requestId": request_ctx["request_id"],
                    "correlationId": request_ctx["correlation_id"],
                }),
                status_code=200,
                mimetype="application/json",
            )
        return reject_response(reason, 400)

    log_api_event("request_validated", route="session-log/upsert-field", **request_ctx)
    ok, blob_path, error = upsert_blob_document(data, captured_at)
    if not ok:
        status = 409 if "ETag" in error or "resolve session blob path" in error else 500
        log_api_event("raw_payload_write_failed", route="session-log/upsert-field", error=error, status_code=status, **request_ctx)
        return reject_response(error, status)
    log_api_event("raw_payload_written", route="session-log/upsert-field", blob_path=blob_path, **request_ctx)

    return func.HttpResponse(
        json.dumps({
            "success": True,
            "blobPath": blob_path,
            "sessionId": normalize_text(data.get("sessionId")),
            "updatedAtUtc": captured_at.isoformat(),
            "requestId": request_ctx["request_id"],
            "correlationId": request_ctx["correlation_id"],
        }),
        status_code=200,
        mimetype="application/json",
    )


@app.route(route="session-log/upsert-batch", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def upsert_batch(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint to upsert multiple session fields in a single batch."""
    # Enforce request size limit before parsing payload.
    body = req.get_body()
    request_ctx = get_request_context(req)
    log_api_event("request_received", route="session-log/upsert-batch", body_size=len(body), **request_ctx)
    if len(body) > MAX_REQUEST_BODY_SIZE:
        log_api_event("request_rejected", route="session-log/upsert-batch", reason="request_body_too_large", **request_ctx)
        return reject_response("Request body too large", 413)

    # Parse JSON payload and validate required batch fields.
    try:
        data = req.get_json()
    except ValueError:
        log_api_event("request_rejected", route="session-log/upsert-batch", reason="invalid_json", **request_ctx)
        return reject_response("Invalid JSON", 400)

    session_id = normalize_text(data.get("sessionId"))
    request_ctx = get_request_context(req, session_id)
    fields = data.get("fields", [])
    if not session_id or not isinstance(fields, list) or not fields:
        log_api_event("request_rejected", route="session-log/upsert-batch", reason="missing_session_or_fields", **request_ctx)
        return reject_response("sessionId and fields are required", 400)

    if len(fields) > MAX_BATCH_FIELDS:
        log_api_event("request_rejected", route="session-log/upsert-batch", reason="too_many_fields", fields_count=len(fields), **request_ctx)
        return reject_response(f"Too many fields, max is {MAX_BATCH_FIELDS}", 400)

    now = utcnow()

    session_common = {
        "sessionId": session_id,
        "botId": data.get("botId"),
        "userId": data.get("userId"),
        "userEmail": data.get("userEmail"),
        "userDisplayName": data.get("userDisplayName"),
        "capturedAtUtc": now.isoformat(),
    }

    if normalize_text(session_common.get("userEmail")) and not validate_email(normalize_text(session_common.get("userEmail"))):
        log_api_event("request_validation_failed", route="session-log/upsert-batch", reason="invalid_user_email", **request_ctx)
        return reject_response("Invalid userEmail", 400)

    validated_payloads = []
    rejected = []
    incompatible_field_names: List[str] = []

    for item in fields:
        if not isinstance(item, dict):
            rejected.append({"fieldName": None, "reason": "Each field item must be an object"})
            continue

        # Merge batch-specific field data with the shared session metadata.
        captured_at_value = item.get("capturedAtUtc", now.isoformat())
        payload = {
            **session_common,
            "fieldName": item.get("fieldName"),
            "fieldValue": item.get("fieldValue"),
            "isKpi": item.get("isKpi", False),
            "eventId": item.get("eventId"),
            "capturedAtUtc": captured_at_value,
        }
        captured_at_raw = payload.get("capturedAtUtc")
        if captured_at_raw is not None and try_parse_timestamp(captured_at_raw) is None:
            raw_field_name = normalize_text(item.get("fieldName"))
            rejected.append({"fieldName": raw_field_name or None, "reason": "capturedAtUtc must be a valid datetime string"})
            continue
        is_valid, reason = validate_payload(payload, require_field_value=True)
        if not is_valid:
            raw_field_name = normalize_text(item.get("fieldName"))
            # Treat invalid field names as incompatible, but still allow the batch to record them when possible.
            # This preserves the session document for analysis while rejecting only invalid field values.
            if raw_field_name and is_field_name_error(reason):
                incompatible_field_names.append(raw_field_name)
            rejected.append({
                "fieldName": raw_field_name,
                "reason": reason,
            })
            continue
        validated_payloads.append(payload)
    log_api_event(
        "request_validated",
        route="session-log/upsert-batch",
        fields_received=len(fields),
        fields_valid=len(validated_payloads),
        fields_rejected=len(rejected),
        incompatible_field_names_count=len(incompatible_field_names),
        **request_ctx,
    )

    if not validated_payloads and incompatible_field_names:
        # If all fields were invalid only due to fieldName errors, preserve those
        # field names in the session document for analysis instead of rejecting the whole session.
        accepted, blob_path, batch_errors = upsert_blob_document_batch(
            session_id, validated_payloads, session_common, now, incompatible_field_names=incompatible_field_names
        )
        rejected.extend(batch_errors)
        if accepted == 0 and batch_errors:
            log_api_event("raw_payload_write_failed", route="session-log/upsert-batch", rejected_count=len(rejected), **request_ctx)
            return func.HttpResponse(
                json.dumps({"success": False, "message": "No valid fields were written", "rejected": rejected}),
                status_code=batch_failure_status(rejected),
                mimetype="application/json",
            )
        log_api_event("raw_payload_written_with_warning", route="session-log/upsert-batch", blob_path=blob_path, rejected_count=len(rejected), **request_ctx)
        return func.HttpResponse(
            json.dumps({
                "success": True,
                "sessionId": session_id,
                "blobPath": blob_path,
                "fieldsProcessed": 0,
                "fieldsRejected": len(rejected),
                "incompatibleFieldNames": sorted(set(incompatible_field_names)),
                "rejected": rejected,
                "updatedAtUtc": now.isoformat(),
                "requestId": request_ctx["request_id"],
                "correlationId": request_ctx["correlation_id"],
            }),
            status_code=200,
            mimetype="application/json",
        )

    if not validated_payloads:
        log_api_event("request_validation_failed", route="session-log/upsert-batch", reason="no_valid_fields", rejected_count=len(rejected), **request_ctx)
        return func.HttpResponse(
            json.dumps({"success": False, "message": "No valid fields were written", "rejected": rejected}),
            status_code=400,
            mimetype="application/json",
        )

    accepted, blob_path, batch_errors = upsert_blob_document_batch(
        session_id, validated_payloads, session_common, now,
        incompatible_field_names=incompatible_field_names,
        include_field_values=bool(validated_payloads),
    )
    rejected.extend(batch_errors)

    if accepted == 0:
        log_api_event("raw_payload_write_failed", route="session-log/upsert-batch", rejected_count=len(rejected), **request_ctx)
        return func.HttpResponse(
            json.dumps({"success": False, "message": "No valid fields were written", "rejected": rejected}),
            status_code=batch_failure_status(rejected),
            mimetype="application/json",
        )

    log_api_event("raw_payload_written", route="session-log/upsert-batch", blob_path=blob_path, fields_processed=accepted, fields_rejected=len(rejected), **request_ctx)
    return func.HttpResponse(
        json.dumps({
            "success": True,
            "sessionId": session_id,
            "blobPath": blob_path,
            "fieldsProcessed": accepted,
            "fieldsRejected": len(rejected),
            "incompatibleFieldNames": sorted(set(incompatible_field_names)),
            "rejected": rejected,
            "updatedAtUtc": now.isoformat(),
            "requestId": request_ctx["request_id"],
            "correlationId": request_ctx["correlation_id"],
        }),
        status_code=200,
        mimetype="application/json",
    )


@app.route(route="session-log/get-session", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def get_session(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP endpoint to retrieve the stored session document by sessionId."""
    # Validate required query parameter before attempting lookup.
    session_id = normalize_text(req.params.get("sessionId"))
    request_ctx = get_request_context(req, session_id)
    log_api_event("request_received", route="session-log/get-session", **request_ctx)
    if not session_id:
        log_api_event("request_rejected", route="session-log/get-session", reason="missing_session_id", **request_ctx)
        return reject_response("sessionId is required", 400)

    blob_client = get_session_blob_client(session_id, for_write=False)
    if not blob_client:
        log_api_event("blob_not_found", route="session-log/get-session", **request_ctx)
        return reject_response("Session not found", 404)

    try:
        downloader = blob_client.download_blob()
        content = downloader.readall()
        log_api_event("request_succeeded", route="session-log/get-session", blob_path=blob_client.blob_name, **request_ctx)
        # Return the raw stored JSON content from the blob.
        return func.HttpResponse(content, status_code=200, mimetype="application/json")
    except ResourceNotFoundError:
        log_api_event("blob_not_found", route="session-log/get-session", **request_ctx)
        return reject_response("Session not found", 404)
    except AzureError as exc:
        logging.error("Error getting session %s: %s", session_id, exc, exc_info=True)
        log_api_event("request_failed", route="session-log/get-session", error=str(exc), **request_ctx)
        return reject_response("Failed to retrieve session", 500)
