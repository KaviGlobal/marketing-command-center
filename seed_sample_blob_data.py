"""Curate existing sample blobs and seed additional compliant sample sessions."""

import hashlib
import json
import logging
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from azure.storage.blob import ContentSettings  # type: ignore[import]

import blob_text_to_azure_sql as worker
from shared_validation import (
    OFFERING_BLUEPRINT,
    canonical_field_name,
    normalize_flow_type,
    normalize_offering_name,
    normalize_text,
    validate_session_id,
)

logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

PROSPECT_FIELDS = {
    "lead_capture_started_flag",
    "lead_capture_completed_flag",
    "lead_name",
    "lead_email",
    "lead_company",
    "lead_phone",
    "lead_industry",
    "lead_job_title",
    "consultation_requested_flag",
    "scheduler_link_clicked_flag",
    "offering_primary",
    "offering_secondary",
    "intent_primary",
}

CAREER_FIELDS = {
    "application_intent_flag",
    "candidate_capture_started_flag",
    "candidate_capture_completed_flag",
    "candidate_name",
    "candidate_email",
    "job_interest_area",
    "job_interest_location",
}

PARTNERSHIP_FIELDS = {
    "partner_capture_started_flag",
    "partner_capture_completed_flag",
    "partner_name",
    "partner_org_name",
    "partner_email",
    "partner_type",
    "partner_consultation_requested_flag",
    "partner_consultation_booked_flag",
}

INDUSTRIES = ["Transportation", "Manufacturing", "Healthcare", "Education", "Pharma", "Rail"]
JOB_TITLES = ["Director", "Manager", "VP", "Analyst", "Engineer"]
CAREER_AREAS = ["Data Science", "Engineering", "Analytics", "Platform"]
CAREER_LOCATIONS = ["Chicago", "Remote", "Dallas", "New York"]
PARTNER_TYPES = ["Consulting", "Technology", "Channel", "Referral"]

ALL_OFFERINGS = [offering for offerings in OFFERING_BLUEPRINT.values() for offering in offerings]
OFFERING_KEYWORDS = [
    ("schedule", "Scheduling Optimization"),
    ("maintenance", "Preventative Maintenance"),
    ("document", "Document Text Extraction"),
    ("text", "Document Text Extraction"),
    ("profit", "Profitability Analytics"),
    ("reliab", "Reliability Analytics"),
    ("advana", "Advana"),
    ("fraud", "Fraud Detection"),
    ("pain", "Pain Detection"),
    ("label", "Data Labeling"),
    ("chat", "Chatbot"),
    ("agent", "AI Agent"),
    ("platform", "Data Platform"),
    ("intelligence", "Business Intelligence"),
    ("science", "Data Science & AI"),
    ("internet", "Internet of Things"),
    ("iot", "Internet of Things"),
    ("apps", "Intelligent Apps"),
    ("managed", "Managed Services"),
    ("rcm", "RCM"),
    ("transport", "Transportation"),
    ("rail", "Rail"),
    ("manufact", "Manufacturing"),
    ("health", "Healthcare"),
    ("pharma", "Pharma"),
    ("educat", "Education"),
    ("micro", "Microsoft"),
    ("aws", "AWS"),
    ("databricks", "Databricks"),
    ("snowflake", "Snowflake"),
]


def iter_blob_payloads() -> Iterable[Tuple[Any, str, Dict[str, Any]]]:
    """Yield blob handles and JSON payloads for non-helper blobs."""
    container = worker.get_blob_service_client().get_container_client(worker.BLOB_CONTAINER)
    for blob in container.list_blobs(name_starts_with=worker.BLOB_PREFIX or None):
        if worker.should_skip_blob_name(blob.name):
            continue
        blob_client = container.get_blob_client(blob.name)
        payload = json.loads(blob_client.download_blob().readall())
        yield blob_client, blob.name, payload


def collect_payload_fields(payload: Dict[str, Any]) -> List[Tuple[str, Any]]:
    """Collect canonical field/value pairs from a payload."""
    pairs: List[Tuple[str, Any]] = []
    if payload.get("fieldName") is not None:
        pairs.append((canonical_field_name(normalize_text(payload.get("fieldName"))), payload.get("fieldValue")))
    kpis = payload.get("kpis")
    if isinstance(kpis, dict):
        for key, value in kpis.items():
            pairs.append((canonical_field_name(key), value))
    return pairs


def infer_offering(value: Any) -> Optional[str]:
    """Infer a valid offering from free-form sample text when possible."""
    canonical = normalize_offering_name(value)
    if canonical:
        return canonical
    lowered = normalize_text(value).lower()
    if not lowered:
        return None
    for needle, offering in OFFERING_KEYWORDS:
        if needle in lowered:
            return offering
    return None


def deterministic_offerings(session_id: str) -> Tuple[str, str]:
    """Choose a stable primary/secondary offering pair for a session."""
    digest = hashlib.sha256(session_id.encode("utf-8")).digest()
    primary = ALL_OFFERINGS[digest[0] % len(ALL_OFFERINGS)]
    secondary = ALL_OFFERINGS[digest[1] % len(ALL_OFFERINGS)]
    if secondary == primary:
        secondary = ALL_OFFERINGS[(ALL_OFFERINGS.index(primary) + 1) % len(ALL_OFFERINGS)]
    return primary, secondary


def deterministic_index(seed: str, modulo: int) -> int:
    """Return a stable non-negative index for a string seed."""
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % modulo


def canonical_sample_session_id(raw_session_id: Any) -> str:
    """Convert legacy sample session IDs into stable GUIDs when needed."""
    normalized = normalize_text(raw_session_id)
    if not normalized:
        return ""
    ok, _ = validate_session_id(normalized)
    if ok:
        return normalized
    return str(uuid.uuid5(uuid.UUID("66666666-7777-8888-9999-aaaaaaaaaaaa"), f"legacy-sample:{normalized}"))


def choose_session_flow(profile: Dict[str, Any]) -> str:
    """Resolve a canonical flow for a session from its observed fields."""
    counts = profile["field_counts"]
    ranked = sorted(
        [("Prospect", counts["Prospect"]), ("Career", counts["Career"]), ("Partnership", counts["Partnership"])],
        key=lambda item: item[1],
        reverse=True,
    )
    if ranked[0][1] > 0:
        return ranked[0][0]
    for raw_flow in profile["raw_flows"]:
        normalized = normalize_flow_type(raw_flow)
        if normalized:
            return normalized
    return "Prospect"


def build_session_profiles() -> Dict[str, Dict[str, Any]]:
    """Aggregate existing blobs into per-session normalization targets."""
    profiles: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "field_counts": {"Prospect": 0, "Career": 0, "Partnership": 0},
        "raw_flows": [],
        "offerings": [],
        "has_secondary_offering_blob": False,
        "user_ids": set(),
        "timestamps": [],
    })
    for _, _, payload in iter_blob_payloads():
        session_id = canonical_sample_session_id(payload.get("sessionId"))
        if not session_id:
            continue
        profile = profiles[session_id]
        user_value = payload.get("user")
        if isinstance(user_value, dict):
            user_id = normalize_text(user_value.get("id"))
            if user_id:
                profile["user_ids"].add(user_id)
        for time_key in ("createdAtUtc", "lastUpdatedUtc", "timestamp"):
            if payload.get(time_key):
                profile["timestamps"].append(normalize_text(payload.get(time_key)))
        flow_candidates = [payload.get("flowType"), payload.get("kpiFlowType")]
        for field_name, field_value in collect_payload_fields(payload):
            if field_name in PROSPECT_FIELDS:
                profile["field_counts"]["Prospect"] += 1
            if field_name in CAREER_FIELDS:
                profile["field_counts"]["Career"] += 1
            if field_name in PARTNERSHIP_FIELDS:
                profile["field_counts"]["Partnership"] += 1
            if field_name == "flow_type":
                flow_candidates.append(field_value)
            if field_name in {"offering_primary", "offering_secondary"}:
                profile["offerings"].append(field_value)
                if field_name == "offering_secondary":
                    profile["has_secondary_offering_blob"] = True
        profile["raw_flows"].extend(value for value in flow_candidates if value is not None)

    for session_id, profile in profiles.items():
        profile["flow"] = choose_session_flow(profile)
        valid_offerings = [offering for offering in (infer_offering(value) for value in profile["offerings"]) if offering]
        if valid_offerings:
            primary = valid_offerings[0]
            secondary = next((off for off in valid_offerings if off != primary), None)
        else:
            primary, secondary = deterministic_offerings(session_id)
        if secondary is None:
            _, secondary = deterministic_offerings(session_id + "-secondary")
        if secondary == primary:
            _, secondary = deterministic_offerings(session_id + "-fallback")
        profile["offering_primary"] = primary
        profile["offering_secondary"] = secondary
    return profiles


def profile_has_seed_user(profile: Dict[str, Any]) -> bool:
    """Return True when the profile belongs to one of the seeded sample-user sessions."""
    return any(user_id.startswith("sample-user-") for user_id in profile["user_ids"])


def choose_existing_timestamp(profile: Dict[str, Any], session_id: str) -> datetime:
    """Use an observed timestamp when available, otherwise pick a stable Feb-Apr date."""
    parsed: List[datetime] = []
    for raw in profile["timestamps"]:
        value = worker.parse_timestamp(raw)
        if value is not None:
            parsed.append(value)
    if parsed:
        return min(parsed)
    day = choose_date(deterministic_index(session_id, 1000))
    hour = 8 + deterministic_index(session_id + "-hour", 10)
    minute = deterministic_index(session_id + "-minute", 60)
    return datetime.combine(day, time(hour=hour, minute=minute), tzinfo=timezone.utc)


def build_legacy_enrichment_payload(session_id: str, profile: Dict[str, Any]) -> Dict[str, Any]:
    """Create one consolidated blob payload to backfill sparse legacy sample sessions."""
    flow = profile["flow"]
    start_at = choose_existing_timestamp(profile, session_id)
    duration_minutes = 4 + deterministic_index(session_id + "-duration", 17)
    end_at = start_at + timedelta(minutes=duration_minutes)
    offering_primary = profile["offering_primary"]
    offering_secondary = profile["offering_secondary"]
    user_suffix = deterministic_index(session_id + "-user", 100000)
    payload: Dict[str, Any] = {
        "sessionId": session_id,
        "botId": "kavi-legacy-sample-bot",
        "createdAtUtc": start_at.isoformat().replace("+00:00", "Z"),
        "lastUpdatedUtc": end_at.isoformat().replace("+00:00", "Z"),
        "timestamp": end_at.isoformat().replace("+00:00", "Z"),
        "flowType": flow,
        "kpiFlowType": flow,
        "user": {
            "id": f"legacy-sample-user-{user_suffix:05d}",
            "email": f"legacy.sample.{user_suffix:05d}@example.com",
            "displayName": f"Legacy Sample {user_suffix:05d}",
        },
        "kpis": {
            "session_start_time": start_at.isoformat().replace("+00:00", "Z"),
            "session_end_time_utc": end_at.isoformat().replace("+00:00", "Z"),
            "engaged_flag": "true",
            "resolved_flag": "true" if deterministic_index(session_id + "-resolved", 5) != 0 else "false",
            "escalated_flag": "true" if deterministic_index(session_id + "-escalated", 11) == 0 else "false",
            "abandoned_flag": "true" if deterministic_index(session_id + "-abandoned", 13) == 0 else "false",
            "flow_type": flow,
            "satisfaction_score": str(3 + deterministic_index(session_id + "-csat", 3)),
            "response_latency_ms_avg": str(650 + deterministic_index(session_id + "-latency", 7) * 90),
            "error_flag": "false",
            "fallback_flag": "true" if deterministic_index(session_id + "-fallback", 9) == 0 else "false",
        },
    }

    if flow == "Prospect":
        payload["kpis"].update({
            "lead_capture_started_flag": "true",
            "lead_capture_completed_flag": "true" if deterministic_index(session_id + "-lead-complete", 6) != 0 else "false",
            "lead_name": f"Legacy Prospect {user_suffix:05d}",
            "lead_email": f"legacy.prospect.{user_suffix:05d}@example.com",
            "lead_company": f"Legacy Company {deterministic_index(session_id + '-company', 40):02d}",
            "lead_phone": f"312-555-{1000 + deterministic_index(session_id + '-phone', 9000):04d}",
            "lead_industry": INDUSTRIES[deterministic_index(session_id + "-industry", len(INDUSTRIES))],
            "lead_job_title": JOB_TITLES[deterministic_index(session_id + "-job", len(JOB_TITLES))],
            "consultation_requested_flag": "true" if deterministic_index(session_id + "-consult", 4) == 0 else "false",
            "scheduler_link_clicked_flag": "true" if deterministic_index(session_id + "-scheduler", 3) == 0 else "false",
            "offering_primary": offering_primary,
            "offering_secondary": offering_secondary,
            "intent_primary": ["Learn more", "Book consultation", "Evaluate solution"][deterministic_index(session_id + "-intent", 3)],
        })
    elif flow == "Career":
        payload["kpis"].update({
            "application_intent_flag": "true",
            "candidate_capture_started_flag": "true",
            "candidate_capture_completed_flag": "true" if deterministic_index(session_id + "-candidate-complete", 5) != 0 else "false",
            "candidate_name": f"Legacy Candidate {user_suffix:05d}",
            "candidate_email": f"legacy.candidate.{user_suffix:05d}@example.com",
            "job_interest_area": CAREER_AREAS[deterministic_index(session_id + "-career-area", len(CAREER_AREAS))],
            "job_interest_location": CAREER_LOCATIONS[deterministic_index(session_id + "-career-location", len(CAREER_LOCATIONS))],
        })
    else:
        payload["kpis"].update({
            "partner_capture_started_flag": "true",
            "partner_capture_completed_flag": "true" if deterministic_index(session_id + "-partner-complete", 5) != 0 else "false",
            "partner_name": f"Legacy Partner {user_suffix:05d}",
            "partner_org_name": f"Legacy Partner Org {deterministic_index(session_id + '-partner-org', 40):02d}",
            "partner_email": f"legacy.partner.{user_suffix:05d}@example.com",
            "partner_type": PARTNER_TYPES[deterministic_index(session_id + "-partner-type", len(PARTNER_TYPES))],
            "partner_consultation_requested_flag": "true" if deterministic_index(session_id + "-partner-consult", 3) == 0 else "false",
            "partner_consultation_booked_flag": "true" if deterministic_index(session_id + "-partner-booked", 7) == 0 else "false",
        })

    return payload


def enrich_legacy_sample_sessions() -> Dict[str, int]:
    """Upload consolidated enrichment blobs for sparse legacy sample sessions."""
    profiles = build_session_profiles()
    container = worker.get_blob_service_client().get_container_client(worker.BLOB_CONTAINER)
    uploaded = 0
    for session_id, profile in profiles.items():
        if profile_has_seed_user(profile):
            continue
        payload = build_legacy_enrichment_payload(session_id, profile)
        blob_name = f"sample-curated/legacy/{session_id}.json"
        container.get_blob_client(blob_name).upload_blob(
            json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )
        uploaded += 1
    return {"uploaded": uploaded}


def curate_existing_sample_blobs() -> Dict[str, int]:
    """Rewrite existing sample blobs to follow the canonical blueprint."""
    profiles = build_session_profiles()
    updated = 0
    scanned = 0
    for blob_client, blob_name, payload in iter_blob_payloads():
        scanned += 1
        raw_session_id = payload.get("sessionId")
        session_id = canonical_sample_session_id(raw_session_id)
        if not session_id or session_id not in profiles:
            continue
        profile = profiles[session_id]
        changed = False

        if payload.get("sessionId") != session_id:
            payload["sessionId"] = session_id
            changed = True

        if payload.get("fieldName") is not None:
            cleaned_field_name = canonical_field_name(normalize_text(payload.get("fieldName")))
            if payload.get("fieldName") != cleaned_field_name:
                payload["fieldName"] = cleaned_field_name
                changed = True

            if cleaned_field_name == "flow_type" and payload.get("fieldValue") != profile["flow"]:
                payload["fieldValue"] = profile["flow"]
                changed = True
            elif cleaned_field_name == "offering_primary" and payload.get("fieldValue") != profile["offering_primary"]:
                payload["fieldValue"] = profile["offering_primary"]
                changed = True
            elif cleaned_field_name == "offering_secondary" and payload.get("fieldValue") != profile["offering_secondary"]:
                payload["fieldValue"] = profile["offering_secondary"]
                changed = True

        for flow_key in ("flowType", "kpiFlowType"):
            if payload.get(flow_key) != profile["flow"]:
                payload[flow_key] = profile["flow"]
                changed = True

        kpis = payload.get("kpis")
        if isinstance(kpis, dict):
            if kpis.get("flow_type") != profile["flow"]:
                kpis["flow_type"] = profile["flow"]
                changed = True
            if profile["flow"] == "Prospect" or "offering_primary" in kpis:
                if kpis.get("offering_primary") != profile["offering_primary"]:
                    kpis["offering_primary"] = profile["offering_primary"]
                    changed = True
            if profile["flow"] == "Prospect" or "offering_secondary" in kpis:
                if kpis.get("offering_secondary") != profile["offering_secondary"]:
                    kpis["offering_secondary"] = profile["offering_secondary"]
                    changed = True

        if changed:
            blob_client.upload_blob(
                json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
            updated += 1

    return {"scanned": scanned, "updated": updated}


def generate_session_id(index: int) -> str:
    """Generate a stable session UUID for a seeded sample session."""
    return str(uuid.uuid5(uuid.UUID("11111111-2222-3333-4444-555555555555"), f"kavi-sample-session-{index}"))


def choose_date(index: int) -> date:
    """Spread generated sessions across February, March, and April 2026."""
    start = date(2026, 2, 1)
    end = date(2026, 4, 29)
    all_dates = [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
    return all_dates[(index * 11 + index // 3) % len(all_dates)]


def choose_flow(index: int) -> str:
    """Distribute generated sessions across the three canonical inquiry flows."""
    if index % 4 in {0, 1}:
        return "Prospect"
    if index % 4 == 2:
        return "Career"
    return "Partnership"


def make_timestamp(session_date: date, index: int) -> datetime:
    """Create a stable UTC timestamp with weekday and hour variety."""
    hour = 8 + (index * 3) % 10
    minute = (index * 11) % 60
    return datetime.combine(session_date, time(hour=hour, minute=minute), tzinfo=timezone.utc)


def build_sample_payload(index: int) -> Tuple[str, Dict[str, Any]]:
    """Build one compliant consolidated sample-session blob payload."""
    session_id = generate_session_id(index)
    flow = choose_flow(index)
    session_date = choose_date(index)
    start_at = make_timestamp(session_date, index)
    duration_minutes = 4 + (index % 17)
    end_at = start_at + timedelta(minutes=duration_minutes)
    offering_primary, offering_secondary = deterministic_offerings(session_id)
    user_id = f"sample-user-{index:03d}"

    payload: Dict[str, Any] = {
        "sessionId": session_id,
        "botId": "kavi-sample-bot",
        "createdAtUtc": start_at.isoformat().replace("+00:00", "Z"),
        "lastUpdatedUtc": end_at.isoformat().replace("+00:00", "Z"),
        "timestamp": end_at.isoformat().replace("+00:00", "Z"),
        "flowType": flow,
        "kpiFlowType": flow,
        "user": {
            "id": user_id,
            "email": f"{user_id}@example.com",
            "displayName": f"Sample User {index:03d}",
        },
        "kpis": {
            "session_start_time": start_at.isoformat().replace("+00:00", "Z"),
            "session_end_time_utc": end_at.isoformat().replace("+00:00", "Z"),
            "engaged_flag": "true",
            "resolved_flag": "true" if index % 5 != 0 else "false",
            "escalated_flag": "true" if index % 11 == 0 else "false",
            "abandoned_flag": "true" if index % 13 == 0 else "false",
            "flow_type": flow,
            "satisfaction_score": str(3 + (index % 3)),
            "response_latency_ms_avg": str(650 + (index % 7) * 90),
            "error_flag": "false",
            "fallback_flag": "true" if index % 9 == 0 else "false",
        },
    }

    if flow == "Prospect":
        payload["kpis"].update({
            "lead_capture_started_flag": "true",
            "lead_capture_completed_flag": "true" if index % 6 != 0 else "false",
            "lead_name": f"Prospect {index:03d}",
            "lead_email": f"prospect{index:03d}@example.com",
            "lead_company": f"Company {index % 20:02d}",
            "lead_phone": f"312-555-{1000 + index:04d}",
            "lead_industry": ["Transportation", "Manufacturing", "Healthcare", "Education", "Pharma", "Rail"][index % 6],
            "lead_job_title": ["Director", "Manager", "VP", "Analyst", "Engineer"][index % 5],
            "consultation_requested_flag": "true" if index % 4 == 0 else "false",
            "scheduler_link_clicked_flag": "true" if index % 3 == 0 else "false",
            "offering_primary": offering_primary,
            "offering_secondary": offering_secondary,
            "intent_primary": ["Learn more", "Book consultation", "Evaluate solution"][index % 3],
        })
    elif flow == "Career":
        payload["kpis"].update({
            "application_intent_flag": "true",
            "candidate_capture_started_flag": "true",
            "candidate_capture_completed_flag": "true" if index % 5 != 0 else "false",
            "candidate_name": f"Candidate {index:03d}",
            "candidate_email": f"candidate{index:03d}@example.com",
            "job_interest_area": ["Data Science", "Engineering", "Analytics", "Platform"][index % 4],
            "job_interest_location": ["Chicago", "Remote", "Dallas", "New York"][index % 4],
        })
    else:
        payload["kpis"].update({
            "partner_capture_started_flag": "true",
            "partner_capture_completed_flag": "true" if index % 5 != 0 else "false",
            "partner_name": f"Partner {index:03d}",
            "partner_org_name": f"Partner Org {index % 15:02d}",
            "partner_email": f"partner{index:03d}@example.com",
            "partner_type": ["Consulting", "Technology", "Channel", "Referral"][index % 4],
            "partner_consultation_requested_flag": "true" if index % 3 == 0 else "false",
            "partner_consultation_booked_flag": "true" if index % 7 == 0 else "false",
        })

    return f"sample-curated/{session_id}.json", payload


def seed_new_sample_sessions(count: int = 100) -> Dict[str, int]:
    """Upload deterministic sample-session blobs that follow the blueprint."""
    container = worker.get_blob_service_client().get_container_client(worker.BLOB_CONTAINER)
    uploaded = 0
    for index in range(count):
        blob_name, payload = build_sample_payload(index)
        container.get_blob_client(blob_name).upload_blob(
            json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )
        uploaded += 1
    return {"uploaded": uploaded}


def main() -> int:
    """Curate the current sample dataset and seed the additional sessions."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    curated = curate_existing_sample_blobs()
    enriched = enrich_legacy_sample_sessions()
    seeded = seed_new_sample_sessions(100)
    print(json.dumps({"curated": curated, "enriched": enriched, "seeded": seeded}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
