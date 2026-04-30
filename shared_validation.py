"""Shared constants and validation functions used by both the Azure Function
HTTP endpoints (function_app.py) and the batch ingestion script
(blob_text_to_azure_sql.py).
"""

# Validation helpers for session IDs, field names, field values, and noisy input.
# This module centralizes rules so both HTTP ingestion and batch ingestion share the same checks.
import re
import uuid
from typing import Any, Optional, Tuple

SESSION_ID_RE = re.compile(r"^[A-Za-z0-9\-_:.]{6,200}$")
FIELD_NAME_RE = re.compile(r"^[A-Za-z0-9_\-:.]+$")
EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)
URL_RE = re.compile(r"https?://|www\.", re.IGNORECASE)
REPEATED_CHAR_RE = re.compile(r"(.)\1{6,}")

# Maximum allowed lengths for field names and values.
MAX_FIELD_NAME_LENGTH = 150
MAX_FIELD_VALUE_LENGTH = 4000

JUNK_TOKENS = {"test", "testing", "asdf", "qwerty", "1234", "n/a", "na", "none", "null"}

CANONICAL_FLOW_TYPES = ("Prospect", "Career", "Partnership")

OFFERING_BLUEPRINT = {
    "Services": [
        "Data Strategy",
        "Data Platform",
        "Business Intelligence",
        "Data Science & AI",
        "Internet of Things",
        "Intelligent Apps",
        "Managed Services",
        "RCM",
    ],
    "Software": [
        "Advana",
    ],
    "Solutions": [
        "AI Agent",
        "Chatbot",
        "Fraud Detection",
        "Pain Detection",
        "Data Labeling",
    ],
    "Industry": [
        "Transportation",
        "Rail",
        "Manufacturing",
        "Healthcare",
        "Pharma",
        "Education",
    ],
    "Technology": [
        "Microsoft",
        "AWS",
        "Databricks",
        "Snowflake",
    ],
    "Advanced Analytics": [
        "Scheduling Optimization",
        "Preventative Maintenance",
        "Document Text Extraction",
        "Profitability Analytics",
        "Reliability Analytics",
    ],
}

FLOW_TYPE_ALIASES = {
    "prospect": "Prospect",
    "prospectpath": "Prospect",
    "system": "Prospect",
    "conversationevaluation": "Prospect",
    "advancedanalytics": "Prospect",
    "career": "Career",
    "careeropportunities": "Career",
    "careeropportunity": "Career",
    "partnership": "Partnership",
    "partner": "Partnership",
    "partnerships": "Partnership",
    "businesspartnership": "Partnership",
    "businesspartnerships": "Partnership",
}

OFFERING_NAME_ALIASES = {
    "preventive maintenance": "Preventative Maintenance",
}

ALLOWED_KPI_FIELDS = {
    "session_start_time",
    "session_start_time_utc",
    "session_end_time_utc",
    "engaged_flag",
    "resolved_flag",
    "escalated_flag",
    "abandoned_flag",
    "flow_type",
    "last_node_id",
    "last_node_name",
    "last_node_time",
    "last_node_time_utc",
    "goal_completed_flag",
    "exit_reason",
    "satisfaction_score",
    "feedback_submitted_flag",
    "satisfaction_submitted_flag",
    "feedback_comment",
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
    "application_intent_flag",
    "candidate_capture_started_flag",
    "candidate_capture_completed_flag",
    "candidate_name",
    "candidate_email",
    "job_interest_area",
    "job_interest_location",
    "partner_capture_started_flag",
    "partner_capture_completed_flag",
    "partner_name",
    "partner_org_name",
    "partner_email",
    "partner_type",
    "partner_consultation_requested_flag",
    "partner_consultation_booked_flag",
    "fallback_flag",
    "fallback_count",
    "error_flag",
    "error_node_id",
    "error_code",
    "error_count",
    "response_latency_ms",
    "response_latency_ms_avg",
    "vendor_name",
    "vendor_company",
    "vendor_email",
    "vendor_phone",
    "service_category",
    "service_details",
    "partner_status",
    "previous_experience",
}

# Email fields that should be validated with an email regex.
KNOWN_EMAIL_FIELDS = {"lead_email", "candidate_email", "partner_email", "vendor_email", "userEmail"}

# Boolean-like fields that are expected to contain true/false values.
KNOWN_BOOL_FIELDS = {
    "engaged_flag",
    "resolved_flag",
    "escalated_flag",
    "abandoned_flag",
    "goal_completed_flag",
    "feedback_submitted_flag",
    "satisfaction_submitted_flag",
    "lead_capture_started_flag",
    "lead_capture_completed_flag",
    "consultation_requested_flag",
    "scheduler_link_clicked_flag",
    "application_intent_flag",
    "candidate_capture_started_flag",
    "candidate_capture_completed_flag",
    "partner_capture_started_flag",
    "partner_capture_completed_flag",
    "partner_consultation_requested_flag",
    "partner_consultation_booked_flag",
    "fallback_flag",
    "error_flag",
}

KNOWN_INT_FIELDS = {"satisfaction_score", "fallback_count", "error_node_id", "error_count", "response_latency_ms", "response_latency_ms_avg"}

# Known field name aliases to normalize incoming JSON keys into canonical KPI field names.
FIELD_NAME_ALIASES = {
    "csat": "satisfaction_score",
    "flowType": "flow_type",
    "sessionStartTime": "session_start_time",
    "sessionStartTimeUtc": "session_start_time",
    "sessionEndTimeUtc": "session_end_time_utc",
    "feedbackSubmittedFlag": "feedback_submitted_flag",
    "satisfactionSubmittedFlag": "satisfaction_submitted_flag",
    "feedbackComment": "feedback_comment",
    "leadCaptureStartedFlag": "lead_capture_started_flag",
    "leadCaptureCompletedFlag": "lead_capture_completed_flag",
    "consultationRequestedFlag": "consultation_requested_flag",
    "schedulerLinkClickedFlag": "scheduler_link_clicked_flag",
    "offeringPrimary": "offering_primary",
    "offeringSecondary": "offering_secondary",
    "intentPrimary": "intent_primary",
    "applicationIntentFlag": "application_intent_flag",
    "candidateCaptureStartedFlag": "candidate_capture_started_flag",
    "candidateCaptureCompletedFlag": "candidate_capture_completed_flag",
    "jobInterestArea": "job_interest_area",
    "jobInterestLocation": "job_interest_location",
    "partnerCaptureStartedFlag": "partner_capture_started_flag",
    "partnerCaptureCompletedFlag": "partner_capture_completed_flag",
    "partnerOrgName": "partner_org_name",
    "partnerConsultationRequestedFlag": "partner_consultation_requested_flag",
    "partnerConsultationBookedFlag": "partner_consultation_booked_flag",
    "fallbackFlag": "fallback_flag",
    "fallbackCount": "fallback_count",
    "errorFlag": "error_flag",
    "errorNodeId": "error_node_id",
    "errorCode": "error_code",
    "errorCount": "error_count",
    "vendorName": "vendor_name",
    "vendorCompany": "vendor_company",
    "vendorEmail": "vendor_email",
    "vendorPhone": "vendor_phone",
    "serviceCategory": "service_category",
    "serviceDetails": "service_details",
    "partnerStatus": "partner_status",
    "previousExperience": "previous_experience",
}


def normalize_lookup_key(value: Any) -> str:
    """Normalize a value into a compact lowercase lookup key."""
    text = str(value).strip() if value is not None else ""
    if len(text) >= 2 and ((text[0] == '"' and text[-1] == '"') or (text[0] == "'" and text[-1] == "'")):
        text = text[1:-1].strip()
    return re.sub(r"[^a-z0-9]+", "", text.lower())


FLOW_TYPE_LOOKUP = {
    normalize_lookup_key(alias): canonical
    for alias, canonical in FLOW_TYPE_ALIASES.items()
}

OFFERING_LOOKUP = {
    normalize_lookup_key(offering): offering
    for offerings in OFFERING_BLUEPRINT.values()
    for offering in offerings
}
for alias, canonical in OFFERING_NAME_ALIASES.items():
    OFFERING_LOOKUP[normalize_lookup_key(alias)] = canonical

OFFERING_CATEGORY_BY_NAME = {
    offering: category
    for category, offerings in OFFERING_BLUEPRINT.items()
    for offering in offerings
}


def strip_surrounding_quotes(value: Any) -> str:
    """Remove a single surrounding quote pair from a string value."""
    text = str(value).strip()
    while len(text) >= 2 and ((text[0] == '"' and text[-1] == '"') or (text[0] == "'" and text[-1] == "'")):
        next_text = text[1:-1].strip()
        if next_text == text:
            break
        text = next_text
    return text


def normalize_text(value: Any) -> str:
    """Normalize any value into a trimmed string for validation or storage."""
    if value is None:
        return ""
    # Strip whitespace and any surrounding quotes so values are compared consistently.
    return strip_surrounding_quotes(str(value).strip())


def normalize_bool(value: Any) -> Optional[int]:
    """Normalize common boolean-like inputs into 1/0 or None."""
    if value is None:
        return None
    text = normalize_text(value).lower()
    if text in {"true", "1", "yes"}:
        return 1
    if text in {"false", "0", "no"}:
        return 0
    return None


def canonical_field_name(field_name: str) -> str:
    """Convert aliased field names into canonical field names."""
    name = normalize_text(field_name)
    return FIELD_NAME_ALIASES.get(name, name)


def normalize_flow_type(value: Any) -> Optional[str]:
    """Map raw flow labels into the canonical 3-flow taxonomy."""
    text = normalize_text(value)
    if not text:
        return None
    lookup = FLOW_TYPE_LOOKUP.get(normalize_lookup_key(text))
    if lookup:
        return lookup
    lowered = text.lower()
    if "partner" in lowered or "business" in lowered:
        return "Partnership"
    if "career" in lowered or "job" in lowered or "applicant" in lowered:
        return "Career"
    return "Prospect"


def normalize_offering_name(value: Any) -> Optional[str]:
    """Return a canonical offering name when it matches the blueprint catalog."""
    text = normalize_text(value)
    if not text:
        return None
    return OFFERING_LOOKUP.get(normalize_lookup_key(text))


def get_offering_category(value: Any) -> Optional[str]:
    """Return the blueprint category for a canonical offering."""
    offering = normalize_offering_name(value)
    if not offering:
        return None
    return OFFERING_CATEGORY_BY_NAME.get(offering)


def normalize_kpi_field_value(field_name: str, value: Any) -> Optional[str]:
    """Normalize special KPI values into canonical storage-ready text."""
    if value is None:
        return None
    canonical = canonical_field_name(field_name)
    if canonical == "flow_type":
        return normalize_flow_type(value)
    if canonical in {"offering_primary", "offering_secondary"}:
        return normalize_offering_name(value)
    return normalize_text(value)


def validate_email(email: str) -> bool:
    """Return True if the provided value matches the email validation regex."""
    return bool(EMAIL_RE.match(email))


def validate_session_id(session_id: str) -> Tuple[bool, str]:
    """Validate that sessionId is present, correctly formatted, and is a GUID."""
    normalized = normalize_text(session_id)
    if not normalized:
        return False, "sessionId is required"
    if not SESSION_ID_RE.match(normalized):
        return False, "Invalid sessionId format"
    try:
        uuid.UUID(normalized)
    except ValueError:
        return False, "sessionId must be a valid GUID"
    return True, ""


def validate_field_name(field_name: str, is_kpi: bool = False) -> Tuple[bool, str]:
    """Validate field name for supported characters, length, and KPI whitelist."""
    if not field_name:
        return False, "fieldName is required"
    canonical = canonical_field_name(field_name)
    if len(canonical) > MAX_FIELD_NAME_LENGTH:
        return False, "fieldName is too long"
    if not FIELD_NAME_RE.match(canonical):
        return False, "fieldName contains unsupported characters"
    if is_kpi and canonical not in ALLOWED_KPI_FIELDS:
        return False, f"Unsupported KPI fieldName: {canonical}"
    return True, ""


def validate_field_value(field_name: str, field_value: str, is_kpi: bool) -> Tuple[bool, str]:
    """Validate field values against rules for email, boolean, integer, and noise detection."""
    if len(field_value) > MAX_FIELD_VALUE_LENGTH:
        return False, f"fieldValue too long for {field_name}"

    if field_name == "flow_type" and field_value:
        if normalize_flow_type(field_value) is None:
            return False, "flow_type must map to Prospect, Career, or Partnership"

    if field_name in {"offering_primary", "offering_secondary"} and field_value:
        if normalize_offering_name(field_value) is None:
            return False, f"{field_name} must match the approved offering blueprint"

    if field_name in KNOWN_EMAIL_FIELDS or field_name.endswith("_email"):
        if field_value and not validate_email(field_value):
            return False, f"Invalid email for {field_name}"

    # Validate boolean-like fields against a small set of accepted text values.
    if field_name in KNOWN_BOOL_FIELDS:
        if normalize_bool(field_value) is None:
            return False, f"Invalid boolean value for {field_name}"

    if field_name in KNOWN_INT_FIELDS:
        if not re.fullmatch(r"-?\d+", field_value or ""):
            return False, f"Invalid integer value for {field_name}"
        if field_name == "satisfaction_score" and field_value not in {"1", "2", "3", "4", "5"}:
            return False, "satisfaction_score must be 1-5"

    # Reject likely noise for non-KPI fields only, since KPI fields may intentionally contain different data types.
    if not is_kpi and field_value and is_probable_noise(field_value):
        return False, f"Probable noise/troll input for {field_name}"

    return True, ""


def is_probable_noise(value: str) -> bool:
    """Return True for values that appear to be spam, repeated characters, or low-quality text."""
    text = value.strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered in JUNK_TOKENS:
        return True
    # Short or trivial text is not useful content.
    if len(text) < 2:
        return True
    if REPEATED_CHAR_RE.search(text):
        return True
    symbol_count = sum(1 for c in text if not c.isalnum() and not c.isspace())
    if len(text) >= 8 and symbol_count / max(len(text), 1) > 0.6:
        return True
    if URL_RE.search(text) and len(text.split()) <= 2:
        return True
    unique_ratio = len(set(text.lower())) / max(len(text), 1)
    if len(text) >= 12 and unique_ratio < 0.2:
        return True
    return False
