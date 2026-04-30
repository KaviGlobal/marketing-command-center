# shared_validation.py Reference

This page documents the shared validation module used by both `function_app.py` and `blob_text_to_azure_sql.py`.

## Purpose

`shared_validation.py` centralizes session and field validation rules so both the HTTP intake layer and the ingestion worker enforce the same data quality constraints.

## Constants and allowed values

- `SESSION_ID_RE`
  - Regular expression for acceptable session IDs.
  - Allows letters, digits, `-`, `_`, `.`, and `:`.
  - Length between 6 and 200 characters.

- `FIELD_NAME_RE`
  - Regular expression for valid field names.
  - Allows letters, digits, `_`, `-`, `.`, and `:`.

- `EMAIL_RE`
  - Regex used for validating email-like values.

- `URL_RE`
  - Regex used to detect web URLs.

- `REPEATED_CHAR_RE`
  - Detects repeated characters, used in noise spam heuristics.

- `MAX_FIELD_NAME_LENGTH` = 150
- `MAX_FIELD_VALUE_LENGTH` = 4000

- `JUNK_TOKENS`
  - Common low-quality values rejected for non-KPI fields.
  - Includes `test`, `testing`, `asdf`, `qwerty`, `1234`, `n/a`, `na`, `none`, `null`.

- `ALLOWED_KPI_FIELDS`
  - Canonical KPI field names allowed for SQL ingestion.
  - Includes session timestamp fields, engagement flags, lead/contact fields, fallback/error fields, and partner/vendor/candidate KPI names.

- `KNOWN_EMAIL_FIELDS`
  - Field names that should be validated as emails.
  - Includes `lead_email`, `candidate_email`, `partner_email`, `vendor_email`, and `userEmail`.

- `KNOWN_BOOL_FIELDS`
  - Fields expected to contain boolean-like values.

- `KNOWN_INT_FIELDS`
  - Fields expected to contain integer values.

- `FIELD_NAME_ALIASES`
  - Maps legacy or alternate field names to canonical names.
  - Example: `csat` → `satisfaction_score`, `flowType` → `flow_type`, `feedbackComment` → `feedback_comment`.

## Functions

### `strip_surrounding_quotes(value)`
- Removes any matching surrounding single or double quotes from the string.
- Useful for normalizing values that arrive quoted.

### `normalize_text(value)`
- Converts any value to a trimmed string.
- Removes surrounding quotes and whitespace.
- Returns `""` for `None`.

### `normalize_bool(value)`
- Converts boolean-like inputs into `1`, `0`, or `None`.
- Accepts `true`, `false`, `1`, `0`, `yes`, `no`.
- Returns `None` for empty values or unsupported truth values.

### `canonical_field_name(field_name)`
- Converts aliased input names to canonical field names.
- Applies the mapping defined in `FIELD_NAME_ALIASES`.

### `validate_email(email)`
- Validates an email string against `EMAIL_RE`.

### `validate_session_id(session_id)`
- Ensures the value is present, normalized, and a valid GUID.
- Returns `(True, "")` for valid session IDs.
- Returns `(False, reason)` for missing, malformed, or non-GUID values.

### `validate_field_name(field_name, is_kpi=False)`
- Validates a field name for supported characters and maximum length.
- Normalizes aliases first.
- If `is_kpi=True`, ensures the canonical field name is in `ALLOWED_KPI_FIELDS`.
- Rejects unknown KPI fields, invalid characters, or oversized names.

### `validate_field_value(field_name, field_value, is_kpi)`
- Validates field values based on the field type.
- Enforces max length.
- Validates `email` fields and `_email` suffix fields.
- Validates `KNOWN_BOOL_FIELDS` values with `normalize_bool()`.
- Validates `KNOWN_INT_FIELDS` values as integers.
- Enforces `satisfaction_score` value range of `1` to `5`.
- Rejects likely noise values for non-KPI fields only.

### `is_probable_noise(value)`
- Detects noise or low-quality text.
- Rejects values containing `JUNK_TOKENS`, repeated characters, or short meaningless content.

## How it is used

- `function_app.py` uses these rules to validate incoming HTTP payloads before writing session blobs.
- `blob_text_to_azure_sql.py` uses the same normalization and validation rules when parsing blob payloads and preparing SQL inserts.

## Why this matters

Using a shared validation module ensures:
- consistent data quality across intake and ingestion
- fewer surprises in SQL writes
- consistent rejection reasons for both real-time and batch processing
