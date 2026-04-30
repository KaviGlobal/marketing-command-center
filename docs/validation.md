# Validation Rules

Validation is centralized in `shared_validation.py` and used by both the Azure Function and ingestion worker.

## Session ID

- Required.
- Must be a valid GUID.
- Must match the allowed character pattern.

## Field name rules

- Supported characters: letters, digits, `_`, `-`, `.`, `:`.
- Max length: 150 characters.
- If `isKpi=true`, the field name must be one of the allowed KPI field names.

## Field value rules

- Max length: 4000 characters.
- Email fields are validated by regex.
- Known boolean fields must use `true`/`false`, `1`/`0`, or `yes`/`no`.
- Known integer fields must contain only integer text.
- `satisfaction_score` must be `1`-`5`.
- Non-KPI fields can be rejected as probable noise.

## Aliases and normalization

Incoming field names are normalized by alias mapping:
- `csat` → `satisfaction_score`
- `flowType` → `flow_type`
- `sessionStartTime` → `session_start_time`
- `sessionEndTimeUtc` → `session_end_time_utc`
- `feedbackSubmittedFlag` → `feedback_submitted_flag`
- `leadEmail` → `lead_email`

The shared validation module also normalizes boolean-like values and trims surrounding quotes.

For full function-level details, see [Shared Validation Reference](shared_validation.md).
