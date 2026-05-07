# SQL Schema

## Purpose

The SQL schema stores ingested session data and supports reporting.

## Key tables

- `dbo.session_blob_session`
  - One row per session.
  - Stores session metadata and insertion timestamps.
- `dbo.session_blob_fact`
  - Approved field-level records extracted from session blobs.
- `dbo.session_blob_rejection`
  - Rejected field records with reasons and raw text.
- `dbo.session_blob_ingestion`
  - Blob-level ingestion status and timestamps.
- `dbo.session_blob_ingestion_run`
  - Batch ingestion run history and retry counts.
- `dbo.kpi_aggregate_refresh_run`
  - KPI refresh bookkeeping.

## Reporting tables

The schema also includes normalized reporting tables such as:
- `dbo.sessions` – session-level KPI and outcome data.
- `dbo.drop_off_nodes` – final node and goal completion data for a session.
- `dbo.satisfaction_feedback` – satisfaction score and feedback details.
- `dbo.prospect_inquiries` – lead and inquiry metadata for prospect flows.
- `dbo.career_inquiries` – candidate inquiry and job interest data.
- `dbo.partner_inquiries` – partner lead and booking activity.
- `dbo.vendor_inquiries` – vendor contact and service interest data.
- `dbo.bot_optimization_metrics` – fallback, latency, and error diagnostics.

## Ingestion bookkeeping

The schema also includes run and blob ingestion tracking tables:
- `dbo.session_blob_ingestion` – one row per processed blob, with status and counts.
- `dbo.session_blob_ingestion_run` – batch run summaries, retry counts, and overall status.

## Notes

- `chatbot-sessions-data-export-schema.sql` is the only Azure SQL deployment script.
- Historical prospect/offering migration deltas are already folded into that canonical schema.
