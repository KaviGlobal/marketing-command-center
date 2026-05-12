# Kavi Marketing Command Center

This repository contains two systems: the ETL pipelines that feed the marketing data warehouse, and the chatbot session ingestion pipeline.

---

## ETL Pipelines

Daily data pulls from YouTube, Facebook, GA4, LinkedIn, Mailchimp, and Dripify into Azure SQL.

See **[etl/README.md](etl/README.md)** for full setup, local run instructions, and GitHub Actions secrets.

Runs automatically at **5 AM UTC daily** via GitHub Actions. Each pipeline can also be triggered manually from the Actions tab with optional `--since` date overrides.

| Pipeline | Source | Destination |
|---|---|---|
| YouTube | YouTube Data API v3 + Analytics | `dw_youtube.*` |
| Facebook | Meta Graph API | `dw_facebook.*` |
| GA4 | Google Analytics Data API | `dw_ga4.*` |
| LinkedIn | LinkedIn Marketing API | `dw_linkedin.*` |
| Mailchimp | Mailchimp API v3 | `dw_mailchimp.*` |
| Dripify | Google Sheets | `dw_dripify.*` |

---

## Kavi Chat Data Export

This repository includes the code and documentation for a chat session ingestion pipeline.

### Documentation

- [Docs Home](docs/README.md)
- [Deployment Runbook](DEPLOYMENT.md)
- [Local settings template](local.settings.example.json)
- [Sample payload](sample_blob.json)

### Quick links

- `function_app.py` - Azure Function HTTP intake
- `blob_text_to_azure_sql.py` - batch ingestion worker
- `shared_validation.py` - shared validation rules
- `chatbot-sessions-data-export-schema.sql` - Azure SQL schema
- `host.json` - Function host configuration
- [Function App Reference](docs/function_app.md)
- [Ingestion Worker Reference](docs/ingestion_functions.md)

### How to use

1. Read `docs/README.md` for full documentation.
2. Use `DEPLOYMENT.md` for deployment guidance.
3. Use `local.settings.example.json` as the local config template.
4. Run `pip install -r requirements.txt` locally.

### Notes

The chatbot section of this README is intentionally short. Full operational, configuration, endpoint, ingestion, SQL, validation, and handoff details are in the `docs/` directory.
