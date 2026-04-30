# Kavi Chat Data Export

This repository includes the code and documentation for a chat session ingestion pipeline.

## Documentation

- [Docs Home](docs/README.md)
- [Deployment Runbook](DEPLOYMENT.md)
- [Local settings template](local.settings.example.json)
- [Sample payload](sample_blob.json)

## Quick links

- `function_app.py` - Azure Function HTTP intake
- `blob_text_to_azure_sql.py` - batch ingestion worker
- `shared_validation.py` - shared validation rules
- `chatbot-sessions-data-export-schema.sql` - Azure SQL schema
- `host.json` - Function host configuration
- [Function App Reference](docs/function_app.md)
- [Ingestion Worker Reference](docs/ingestion_functions.md)

## How to use

1. Read `docs/README.md` for full documentation.
2. Use `DEPLOYMENT.md` for deployment guidance.
3. Use `local.settings.example.json` as the local config template.
4. Run `pip install -r requirements.txt` locally.

## Notes

The top-level README is intentionally short. Full operational, configuration, endpoint, ingestion, SQL, validation, and handoff details are in the `docs/` directory.
