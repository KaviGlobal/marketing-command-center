# Overview

## Purpose

This project implements a two-stage data pipeline for chat session data.

1. Azure Function HTTP intake captures session updates and stores canonical JSON in Azure Blob Storage.
2. A batch ingestion worker reads the stored blobs, validates the data, and writes approved rows to Azure SQL.

## Architecture

```
Client / HTTP clients
   ↓
Azure Function App (`function_app.py`)
   ↓
Azure Blob Storage (`SESSION_LOG_CONTAINER`)
   ↓
Ingestion worker (`blob_text_to_azure_sql.py`)
   ↓
Azure SQL Database (`chatbot-sessions-data-export-schema.sql`)
```

## Files and responsibilities

- `function_app.py`: Azure Functions HTTP API for session updates.
- `blob_text_to_azure_sql.py`: Batch ingestion worker for blob-to-SQL processing.
- `shared_validation.py`: Common validation rules shared by both runtimes.
- `chatbot-sessions-data-export-schema.sql`: Azure SQL schema, ingestion tables, and reporting model.
- `host.json`: Azure Functions host behavior and timeout settings.
- `local.settings.example.json`: Local development configuration template.
- `DEPLOYMENT.md`: Deployment runbook for Azure resources.
- `README.md`: Top-level index and quick start.
