# Kavi Marketing Command Center

Central repository for Kavi Global's marketing data infrastructure. Contains two systems:

| System | Description |
|---|---|
| [**ETL Pipelines**](etl/README.md) | Daily data pulls from YouTube, Facebook, GA4, LinkedIn, Mailchimp, and Dripify into Azure SQL |
| [**Chatbot Ingestion**](#chatbot-ingestion) | Azure Function that ingests chatbot session data from blob storage into Azure SQL |

---

## ETL Pipelines

See **[etl/README.md](etl/README.md)** for full setup and usage.

Runs automatically at **5 AM UTC daily** via GitHub Actions. Each pipeline can also be triggered manually from the Actions tab with optional `--since` date overrides.

**Pipelines:**

| Pipeline | Source | Destination |
|---|---|---|
| YouTube | YouTube Data API v3 + Analytics | `dw_youtube.*` |
| Facebook | Meta Graph API | `dw_facebook.*` |
| GA4 | Google Analytics Data API | `dw_ga4.*` |
| LinkedIn | LinkedIn Marketing API | `dw_linkedin.*` |
| Mailchimp | Mailchimp API v3 | `dw_mailchimp.*` |
| Dripify | Google Sheets | `dw_dripify.*` |

---

## Chatbot Ingestion

Azure Function app that receives chatbot session exports and loads them into Azure SQL.

**Key files:**

| File | Purpose |
|---|---|
| `function_app.py` | HTTP intake endpoint |
| `blob_text_to_azure_sql.py` | Batch ingestion worker |
| `shared_validation.py` | Shared validation logic |
| `ingestion_logging.py` | Logging helpers |
| `chatbot-sessions-data-export-schema.sql` | Azure SQL schema |
| `host.json` | Azure Function host config |

**Docs:** See the [`docs/`](docs/README.md) directory for full operational, configuration, endpoint, ingestion, and deployment details.

**Deployment:** See [`DEPLOYMENT.md`](DEPLOYMENT.md).

**Local setup:**
```bash
pip install -r requirements.txt
cp local.settings.example.json local.settings.json
# Fill in local.settings.json, then:
func start
```

---

## Repository structure

```
├── etl/                              # ETL pipelines
│   ├── README.md
│   ├── requirements.txt
│   ├── .env.template
│   ├── youtube_ETL.py
│   ├── facebook_ETL.py
│   ├── ga4_ETL.py
│   ├── linkedin_ETL.py
│   ├── mailchimp/
│   └── dripify/
│
├── .github/workflows/daily_etl.yml  # Scheduled ETL runner
│
├── function_app.py                   # Chatbot ingestion (Azure Function)
├── blob_text_to_azure_sql.py
├── shared_validation.py
├── ingestion_logging.py
├── host.json
├── requirements.txt
├── local.settings.example.json
├── chatbot-sessions-data-export-schema.sql
├── DEPLOYMENT.md
└── docs/
```

---

## GitHub Actions secrets

Secrets are shared across both systems. Add them in **Settings → Secrets and variables → Actions**.

### Azure SQL (shared)
| Secret | Description |
|---|---|
| `AZURE_SQL_SERVER` | e.g. `yourserver.database.windows.net` |
| `AZURE_SQL_DB` | Database name |
| `AZURE_SQL_USER` | SQL login username |
| `AZURE_SQL_PWD` | SQL login password |
| `AZURE_SQL_DRIVER` | e.g. `ODBC Driver 18 for SQL Server` |

See [etl/README.md](etl/README.md#github-actions-secrets) for ETL-specific secrets.
