# ETL Pipelines

Pulls data from YouTube, Facebook, Google Analytics 4, LinkedIn, Mailchimp, and Dripify into Azure SQL. Runs automatically every day at 5 AM UTC via GitHub Actions.

---

## Pipelines

| Pipeline | Source | Azure SQL schema |
|---|---|---|
| YouTube | YouTube Data API v3 + Analytics | `dw_youtube` |
| Facebook | Meta Graph API | `dw_facebook` |
| GA4 | Google Analytics Data API | `dw_ga4` |
| LinkedIn | LinkedIn Marketing API | `dw_linkedin` |
| Mailchimp | Mailchimp API v3 (OAuth) | `dw_mailchimp` |
| Dripify | Google Sheets → Excel | `dw_dripify` |

---

## Local setup

### 1. Install dependencies

```bash
pip install -r etl/requirements.txt
```

You also need **ODBC Driver 18 for SQL Server**:
- Mac: `brew install msodbcsql18` (also run `export ODBCSYSINI=/opt/homebrew/etc`)
- Windows/Linux: [Microsoft docs](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

### 2. Configure environment variables

```bash
cp etl/.env.template etl/.env
```

Fill in `etl/.env` with your credentials. See [GitHub Actions secrets](#github-actions-secrets) for where each value comes from.

---

## Running locally

Run from the repo root, or `cd etl` first and drop the `etl/` prefix:

```bash
# YouTube — full backfill from a date
python etl/youtube_ETL.py --since 2024-01-01

# Facebook — full history
python etl/facebook_ETL.py --since 2023-01-01

# GA4
python etl/ga4_ETL.py --since 2024-01-01

# LinkedIn
python etl/linkedin_ETL.py --since 2024-01-01

# Mailchimp (after one-time OAuth setup)
cd etl/mailchimp && python auto_refresh.py

# Dripify
cd etl/dripify && python auto_refresh.py && python dripify_to_azure.py
```

---

## Triggering via GitHub Actions

Go to **Actions → Daily ETL → Run workflow**. Optional inputs:

| Input | Default | Description |
|---|---|---|
| `youtube_since` | 35 days ago | Analytics backfill window (dims refresh all videos regardless) |
| `facebook_since` | 2 days ago | Set to e.g. `2020-01-01` for a full history reload |
| `linkedin_since` | 2 days ago | Leave blank on manual run to pull all history |

---

## GitHub Actions secrets

Add these in **Settings → Secrets and variables → Actions**.

### Azure SQL
| Secret | Description |
|---|---|
| `AZURE_SQL_SERVER` | e.g. `yourserver.database.windows.net` |
| `AZURE_SQL_DB` | Database name |
| `AZURE_SQL_USER` | SQL login username |
| `AZURE_SQL_PWD` | SQL login password |
| `AZURE_SQL_DRIVER` | e.g. `ODBC Driver 18 for SQL Server` |

### YouTube
| Secret | How to get it |
|---|---|
| `YOUTUBE_CLIENT_ID` | Google Cloud Console → OAuth 2.0 client |
| `YOUTUBE_CLIENT_SECRET` | Same |
| `YOUTUBE_REFRESH_TOKEN` | Run `python youtube_token_collection.py` locally |

### Facebook
| Secret | How to get it |
|---|---|
| `FB_APP_ID` | Meta for Developers → Your App |
| `FB_APP_SECRET` | Same |
| `FB_PAGE_TOKEN_KAVI_PHILIPPINES` | Run `python facebook_token_exchange.py` |
| `FB_PAGE_TOKEN_KAVI_GLOBAL` | Same flow, Global page |

### Google Analytics 4
| Secret | How to get it |
|---|---|
| `GA4_PROPERTY_ID` | GA4 Admin → Property → Property ID |

GA4 reuses the YouTube OAuth credentials (`YOUTUBE_CLIENT_ID`, `YOUTUBE_CLIENT_SECRET`, `YOUTUBE_REFRESH_TOKEN`).

### LinkedIn
| Secret | How to get it |
|---|---|
| `LINKEDIN_CLIENT_ID` | LinkedIn Developers → App → Auth tab |
| `LINKEDIN_CLIENT_SECRET` | Same |
| `LINKEDIN_ACCESS_TOKEN` | Run `python linkedin_token_collection.py` locally |
| `LINKEDIN_REFRESH_TOKEN` | Same script |
| `LINKEDIN_ORG_ID_KAVI_GLOBAL` | LinkedIn Company Page URL id |
| `LINKEDIN_ORG_ID_KAVI_PHILIPPINES` | Same, Philippines page |

### Mailchimp

One-time local OAuth flow:

```bash
cd etl/mailchimp
# Set MAILCHIMP_CLIENT_ID and MAILCHIMP_CLIENT_SECRET in .env, then:
python mailchimp_extract_data.py
# Open http://127.0.0.1:8000, complete login — token prints to stdout
```

| Secret | Value |
|---|---|
| `MAILCHIMP_ACCESS_TOKEN` | `access_token` from the OAuth output |
| `MAILCHIMP_API_ROOT` | `api_root` from the OAuth output (e.g. `https://us16.api.mailchimp.com/3.0`) |

Mailchimp tokens don't expire — this is a one-time step.

### Dripify (Google Sheets)

```bash
# Encode your local token and store as a secret:
base64 -i etl/dripify/token.pickle | tr -d '\n'
```

| Secret | Value |
|---|---|
| `GOOGLE_TOKEN_PICKLE_B64` | Output of the command above |

---

## File structure

```
etl/
├── README.md
├── requirements.txt
├── .env.template
│
├── youtube_ETL.py              # Full backfill + daily refresh
├── youtube_token_collection.py # One-time OAuth token setup
├── youtube_token_connection.py
├── youtube_daily_refresh.py
│
├── facebook_ETL.py             # Posts + comments for both pages
├── facebook_oauth.py
├── facebook_token_exchange.py  # Short-lived → long-lived token exchange
│
├── ga4_ETL.py                  # Traffic daily, by page, device, source, geo
│
├── linkedin_ETL.py
├── linkedin_token_collection.py
├── linkedin.py
│
├── mailchimp/
│   ├── auto_refresh.py         # Main entry point
│   ├── mailchimp_extract_data.py
│   ├── mailchimp_to_azure.py
│   └── README.md
│
└── dripify/
    ├── auto_refresh.py         # Sheets → Excel (incremental)
    ├── dripify_to_azure.py     # Excel → Azure SQL
    └── README.md
```
