# Mailchimp Data Pipeline

Pulls data from Mailchimp via OAuth and loads it into Azure SQL Database.

## What it does

1. Authenticates with Mailchimp using OAuth2
2. Exports 7 tables to a timestamped folder under `export_mailchimp/`
3. Uploads all tables to Azure SQL under the `dw` schema

**Tables exported:**
- `dim_user` — Mailchimp account info
- `dim_date` — Date dimension (2018 to today)
- `dim_mailchimp_audience` — Audience/list metadata
- `dim_mailchimp_campaign` — Campaign metadata
- `dim_mailchimp_member` — All members across all lists
- `fact_mailchimp_audience_monthly_members_based` — Monthly audience metrics
- `fact_mailchimp_campaign_monthly` — Monthly campaign performance metrics

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

You also need **ODBC Driver 18 for SQL Server** installed on your machine.
- Mac: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos
- Windows: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### 2. Configure environment variables

Copy or edit `.env`:

```
MAILCHIMP_CLIENT_ID=your_client_id
MAILCHIMP_CLIENT_SECRET=your_client_secret
MAILCHIMP_REDIRECT_URI=http://127.0.0.1:8000/callback
FLASK_SECRET_KEY=your_secret_key
MAILCHIMP_EXPORT_DIR=export_mailchimp
MAILCHIMP_SINCE_SEND_TIME=2018-01-01T00:00:00Z
```

### 3. Configure Azure SQL credentials

Edit `mailchimp_to_azure.py` and fill in:

```python
SERVER   = "your-server.database.windows.net"
DATABASE = "your_database"
USERNAME = "your_username"
PASSWORD = "your_password"
SCHEMA   = "dw"
```

Make sure your IP is whitelisted in the Azure SQL firewall rules before running.

---

## First-time OAuth setup

You only need to do this once. It saves a `token.json` file that is reused on every future run.

**Step 1** — Start the Flask server:
```bash
python mailchimp_extract_data.py
```

**Step 2** — Open your browser and go to:
```
http://127.0.0.1:8000
```

**Step 3** — Click **"Connect Mailchimp"** and log in with your Mailchimp account.

**Step 4** — Once you see **"Connected ✅"**, the token is saved. You can stop the Flask server (`Ctrl+C`).

---

## Running a refresh

After the first-time setup, run this single command to pull the latest data from Mailchimp and upload it to Azure SQL:

```bash
python auto_refresh.py
```

This will:
1. Load the saved OAuth token from `token.json`
2. Fetch all lists, campaigns, and members from Mailchimp
3. Save CSVs to `export_mailchimp/<timestamp>/`
4. Drop and recreate all 7 tables in Azure SQL, then upload the new data

---

## Files

| File | Purpose |
|------|---------|
| `auto_refresh.py` | **Main entry point** — run this for every refresh |
| `mailchimp_extract_data.py` | Flask OAuth app + Mailchimp export logic |
| `mailchimp_to_azure.py` | Uploads CSVs to Azure SQL |
| `token.json` | Saved OAuth token (auto-generated, do not commit) |
| `export_mailchimp/` | Exported CSV snapshots, one folder per run |
| `.env` | Environment variables |
