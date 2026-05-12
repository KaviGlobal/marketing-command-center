# dripify_data

Pulls LinkedIn lead data from Dripify (stored in Google Sheets), cleans it, and loads it into Azure SQL.

## Data flow

```
Google Sheets (Dripify campaigns)
        ↓  auto_refresh.py
dripify_data_filled.xlsx
        ↓  dripify_to_azure.py
Azure SQL  (dw.dripify_leads, dw.dripify_leads_by_campaign, dw.dripify_leads_by_person)
```

## Files

| File | Purpose |
|------|---------|
| `dripify_retrieve_data.ipynb` | One-time notebook: reads all Sheets tabs, cleans data, exports to Excel |
| `auto_refresh.py` | Incremental refresh: fetches Sheets, appends only new rows to Excel (dedup key: `link + campaign_name + hookDate`) |
| `dripify_to_azure.py` | Reads Excel and uploads to Azure SQL (replaces all three tables each run) |
| `dripify_data_filled.xlsx` | Local cache / output of the pipeline |
| `credentials.json` | Google OAuth client secret (do not commit) |
| `token.pickle` | Cached Google auth token (auto-refreshed) |

## Setup

**Dependencies**
```bash
pip install gspread google-auth google-auth-oauthlib pandas openpyxl sqlalchemy pyodbc
```

**Google auth** — on first run, a browser window will open for OAuth login. The token is saved to `token.pickle` and auto-refreshed on subsequent runs.

**Azure SQL** — use the company database connection.

## Usage

**Incremental refresh from Google Sheets → Excel:**
```bash
python auto_refresh.py
```

**Upload Excel → Azure SQL:**
```bash
python dripify_to_azure.py
```

## Azure tables

| Table | Description |
|-------|-------------|
| `dw.dripify_leads` | One row per lead per campaign (raw) |
| `dw.dripify_leads_by_campaign` | Aggregated stats per campaign |
| `dw.dripify_leads_by_person` | One row per unique LinkedIn profile, with all campaigns listed |

## Data cleaning

- Country names normalized (e.g. `USA` → `United States`)
- Missing `lastName` filled by matching on email, then first name
- `premium` coerced to `0/1`; `hookDate` parsed to datetime; employee/follower counts to integers
