"""
auto_refresh.py
Fetches the latest data from Google Sheets, runs the same preprocessing
as dripify_retrieve_data.ipynb, and appends only NEW rows to
dripify_data_filled.xlsx (existing rows are never modified).

Deduplication key: link + campaign_name + hookDate
"""

import json
import os
import pickle
import pandas as pd
import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ── Config ────────────────────────────────────────────────────────────────────
SPREADSHEET_ID = "16Nj6ixAKcpJH76TV5yrO6Tbcf7_2LgSsARjaDnw8BM4"
EXCEL_FILE     = "dripify_data_filled.xlsx"
DEDUP_KEYS     = ["link", "campaign_name", "hookDate"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

STANDARD_COLUMNS = [
    "firstName", "lastName", "location", "city", "country", "premium",
    "link", "website", "email", "manualEmail", "corporateEmail",
    "linkedInEmail", "phone", "company", "companyWebsite", "position",
    "industry", "education", "hookDate", "numberOfCompanyEmployees",
    "numberOfCompanyFollowers",
]

HEADER_MAP = {
    "First Name":                  "firstName",
    "Last Name":                   "lastName",
    "firstname":                   "firstName",
    "lastname":                    "lastName",
    "LinkedIn Email":              "linkedInEmail",
    "Corporate Email":             "corporateEmail",
    "Manual Email":                "manualEmail",
    "Company Website":             "companyWebsite",
    "Hook Date":                   "hookDate",
    "Number Of Company Employees": "numberOfCompanyEmployees",
    "Number Of Company Followers": "numberOfCompanyFollowers",
}

COUNTRY_MAP = {
    "usa":                      "United States",
    "us":                       "United States",
    "u.s.":                     "United States",
    "u.s.a.":                   "United States",
    "united states of america": "United States",
}

# ── Google Sheets auth ────────────────────────────────────────────────────────
def get_gspread_client():
    # CI: use a Google Service Account JSON stored in an environment variable
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        from google.oauth2.service_account import Credentials
        info = json.loads(sa_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)

    # Local dev: reuse token.pickle (OAuth user credentials)
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.pickle", "wb") as f:
            pickle.dump(creds, f)
    return gspread.authorize(creds)

# ── Read all worksheets (mirrors notebook read_all_sheets) ────────────────────
def _clean_header(h, idx):
    return f"col_{idx}" if h is None or str(h).strip() == "" else str(h).strip()

def read_all_sheets():
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    all_data = []

    for worksheet in sh.worksheets():
        sheet_name = worksheet.title
        print(f"  Reading: {sheet_name}")
        data = worksheet.get_all_values()
        if not data or len(data) < 2:
            continue

        headers = [_clean_header(h, i) for i, h in enumerate(data[0])]
        df = pd.DataFrame(data[1:], columns=headers).replace("", pd.NA)
        df.rename(columns=HEADER_MAP, inplace=True)

        if "Name" in df.columns:
            if "firstName" not in df.columns:
                df["firstName"] = df["Name"].astype("string").str.split().str[0]
            if "lastName" not in df.columns:
                df["lastName"] = df["Name"].astype("string").str.split().str[1:].str.join(" ")

        for col in STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[STANDARD_COLUMNS].copy()
        df["campaign_name"] = sheet_name
        all_data.append(df)

    if not all_data:
        return pd.DataFrame(columns=STANDARD_COLUMNS + ["campaign_name"])
    return pd.concat(all_data, ignore_index=True)

# ── Preprocessing (mirrors notebook cleaning cells) ───────────────────────────
def _normalize_country(val):
    if pd.isna(val):
        return val
    return COUNTRY_MAP.get(str(val).strip().lower(), str(val).strip())

def _build_last_name_lookups(source):
    """Build email->lastName and firstName->lastName dicts from source DataFrame.
    Uses explicit str() to avoid ArrowStringArray type-mismatch issues.
    Only stores mappings where the lastName is unambiguous (exactly one unique value).
    """
    email_to_last = {}
    for email, group in source[source["lastName"].notna() & source["email"].notna()].groupby("email")["lastName"]:
        unique_vals = [str(v) for v in group.unique() if not pd.isna(v)]
        if len(unique_vals) == 1:
            email_to_last[str(email).strip()] = unique_vals[0]

    first_to_last = {}
    for first, group in source[source["lastName"].notna()].groupby("firstName")["lastName"]:
        unique_vals = [str(v) for v in group.unique() if not pd.isna(v)]
        if len(unique_vals) == 1:
            first_to_last[str(first).strip()] = unique_vals[0]

    return email_to_last, first_to_last

def _fill_last_name(df, reference=None):
    """Fill missing lastName by email then firstName.

    reference: extra DataFrame (e.g. existing Excel rows) used to build the
               lookup so that new rows can inherit lastNames already resolved
               in prior runs even if Google Sheets still has them blank.
    """
    lookup_source = pd.concat([df, reference], ignore_index=True) if reference is not None else df
    email_to_last, first_to_last = _build_last_name_lookups(lookup_source)

    def by_email(row):
        if pd.notna(row["lastName"]):
            return row["lastName"]
        if pd.isna(row["email"]):
            return pd.NA
        return email_to_last.get(str(row["email"]).strip(), pd.NA)

    df["lastName"] = df.apply(by_email, axis=1)

    missing = df["lastName"].isna()
    df.loc[missing, "lastName"] = df.loc[missing, "firstName"].apply(
        lambda v: first_to_last.get(str(v).strip(), pd.NA) if pd.notna(v) else pd.NA
    )
    return df

def preprocess(df, existing=None):
    df = df.copy()
    df["country"] = df["country"].apply(_normalize_country)
    df = _fill_last_name(df, reference=existing)
    return df

# ── Load existing Excel ───────────────────────────────────────────────────────
def load_existing():
    if not os.path.exists(EXCEL_FILE):
        print(f"  {EXCEL_FILE} not found — will create a new file.")
        return pd.DataFrame(columns=STANDARD_COLUMNS + ["campaign_name"])
    return pd.read_excel(EXCEL_FILE, engine="openpyxl")

# ── Deduplication ─────────────────────────────────────────────────────────────
def find_new_rows(existing, fresh):
    """Return rows in fresh whose DEDUP_KEYS combination is absent in existing."""
    def key(df):
        return df[DEDUP_KEYS].fillna("__NA__").astype(str).agg("|".join, axis=1)

    existing_keys = set(key(existing)) if not existing.empty else set()
    return fresh[~key(fresh).isin(existing_keys)].copy()

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("Fetching data from Google Sheets...")
    fresh = read_all_sheets()
    print(f"Fetched {len(fresh)} total rows from Sheets.")

    print("Loading existing Excel...")
    existing = load_existing()
    print(f"Existing rows in {EXCEL_FILE}: {len(existing)}")

    print("Preprocessing...")
    fresh = preprocess(fresh, existing=existing)

    new_rows = find_new_rows(existing, fresh)
    print(f"New rows to append: {len(new_rows)}")

    if new_rows.empty:
        print("No new data — nothing to do.")
        return

    combined = pd.concat([existing, new_rows], ignore_index=True)
    combined.to_excel(EXCEL_FILE, index=False)
    print(f"Done. {EXCEL_FILE} now has {len(combined)} rows ({len(new_rows)} added).")

if __name__ == "__main__":
    main()
