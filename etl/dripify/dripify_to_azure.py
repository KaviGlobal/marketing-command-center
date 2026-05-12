import os
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine

SERVER   = os.environ.get("AZURE_SQL_SERVER", "")
DATABASE = os.environ.get("AZURE_SQL_DB", "")
USERNAME = os.environ.get("AZURE_SQL_USER", "")
PASSWORD = os.environ.get("AZURE_SQL_PWD", "")
SCHEMA   = "dw"
TABLE    = "dripify_leads"
DRIVER   = os.environ.get("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

EXCEL_FILE = "dripify_data_filled.xlsx"

def get_engine():
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=60;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True
    )

def read_and_clean_excel(filepath):
    df = pd.read_excel(filepath, engine="openpyxl")

    rename_map = {
        "firstName":                  "first_name",
        "lastName":                   "last_name",
        "location":                   "location",
        "city":                       "city",
        "country":                    "country",
        "premium":                    "premium",
        "link":                       "link",
        "website":                    "website",
        "email":                      "email",
        "manualEmail":                "manual_email",
        "corporateEmail":             "corporate_email",
        "linkedInEmail":              "linkedin_email",
        "phone":                      "phone",
        "company":                    "company",
        "companyWebsite":             "company_website",
        "position":                   "position",
        "industry":                   "industry",
        "education":                  "education",
        "hookDate":                   "hook_date",
        "numberOfCompanyEmployees":   "number_of_company_employees",
        "numberOfCompanyFollowers":   "number_of_company_followers",
        "campaign_name":              "campaign_name",
    }
    df = df.rename(columns=rename_map)

    expected_cols = [
        "first_name", "last_name", "location", "city", "country", "premium",
        "link", "website", "email", "manual_email", "corporate_email",
        "linkedin_email", "phone", "company", "company_website", "position",
        "industry", "education", "hook_date", "number_of_company_employees",
        "number_of_company_followers", "campaign_name",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
    df = df[expected_cols].copy()

    text_cols = [c for c in expected_cols if c not in (
        "hook_date", "number_of_company_employees", "number_of_company_followers", "premium"
    )]
    for col in text_cols:
        df[col] = df[col].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != "" else None
        )

    df["premium"] = df["premium"].apply(
        lambda x: {"true": 1, "yes": 1, "1": 1,
                   "false": 0, "no": 0, "0": 0}.get(str(x).strip().lower(), None)
        if pd.notna(x) else None
    )
    df["hook_date"] = pd.to_datetime(df["hook_date"], errors="coerce")
    df["number_of_company_employees"] = pd.to_numeric(
        df["number_of_company_employees"], errors="coerce"
    ).astype("Int64")
    df["number_of_company_followers"] = pd.to_numeric(
        df["number_of_company_followers"], errors="coerce"
    ).astype("Int64")

    return df

def build_by_campaign(df):
    return (
        df.groupby("campaign_name", dropna=False)
        .agg(
            total_leads=("link", "count"),
            leads_with_email=("email", lambda s: s.notna().sum()),
            leads_with_phone=("phone", lambda s: s.notna().sum()),
            leads_with_company=("company", lambda s: s.notna().sum()),
            unique_countries=("country", "nunique"),
            unique_industries=("industry", "nunique"),
            earliest_hook_date=("hook_date", "min"),
            latest_hook_date=("hook_date", "max"),
        )
        .reset_index()
    )

def build_by_person(df):
    person_cols = [
        "link", "first_name", "last_name", "email", "manual_email",
        "corporate_email", "linkedin_email", "phone",
        "company", "company_website", "position", "industry",
        "country", "city", "location", "premium",
        "number_of_company_employees", "number_of_company_followers",
    ]
    person_df = df[person_cols].drop_duplicates(subset=["link"]).copy()
    campaigns = (
        df.dropna(subset=["link"])
        .groupby("link")["campaign_name"]
        .apply(lambda s: s.dropna().unique())
        .reset_index()
    )
    campaigns["campaigns"] = campaigns["campaign_name"].apply(lambda x: ", ".join(x))
    campaigns["campaign_count"] = campaigns["campaign_name"].apply(len)
    campaigns = campaigns.drop(columns=["campaign_name"])
    return person_df.merge(campaigns, on="link", how="left").reset_index(drop=True)

def upload(engine, name, frame):
    print(f"  Uploading {SCHEMA}.{name}  ({len(frame):,} rows)...")
    frame.to_sql(
        name,
        con=engine,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
        chunksize=1000,
    )

def main():
    print("Reading Excel...")
    df = read_and_clean_excel(EXCEL_FILE)
    print(f"Shape: {df.shape}")

    engine = get_engine()

    upload(engine, TABLE, df)
    upload(engine, "dripify_leads_by_campaign", build_by_campaign(df))
    upload(engine, "dripify_leads_by_person", build_by_person(df))

    print("Done.")

if __name__ == "__main__":
    main()
