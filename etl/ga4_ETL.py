"""
ga4_ETL.py
──────────
Fetches Google Analytics 4 data for the Kavi Global property and loads it
into Azure SQL — matching the staging → DW pattern used in youtube_ETL.py.

Uses the SAME OAuth credentials and refresh token as YouTube.
No new token collection needed.

Required .env vars
──────────────────
  YOUTUBE_CLIENT_ID      (same as YouTube — already in your .env)
  YOUTUBE_CLIENT_SECRET  (same as YouTube — already in your .env)
  YOUTUBE_REFRESH_TOKEN  (same as YouTube — already in your .env)

  GA4_PROPERTY_ID = 399565425   ← add this one line

  AZURE_SQL_SERVER / DB / USER / PWD / DRIVER  (same as YouTube)

Reports pulled
──────────────
  1. traffic_daily     — sessions, users, new users, pageviews, bounce rate,
                         avg session duration  (by date)
  2. traffic_by_source — sessions, users, conversions by source / medium
  3. traffic_by_page   — top pages: views, avg time on page, entrances
  4. traffic_by_geo    — users, sessions by country + city
  5. traffic_by_device — sessions, users by device category

Run
───
  python ga4_ETL.py                            # last 90 days
  python ga4_ETL.py --since 2025-01-01         # custom start date
  python ga4_ETL.py --since 2024-01-01 --until 2024-12-31   # date range
"""

import os
import sys
import time
import random
import argparse
import requests
import pandas as pd
import urllib.parse
from datetime import date, datetime, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from sqlalchemy import create_engine, text, types
from sqlalchemy.exc import DBAPIError, OperationalError


# =============================================================================
# CONFIG
# =============================================================================

load_dotenv()

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
if not GA4_PROPERTY_ID:
    sys.exit("❌  GA4_PROPERTY_ID not set in .env")
PROPERTY = f"properties/{GA4_PROPERTY_ID}"

STG_SCHEMA = "stg_ga4"
DW_SCHEMA  = "dw_ga4"

# How many rows to request per API call (GA4 max is 250,000)
ROW_LIMIT  = 10000

# GA4 API retry settings
MAX_RETRIES = 6


# =============================================================================
# OAUTH — reuse YouTube credentials
# =============================================================================

CLIENT_ID     = os.getenv("YOUTUBE_CLIENT_ID")
CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN")

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN]):
    sys.exit(
        "❌  Missing env vars. Ensure .env contains:\n"
        "    YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN"
    )

def build_analytics_client():
    """Build GA4 client using the existing YouTube refresh token."""
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
    )
    return build("analyticsdata", "v1beta", credentials=creds)


# =============================================================================
# AZURE SQL
# =============================================================================

SERVER = os.getenv("AZURE_SQL_SERVER", "mcckavi.database.windows.net")
DB     = os.getenv("AZURE_SQL_DB",     "mcc")
USER   = os.getenv("AZURE_SQL_USER",   "mccuser")
PWD    = os.getenv("AZURE_SQL_PWD")
DRIVER = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

if not PWD:
    sys.exit("❌  AZURE_SQL_PWD not set in .env")

conn_str = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DB};"
    f"UID={USER};"
    f"PWD={PWD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=120;"
)

engine = create_engine(
    "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
    fast_executemany=True,
    pool_pre_ping=True,
    connect_args={"timeout": 120},
)

TRANSIENT_CODES = {"40613","40197","40501","10928","10929","10053","10054","10060"}

def _is_transient(exc):
    return any(c in str(exc) for c in TRANSIENT_CODES)

def run_sql(sql: str, max_retries: int = 8):
    for attempt in range(max_retries + 1):
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
            return
        except (DBAPIError, OperationalError) as e:
            if not _is_transient(e) or attempt == max_retries:
                raise
            sleep = min(120, (2 ** attempt) + random.uniform(0, 1.5))
            print(f"  ⚠️  Azure SQL transient error. Sleeping {sleep:.1f}s …")
            time.sleep(sleep)

def load_stage(df: pd.DataFrame, table: str, schema: str = STG_SCHEMA):
    for attempt in range(6):
        try:
            dtype_map = {}

            if "fetched_at" in df.columns:
                dtype_map["fetched_at"] = types.DateTime()

            df.to_sql(
                table,
                engine,
                schema=schema,
                if_exists="replace",
                index=False,
                dtype=dtype_map
            )
            print(f"  ✅  Staged {schema}.{table} ({len(df):,} rows)")
            return
        except (DBAPIError, OperationalError) as e:
            if attempt == 5 or not _is_transient(e):
                raise
            sleep = min(120, (2 ** attempt) + random.uniform(0, 1.5))
            time.sleep(sleep)


# =============================================================================
# GA4 API HELPERS
# =============================================================================

def run_report(client, body: dict, retries: int = MAX_RETRIES) -> list[dict]:
    """
    Execute a GA4 runReport call, paginate through all rows,
    and return a flat list of {dimension: value, ..., metric: value, ...} dicts.
    """
    dimensions = [d["name"] for d in body.get("dimensions", [])]
    metrics    = [m["name"] for m in body.get("metrics", [])]

    rows = []
    offset = 0

    while True:
        paged_body = {**body, "limit": ROW_LIMIT, "offset": offset}

        for attempt in range(retries + 1):
            try:
                resp = client.properties().runReport(
                    property=PROPERTY, body=paged_body
                ).execute()
                break
            except HttpError as e:
                if e.resp.status == 429 or e.resp.status >= 500:
                    if attempt == retries:
                        raise
                    sleep = min(120, (2 ** attempt) + random.uniform(0, 2))
                    print(f"  ⚠️  GA4 HTTP {e.resp.status}. Backing off {sleep:.1f}s …")
                    time.sleep(sleep)
                else:
                    raise

        batch = resp.get("rows", [])
        for row in batch:
            record = {}
            for i, dim in enumerate(dimensions):
                record[dim] = row["dimensionValues"][i]["value"]
            for i, met in enumerate(metrics):
                record[met] = row["metricValues"][i]["value"]
            rows.append(record)

        row_count = resp.get("rowCount", 0)
        offset   += ROW_LIMIT
        if offset >= row_count:
            break

    return rows


# =============================================================================
# FETCH EACH REPORT
# =============================================================================

def fetch_traffic_daily(client, start: str, end: str) -> pd.DataFrame:
    print("  Fetching daily traffic overview …")
    rows = run_report(client, {
        "dimensions": [{"name": "date"}],
        "metrics": [
            {"name": "sessions"},
            {"name": "totalUsers"},
            {"name": "newUsers"},
            {"name": "screenPageViews"},
            {"name": "bounceRate"},
            {"name": "averageSessionDuration"},
            {"name": "engagementRate"},
            {"name": "conversions"},
        ],
        "dateRanges": [{"startDate": start, "endDate": end}],
        "orderBys": [{"dimension": {"dimensionName": "date"}}],
    })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["property_id"] = GA4_PROPERTY_ID
    df["fetched_at"]  = datetime.now(timezone.utc)
    # Cast numerics
    for col in ["sessions","totalUsers","newUsers","screenPageViews","conversions"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in ["bounceRate","averageSessionDuration","engagementRate"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    print(f"    {len(df):,} rows")
    return df


def fetch_traffic_by_source(client, start: str, end: str) -> pd.DataFrame:
    print("  Fetching traffic by source / medium …")
    rows = run_report(client, {
        "dimensions": [
            {"name": "date"},
            {"name": "sessionSource"},
            {"name": "sessionMedium"},
            {"name": "sessionCampaignName"},
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "totalUsers"},
            {"name": "newUsers"},
            {"name": "conversions"},
            {"name": "engagementRate"},
        ],
        "dateRanges": [{"startDate": start, "endDate": end}],
    })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["property_id"] = GA4_PROPERTY_ID
    df["fetched_at"]  = datetime.now(timezone.utc)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    for col in ["sessions","totalUsers","newUsers","conversions"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["engagementRate"] = pd.to_numeric(df.get("engagementRate"), errors="coerce")
    print(f"    {len(df):,} rows")
    return df


def fetch_traffic_by_page(client, start: str, end: str) -> pd.DataFrame:
    print("  Fetching traffic by page …")
    rows = run_report(client, {
        "dimensions": [
            {"name": "date"},
            {"name": "pagePath"},
            {"name": "pageTitle"},
        ],
        "metrics": [
            {"name": "screenPageViews"},
            {"name": "totalUsers"},
            {"name": "averageSessionDuration"},
            {"name": "bounceRate"},
        ],
        "dateRanges": [{"startDate": start, "endDate": end}],
        "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}],
    })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["property_id"] = GA4_PROPERTY_ID
    df["fetched_at"]  = datetime.now(timezone.utc)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    for col in ["screenPageViews","totalUsers"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in ["averageSessionDuration","bounceRate"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    print(f"    {len(df):,} rows")
    return df

def fetch_traffic_by_geo(client, start: str, end: str) -> pd.DataFrame:
    print("  Fetching traffic by geography …")
    rows = run_report(client, {
        "dimensions": [
            {"name": "date"},
            {"name": "country"},
            {"name": "city"},
        ],
        "metrics": [
            {"name": "totalUsers"},
            {"name": "sessions"},
            {"name": "newUsers"},
            {"name": "screenPageViews"},
        ],
        "dateRanges": [{"startDate": start, "endDate": end}],
        "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
    })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["property_id"] = GA4_PROPERTY_ID
    df["fetched_at"]  = datetime.now(timezone.utc)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    for col in ["totalUsers","sessions","newUsers","screenPageViews"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    print(f"    {len(df):,} rows")
    return df


def fetch_traffic_by_device(client, start: str, end: str) -> pd.DataFrame:
    print("  Fetching traffic by device …")
    rows = run_report(client, {
        "dimensions": [
            {"name": "date"},
            {"name": "deviceCategory"},
            {"name": "operatingSystem"},
        ],
        "metrics": [
            {"name": "sessions"},
            {"name": "totalUsers"},
            {"name": "screenPageViews"},
            {"name": "bounceRate"},
        ],
        "dateRanges": [{"startDate": start, "endDate": end}],
    })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["property_id"] = GA4_PROPERTY_ID
    df["fetched_at"]  = datetime.now(timezone.utc)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    for col in ["sessions","totalUsers","screenPageViews"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["bounceRate"] = pd.to_numeric(df.get("bounceRate"), errors="coerce")
    print(f"    {len(df):,} rows")
    return df


# =============================================================================
# DW DDL + UPSERTS
# =============================================================================

def ensure_schemas():
    for schema in [STG_SCHEMA, DW_SCHEMA]:
        run_sql(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema}') EXEC('CREATE SCHEMA {schema}')")

DW_TRAFFIC_DAILY = f"""
IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='traffic_daily')
CREATE TABLE {DW_SCHEMA}.traffic_daily (
    property_id             NVARCHAR(50),
    date                    DATE NOT NULL,
    sessions                INT,
    totalUsers              INT,
    newUsers                INT,
    screenPageViews         INT,
    bounceRate              FLOAT,
    averageSessionDuration  FLOAT,
    engagementRate          FLOAT,
    conversions             INT,
    fetched_at              DATETIMEOFFSET,
    PRIMARY KEY (property_id, date)
);
MERGE {DW_SCHEMA}.traffic_daily AS tgt
USING {STG_SCHEMA}.stg_traffic_daily AS src
    ON tgt.property_id = src.property_id AND tgt.date = src.date
WHEN MATCHED THEN UPDATE SET
    sessions=src.sessions, totalUsers=src.totalUsers, newUsers=src.newUsers,
    screenPageViews=src.screenPageViews, bounceRate=src.bounceRate,
    averageSessionDuration=src.averageSessionDuration, engagementRate=src.engagementRate,
    conversions=src.conversions, fetched_at=src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT
    (property_id,date,sessions,totalUsers,newUsers,screenPageViews,
     bounceRate,averageSessionDuration,engagementRate,conversions,fetched_at)
VALUES
    (src.property_id,src.date,src.sessions,src.totalUsers,src.newUsers,src.screenPageViews,
     src.bounceRate,src.averageSessionDuration,src.engagementRate,src.conversions,src.fetched_at);
"""

DW_TRAFFIC_SOURCE = f"""
IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='traffic_by_source')
CREATE TABLE {DW_SCHEMA}.traffic_by_source (
    property_id          NVARCHAR(50)  NOT NULL,
    date                 DATE          NOT NULL,
    sessionSource        NVARCHAR(500) NOT NULL,
    sessionMedium        NVARCHAR(200) NOT NULL,
    sessionCampaignName  NVARCHAR(500) NOT NULL,
    sessions             INT,
    totalUsers           INT,
    newUsers             INT,
    conversions          INT,
    engagementRate       FLOAT,
    fetched_at           DATETIMEOFFSET,
    PRIMARY KEY (property_id, date, sessionSource, sessionMedium, sessionCampaignName)
);
MERGE {DW_SCHEMA}.traffic_by_source AS tgt
USING {STG_SCHEMA}.stg_traffic_by_source AS src
    ON  tgt.property_id         = src.property_id
    AND tgt.date                = src.date
    AND tgt.sessionSource       = src.sessionSource
    AND tgt.sessionMedium       = src.sessionMedium
    AND tgt.sessionCampaignName = src.sessionCampaignName
WHEN MATCHED THEN UPDATE SET
    sessions=src.sessions, totalUsers=src.totalUsers, newUsers=src.newUsers,
    conversions=src.conversions, engagementRate=src.engagementRate, fetched_at=src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT
    (property_id,date,sessionSource,sessionMedium,sessionCampaignName,
     sessions,totalUsers,newUsers,conversions,engagementRate,fetched_at)
VALUES
    (src.property_id,src.date,src.sessionSource,src.sessionMedium,src.sessionCampaignName,
     src.sessions,src.totalUsers,src.newUsers,src.conversions,src.engagementRate,src.fetched_at);
"""

# DDL and MERGE are split into separate run_sql calls — SQL Server batch-compiles
# the entire string and rejects column references that don't exist yet in the
# pre-migration table, even when the DROP/CREATE would have added them first.
DW_TRAFFIC_PAGE_DDL = f"""
IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='traffic_by_page'
) AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id=OBJECT_ID('{DW_SCHEMA}.traffic_by_page') AND name='date'
)
    DROP TABLE {DW_SCHEMA}.traffic_by_page;

IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='traffic_by_page')
CREATE TABLE {DW_SCHEMA}.traffic_by_page (
    property_id             NVARCHAR(50)   NOT NULL,
    date                    DATE           NOT NULL,
    pagePath                NVARCHAR(1000) NOT NULL,
    pageTitle               NVARCHAR(1000),
    screenPageViews         INT,
    totalUsers              INT,
    averageSessionDuration  FLOAT,
    bounceRate              FLOAT,
    fetched_at              DATETIMEOFFSET,
    PRIMARY KEY (property_id, date, pagePath)
);
"""

DW_TRAFFIC_PAGE_MERGE = f"""
MERGE {DW_SCHEMA}.traffic_by_page AS tgt
USING (
    SELECT
        property_id,
        date,
        LEFT(pagePath, 1000)          AS pagePath,
        MAX(pageTitle)                AS pageTitle,
        SUM(screenPageViews)          AS screenPageViews,
        SUM(totalUsers)               AS totalUsers,
        AVG(averageSessionDuration)   AS averageSessionDuration,
        AVG(bounceRate)               AS bounceRate,
        MAX(fetched_at)               AS fetched_at
    FROM {STG_SCHEMA}.stg_traffic_by_page
    GROUP BY property_id, date, LEFT(pagePath, 1000)
) AS src
    ON  tgt.property_id = src.property_id
    AND tgt.date        = src.date
    AND tgt.pagePath    = src.pagePath
WHEN MATCHED THEN UPDATE SET
    pageTitle=src.pageTitle, screenPageViews=src.screenPageViews,
    totalUsers=src.totalUsers, averageSessionDuration=src.averageSessionDuration,
    bounceRate=src.bounceRate, fetched_at=src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT
    (property_id,date,pagePath,pageTitle,screenPageViews,totalUsers,
     averageSessionDuration,bounceRate,fetched_at)
VALUES
    (src.property_id,src.date,src.pagePath,src.pageTitle,src.screenPageViews,
     src.totalUsers,src.averageSessionDuration,src.bounceRate,src.fetched_at);
"""

DW_TRAFFIC_GEO_DDL = f"""
IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='traffic_by_geo'
) AND NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id=OBJECT_ID('{DW_SCHEMA}.traffic_by_geo') AND name='date'
)
    DROP TABLE {DW_SCHEMA}.traffic_by_geo;

IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='traffic_by_geo')
CREATE TABLE {DW_SCHEMA}.traffic_by_geo (
    property_id      NVARCHAR(50)  NOT NULL,
    date             DATE          NOT NULL,
    country          NVARCHAR(200) NOT NULL,
    city             NVARCHAR(200) NOT NULL,
    totalUsers       INT,
    sessions         INT,
    newUsers         INT,
    screenPageViews  INT,
    fetched_at       DATETIMEOFFSET,
    PRIMARY KEY (property_id, date, country, city)
);
"""

DW_TRAFFIC_GEO_MERGE = f"""
MERGE {DW_SCHEMA}.traffic_by_geo AS tgt
USING (
    SELECT
        property_id,
        date,
        country,
        city,
        SUM(totalUsers)      AS totalUsers,
        SUM(sessions)        AS sessions,
        SUM(newUsers)        AS newUsers,
        SUM(screenPageViews) AS screenPageViews,
        MAX(fetched_at)      AS fetched_at
    FROM {STG_SCHEMA}.stg_traffic_by_geo
    GROUP BY property_id, date, country, city
) AS src
    ON  tgt.property_id = src.property_id
    AND tgt.date        = src.date
    AND tgt.country     = src.country
    AND tgt.city        = src.city
WHEN MATCHED THEN UPDATE SET
    totalUsers=src.totalUsers, sessions=src.sessions,
    newUsers=src.newUsers, screenPageViews=src.screenPageViews, fetched_at=src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT
    (property_id,date,country,city,totalUsers,sessions,newUsers,screenPageViews,fetched_at)
VALUES
    (src.property_id,src.date,src.country,src.city,
     src.totalUsers,src.sessions,src.newUsers,src.screenPageViews,src.fetched_at);
"""

DW_TRAFFIC_DEVICE = f"""
IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='traffic_by_device')
CREATE TABLE {DW_SCHEMA}.traffic_by_device (
    property_id     NVARCHAR(50)  NOT NULL,
    date            DATE          NOT NULL,
    deviceCategory  NVARCHAR(100) NOT NULL,
    operatingSystem NVARCHAR(100) NOT NULL,
    sessions        INT,
    totalUsers      INT,
    screenPageViews INT,
    bounceRate      FLOAT,
    fetched_at      DATETIMEOFFSET,
    PRIMARY KEY (property_id, date, deviceCategory, operatingSystem)
);
MERGE {DW_SCHEMA}.traffic_by_device AS tgt
USING {STG_SCHEMA}.stg_traffic_by_device AS src
    ON  tgt.property_id     = src.property_id
    AND tgt.date            = src.date
    AND tgt.deviceCategory  = src.deviceCategory
    AND tgt.operatingSystem = src.operatingSystem
WHEN MATCHED THEN UPDATE SET
    sessions=src.sessions, totalUsers=src.totalUsers,
    screenPageViews=src.screenPageViews, bounceRate=src.bounceRate, fetched_at=src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT
    (property_id,date,deviceCategory,operatingSystem,
     sessions,totalUsers,screenPageViews,bounceRate,fetched_at)
VALUES
    (src.property_id,src.date,src.deviceCategory,src.operatingSystem,
     src.sessions,src.totalUsers,src.screenPageViews,src.bounceRate,src.fetched_at);
"""


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Kavi Global GA4 ETL")
    parser.add_argument("--since", default=None, metavar="YYYY-MM-DD",
                        help="Start date for data pull. Default: 90 days ago.")
    parser.add_argument("--until", default=None, metavar="YYYY-MM-DD",
                        help="End date. Default: today.")
    args = parser.parse_args()

    end_date   = args.until or date.today().isoformat()
    if args.since:
        start_date = args.since
    else:
        from datetime import timedelta
        start_date = (date.today() - timedelta(days=90)).isoformat()

    print("\n" + "=" * 60)
    print("  Kavi Global — GA4 ETL")
    print(f"  Property : {GA4_PROPERTY_ID}")
    print(f"  Range    : {start_date}  →  {end_date}")
    print("=" * 60 + "\n")

    client = build_analytics_client()
    ensure_schemas()

    reports = [
        ("stg_traffic_daily",     fetch_traffic_daily,     [DW_TRAFFIC_DAILY]),
        ("stg_traffic_by_source", fetch_traffic_by_source, [DW_TRAFFIC_SOURCE]),
        ("stg_traffic_by_page",   fetch_traffic_by_page,   [DW_TRAFFIC_PAGE_DDL, DW_TRAFFIC_PAGE_MERGE]),
        ("stg_traffic_by_geo",    fetch_traffic_by_geo,    [DW_TRAFFIC_GEO_DDL,  DW_TRAFFIC_GEO_MERGE]),
        ("stg_traffic_by_device", fetch_traffic_by_device, [DW_TRAFFIC_DEVICE]),
    ]

    for stg_table, fetch_fn, dw_sqls in reports:
        df = fetch_fn(client, start_date, end_date)
        if df.empty:
            print(f"  ⚠️  No data returned for {stg_table}, skipping.")
            continue
        load_stage(df, stg_table)
        print(f"  Merging {stg_table} into DW …")
        for sql in dw_sqls:
            run_sql(sql)
        print()

    print("🎉  GA4 ETL complete.\n")


if __name__ == "__main__":
    main()
