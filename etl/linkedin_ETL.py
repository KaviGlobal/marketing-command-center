"""
linkedin_ETL.py
───────────────
Fetches posts (+ reactions, comments), follower statistics, and page analytics
from Kavi Global and Kavi Philippines LinkedIn pages, then loads into Azure SQL.

Matches the staging → DW pattern used in youtube_ETL.py and facebook_ETL.py.

Required .env vars
──────────────────
  LINKEDIN_CLIENT_ID
  LINKEDIN_CLIENT_SECRET
  LINKEDIN_REFRESH_TOKEN       (from linkedin_token_collection.py — preferred)
  LINKEDIN_ACCESS_TOKEN        (fallback if no refresh token yet)

  LINKEDIN_ORG_ID_KAVI_GLOBAL              = e.g. 12345678
  LINKEDIN_ORG_ID_KAVI_PHILIPPINES         = e.g. 87654321   (if available)

  AZURE_SQL_SERVER   = mcckavi.database.windows.net
  AZURE_SQL_DB       = mcc
  AZURE_SQL_USER     = mccuser
  AZURE_SQL_PWD      = <your password>

Run
───
  python linkedin_ETL.py                          # all orgs, full history
  python linkedin_ETL.py --org kavi_global        # single org
  python linkedin_ETL.py --since 2025-01-01       # posts on/after date
  python linkedin_ETL.py --skip-analytics         # skip page/follower stats
"""

import os
import sys
import time
import random
import argparse
import requests
import pandas as pd
import urllib.parse
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, types
from sqlalchemy.exc import DBAPIError, OperationalError


# =============================================================================
# CONFIG
# =============================================================================

load_dotenv()

LI_BASE     = "https://api.linkedin.com"
LI_VERSION  = "202401"   # LinkedIn versioned API header

STG_SCHEMA  = "stg_linkedin"
DW_SCHEMA   = "dw_linkedin"

POSTS_PER_PAGE    = 50   # LinkedIn max per call
COMMENTS_PER_PAGE = 100
SECONDS_BETWEEN_CALLS = 0.5
_last_call_ts = 0.0

ORGS = {
    "kavi_global": {
        "label":  "Kavi Global",
        "org_id": os.getenv("LINKEDIN_ORG_ID_KAVI_GLOBAL"),
    },
    "kavi_philippines": {
        "label":  "Kavi Philippines",
        "org_id": os.getenv("LINKEDIN_ORG_ID_KAVI_PHILIPPINES"),
    },
}


# =============================================================================
# TOKEN MANAGEMENT
# =============================================================================

CLIENT_ID     = os.getenv("LINKEDIN_CLIENT_ID")
CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("LINKEDIN_REFRESH_TOKEN")
_access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "")

def refresh_access_token() -> str:
    """Exchange refresh token for a new access token (valid 60 days)."""
    global _access_token
    if not REFRESH_TOKEN:
        if _access_token:
            return _access_token
        sys.exit(
            "❌  No LINKEDIN_REFRESH_TOKEN or LINKEDIN_ACCESS_TOKEN in .env.\n"
            "    Run linkedin_token_collection.py first."
        )
    resp = requests.post(
        "https://www.linkedin.com/oauth/v2/accessToken",
        data={
            "grant_type":    "refresh_token",
            "refresh_token": REFRESH_TOKEN,
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if not resp.ok:
        print(f"⚠️  Token refresh failed ({resp.status_code}): {resp.text}")
        print("    Falling back to existing access token.")
        return _access_token
    _access_token = resp.json()["access_token"]
    print("✅  Access token refreshed.")
    return _access_token

def get_headers() -> dict:
    return {
        "Authorization":             f"Bearer {_access_token}",
        "LinkedIn-Version":          LI_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
    }


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

TRANSIENT_CODES = {"40613", "40197", "40501", "10928", "10929", "10053", "10054", "10060"}

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
    dtype_map = {
        col: types.DateTime()
        for col in df.columns
        if pd.api.types.is_datetime64_any_dtype(df[col])
    }
    for attempt in range(6):
        try:
            df.to_sql(table, engine, schema=schema, if_exists="replace", index=False, dtype=dtype_map)
            print(f"  ✅  Staged {schema}.{table} ({len(df):,} rows)")
            return
        except (DBAPIError, OperationalError) as e:
            if attempt == 5 or not _is_transient(e):
                raise
            sleep = min(120, (2 ** attempt) + random.uniform(0, 1.5))
            time.sleep(sleep)


# =============================================================================
# LINKEDIN API HELPERS
# =============================================================================

def _throttle():
    global _last_call_ts
    wait = SECONDS_BETWEEN_CALLS - (time.time() - _last_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()

def li_get(path: str, params: dict = None, retries: int = 5) -> dict | None:
    """GET from LinkedIn API with throttle + exponential backoff."""
    url = f"{LI_BASE}/{path.lstrip('/')}"
    for attempt in range(retries + 1):
        _throttle()
        resp = requests.get(url, params=params or {}, headers=get_headers(), timeout=30)
        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == retries:
                print(f"  ❌  {resp.status_code} after {retries} retries: {path}")
                return None
            sleep = min(120, (2 ** attempt) + random.uniform(0, 2))
            print(f"  ⚠️  HTTP {resp.status_code}. Backing off {sleep:.1f}s …")
            time.sleep(sleep)
            continue
        if resp.status_code == 403:
            print(f"  ⚠️  403 Forbidden: {path}")
            print("      This endpoint may require additional scope approval.")
            return None
        if not resp.ok:
            print(f"  ⚠️  {resp.status_code}: {resp.text[:200]}")
            return None
        return resp.json()
    return None

def li_paginate(path: str, params: dict, data_key: str = "elements") -> list:
    """Paginate through LinkedIn's start/count paging."""
    items = []
    start = 0
    count = params.get("count", POSTS_PER_PAGE)
    while True:
        page_params = {**params, "start": start, "count": count}
        data = li_get(path, page_params)
        if data is None:
            break
        batch = data.get(data_key, [])
        items.extend(batch)
        total = data.get("paging", {}).get("total", len(items))
        if len(batch) < count or len(items) >= total:
            break
        start += count
    return items


# =============================================================================
# FETCH: POSTS
# =============================================================================

def fetch_posts(org_id: str, since_ts: int | None = None) -> list[dict]:
    """
    Fetch org posts using the UGC Posts API.
    Returns raw post dicts.
    """
    org_urn = f"urn:li:organization:{org_id}"
    params = {
        "q":       "authors",
        "authors": f"List({org_urn})",
        "count":   POSTS_PER_PAGE,
    }
    posts = li_paginate("v2/ugcPosts", params)

    # Filter by date if requested
    if since_ts and posts:
        posts = [p for p in posts
                 if p.get("firstPublishedAt", 0) >= since_ts * 1000]

    print(f"    Fetched {len(posts)} posts")
    return posts


def fetch_social_actions(post_urn: str) -> dict | None:
    """Fetch likes summary and comments for a single post."""
    encoded = urllib.parse.quote(post_urn, safe="")
    return li_get(f"v2/socialActions/{encoded}", {"projection": "(likesSummary,commentsSummary)"})


def fetch_comments(post_urn: str) -> list[dict]:
    """Paginate all comments on a post."""
    encoded = urllib.parse.quote(post_urn, safe="")
    return li_paginate(
        f"v2/socialActions/{encoded}/comments",
        {"count": COMMENTS_PER_PAGE},
        data_key="elements",
    )


# =============================================================================
# FETCH: FOLLOWER STATS
# =============================================================================

def fetch_follower_stats(org_id: str) -> dict | None:
    """Follower count + breakdown by function, seniority, industry, geo."""
    org_urn = urllib.parse.quote(f"urn:li:organization:{org_id}", safe="")
    return li_get(
        "v2/organizationalEntityFollowerStatistics",
        {
            "q":                      "organizationalEntity",
            "organizationalEntity":   f"urn:li:organization:{org_id}",
        },
    )


# =============================================================================
# FETCH: PAGE ANALYTICS (impressions, clicks, engagement rate)
# =============================================================================

def fetch_page_stats(org_id: str) -> dict | None:
    """Monthly page-level stats: impressions, unique visitors, clicks."""
    return li_get(
        "v2/organizationPageStatistics",
        {
            "q":            "organization",
            "organization": f"urn:li:organization:{org_id}",
        },
    )


def fetch_post_analytics(org_id: str, post_urns: list[str]) -> list[dict]:
    """
    Post-level share statistics (impressions, clicks, engagement, likes, comments, shares).
    LinkedIn allows batching up to 100 URNs per call.
    """
    if not post_urns:
        return []
    results = []
    org_urn = f"urn:li:organization:{org_id}"
    for i in range(0, len(post_urns), 100):
        batch = post_urns[i:i + 100]
        urn_list = "List(" + ",".join(urllib.parse.quote(u, safe="") for u in batch) + ")"
        data = li_get(
            "v2/organizationalEntityShareStatistics",
            {
                "q":                    "organizationalEntity",
                "organizationalEntity": org_urn,
                "ugcPosts":             urn_list,
            },
        )
        if data:
            results.extend(data.get("elements", []))
    print(f"    Fetched analytics for {len(results)} posts")
    return results


# =============================================================================
# FLATTEN TO DATAFRAMES
# =============================================================================

def flatten_posts(posts: list[dict], org_id: str, org_label: str,
                  social_map: dict, analytics_map: dict) -> pd.DataFrame:
    rows = []
    for p in posts:
        urn          = p.get("id", "")
        published_ms = p.get("firstPublishedAt", 0)
        published_dt = datetime.fromtimestamp(published_ms / 1000, tz=timezone.utc) if published_ms else None

        # Text content
        content = (
            p.get("specificContent", {})
             .get("com.linkedin.ugc.ShareContent", {})
             .get("shareCommentary", {})
             .get("text", "")
        )
        # Media type
        media_category = (
            p.get("specificContent", {})
             .get("com.linkedin.ugc.ShareContent", {})
             .get("shareMediaCategory", "NONE")
        )

        social  = social_map.get(urn, {})
        analyt  = analytics_map.get(urn, {}).get("totalShareStatistics", {})

        rows.append({
            "post_urn":         urn,
            "org_id":           org_id,
            "org_name":         org_label,
            "content":          content,
            "media_category":   media_category,
            "published_at":     published_dt,
            "like_count":       social.get("likesSummary", {}).get("totalLikes", analyt.get("likeCount", 0)),
            "comment_count":    social.get("commentsSummary", {}).get("totalFirstLevelComments", analyt.get("commentCount", 0)),
            "share_count":      analyt.get("shareCount", 0),
            "impression_count": analyt.get("impressionCount", 0),
            "click_count":      analyt.get("clickCount", 0),
            "engagement_rate":  analyt.get("engagement", None),
            "fetched_at":       datetime.now(timezone.utc),
        })
    return pd.DataFrame(rows)


def flatten_comments(posts: list[dict], comments_map: dict, org_id: str) -> pd.DataFrame:
    rows = []
    for p in posts:
        post_urn = p.get("id", "")
        for c in comments_map.get(post_urn, []):
            actor = c.get("actor", "")
            rows.append({
                "comment_urn":  c.get("$URN", c.get("id", "")),
                "post_urn":     post_urn,
                "org_id":       org_id,
                "message":      c.get("message", {}).get("text", ""),
                "actor_urn":    actor,
                "created_at":   datetime.fromtimestamp(
                                    c.get("created", {}).get("time", 0) / 1000,
                                    tz=timezone.utc
                                ) if c.get("created", {}).get("time") else None,
                "like_count":   c.get("likesSummary", {}).get("totalLikes", 0),
                "fetched_at":   datetime.now(timezone.utc),
            })
    return pd.DataFrame(rows)


def flatten_follower_stats(data: dict | None, org_id: str, org_label: str) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    rows = []
    fetched = datetime.now(timezone.utc)
    for elem in data.get("elements", [data]):  # some endpoints return directly
        total = elem.get("followerCountsByAssociationType", [{}])
        for entry in total:
            rows.append({
                "org_id":            org_id,
                "org_name":          org_label,
                "association_type":  entry.get("associationType", "MEMBER"),
                "follower_count":    entry.get("followerCounts", {}).get("organicFollowerCount", 0)
                                     + entry.get("followerCounts", {}).get("paidFollowerCount", 0),
                "organic_count":     entry.get("followerCounts", {}).get("organicFollowerCount", 0),
                "paid_count":        entry.get("followerCounts", {}).get("paidFollowerCount", 0),
                "fetched_at":        fetched,
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def flatten_page_stats(data: dict | None, org_id: str, org_label: str) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    rows = []
    fetched = datetime.now(timezone.utc)
    for elem in data.get("elements", []):
        time_range = elem.get("timeRange", {})
        page_stats = elem.get("totalPageStatistics", {})
        views      = page_stats.get("views", {})
        rows.append({
            "org_id":            org_id,
            "org_name":          org_label,
            "period_start":      datetime.fromtimestamp(time_range.get("start", 0) / 1000, tz=timezone.utc)
                                  if time_range.get("start") else None,
            "period_end":        datetime.fromtimestamp(time_range.get("end", 0) / 1000, tz=timezone.utc)
                                  if time_range.get("end") else None,
            "all_page_views":    views.get("allPageViews", {}).get("pageViews", 0),
            "unique_visitors":   views.get("allPageViews", {}).get("uniquePageViews", 0),
            "mobile_views":      views.get("mobilePageViews", {}).get("pageViews", 0),
            "desktop_views":     views.get("desktopPageViews", {}).get("pageViews", 0),
            "fetched_at":        fetched,
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# =============================================================================
# DW DDL + UPSERTS
# =============================================================================

def ensure_schema(schema):
    run_sql(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema}') EXEC('CREATE SCHEMA {schema}')")

DW_UPSERT_POSTS = f"""
IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='posts')
CREATE TABLE {DW_SCHEMA}.posts (
    post_urn         NVARCHAR(300) NOT NULL PRIMARY KEY,
    org_id           NVARCHAR(50),
    org_name         NVARCHAR(200),
    content          NVARCHAR(MAX),
    media_category   NVARCHAR(100),
    published_at     DATETIMEOFFSET,
    like_count       INT,
    comment_count    INT,
    share_count      INT,
    impression_count INT,
    click_count      INT,
    engagement_rate  FLOAT,
    fetched_at       DATETIMEOFFSET
);
MERGE {DW_SCHEMA}.posts AS tgt
USING {STG_SCHEMA}.stg_li_posts AS src ON tgt.post_urn = src.post_urn
WHEN MATCHED THEN UPDATE SET
    like_count=src.like_count, comment_count=src.comment_count,
    share_count=src.share_count, impression_count=src.impression_count,
    click_count=src.click_count, engagement_rate=src.engagement_rate,
    fetched_at=src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT
    (post_urn,org_id,org_name,content,media_category,published_at,
     like_count,comment_count,share_count,impression_count,click_count,engagement_rate,fetched_at)
VALUES
    (src.post_urn,src.org_id,src.org_name,src.content,src.media_category,src.published_at,
     src.like_count,src.comment_count,src.share_count,src.impression_count,src.click_count,src.engagement_rate,src.fetched_at);
"""

DW_UPSERT_COMMENTS = f"""
IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='comments')
CREATE TABLE {DW_SCHEMA}.comments (
    comment_urn  NVARCHAR(300) NOT NULL PRIMARY KEY,
    post_urn     NVARCHAR(300),
    org_id       NVARCHAR(50),
    message      NVARCHAR(MAX),
    actor_urn    NVARCHAR(300),
    created_at   DATETIMEOFFSET,
    like_count   INT,
    fetched_at   DATETIMEOFFSET
);
MERGE {DW_SCHEMA}.comments AS tgt
USING {STG_SCHEMA}.stg_li_comments AS src ON tgt.comment_urn = src.comment_urn
WHEN MATCHED THEN UPDATE SET like_count=src.like_count, fetched_at=src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT
    (comment_urn,post_urn,org_id,message,actor_urn,created_at,like_count,fetched_at)
VALUES
    (src.comment_urn,src.post_urn,src.org_id,src.message,src.actor_urn,src.created_at,src.like_count,src.fetched_at);
"""

DW_UPSERT_FOLLOWERS = f"""
IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='follower_stats')
CREATE TABLE {DW_SCHEMA}.follower_stats (
    id               INT IDENTITY PRIMARY KEY,
    org_id           NVARCHAR(50),
    org_name         NVARCHAR(200),
    association_type NVARCHAR(100),
    follower_count   INT,
    organic_count    INT,
    paid_count       INT,
    fetched_at       DATETIMEOFFSET
);
INSERT INTO {DW_SCHEMA}.follower_stats
    (org_id,org_name,association_type,follower_count,organic_count,paid_count,fetched_at)
SELECT org_id,org_name,association_type,follower_count,organic_count,paid_count,fetched_at
FROM {STG_SCHEMA}.stg_li_followers;
"""

DW_UPSERT_PAGE_STATS = f"""
IF NOT EXISTS (SELECT 1 FROM information_schema.tables
    WHERE table_schema='{DW_SCHEMA}' AND table_name='page_stats')
CREATE TABLE {DW_SCHEMA}.page_stats (
    id             INT IDENTITY PRIMARY KEY,
    org_id         NVARCHAR(50),
    org_name       NVARCHAR(200),
    period_start   DATETIMEOFFSET,
    period_end     DATETIMEOFFSET,
    all_page_views INT,
    unique_visitors INT,
    mobile_views   INT,
    desktop_views  INT,
    fetched_at     DATETIMEOFFSET
);
INSERT INTO {DW_SCHEMA}.page_stats
    (org_id,org_name,period_start,period_end,all_page_views,unique_visitors,mobile_views,desktop_views,fetched_at)
SELECT org_id,org_name,period_start,period_end,all_page_views,unique_visitors,mobile_views,desktop_views,fetched_at
FROM {STG_SCHEMA}.stg_li_page_stats;
"""


# =============================================================================
# MAIN
# =============================================================================

def run_org(key: str, cfg: dict, since_ts: int | None, skip_analytics: bool):
    org_id    = cfg["org_id"]
    org_label = cfg["label"]

    if not org_id:
        print(f"  ⚠️  Skipping {org_label} — LINKEDIN_ORG_ID_{key.upper()} not set in .env")
        return

    print(f"\n{'─'*60}")
    print(f"📄  Processing: {org_label}  (Org ID: {org_id})")
    print(f"{'─'*60}")

    # ── Posts ──────────────────────────────────────────────────────────────
    posts = fetch_posts(org_id, since_ts)
    if not posts:
        print("  No posts returned.")
        return

    post_urns = [p["id"] for p in posts if p.get("id")]

    # ── Social actions (likes / comment counts per post) ───────────────────
    print(f"    Fetching social actions for {len(post_urns)} posts …")
    social_map = {}
    for urn in post_urns:
        actions = fetch_social_actions(urn)
        if actions:
            social_map[urn] = actions

    # ── Comments ───────────────────────────────────────────────────────────
    print(f"    Fetching comments …")
    comments_map = {}
    for urn in post_urns:
        comments = fetch_comments(urn)
        if comments:
            comments_map[urn] = comments
    total_comments = sum(len(v) for v in comments_map.values())

    # ── Post-level analytics ───────────────────────────────────────────────
    analytics_map = {}
    if not skip_analytics:
        analytics = fetch_post_analytics(org_id, post_urns)
        for a in analytics:
            urn = a.get("ugcPost") or a.get("share") or ""
            if urn:
                analytics_map[urn] = a

    # ── Flatten ────────────────────────────────────────────────────────────
    df_posts    = flatten_posts(posts, org_id, org_label, social_map, analytics_map)
    df_comments = flatten_comments(posts, comments_map, org_id)

    print(f"    Posts    : {len(df_posts):,}")
    print(f"    Comments : {len(df_comments):,}")

    # ── Follower + page stats ──────────────────────────────────────────────
    df_followers  = pd.DataFrame()
    df_page_stats = pd.DataFrame()
    if not skip_analytics:
        print("    Fetching follower statistics …")
        df_followers  = flatten_follower_stats(fetch_follower_stats(org_id), org_id, org_label)
        print("    Fetching page analytics …")
        df_page_stats = flatten_page_stats(fetch_page_stats(org_id), org_id, org_label)

    # ── Stage ──────────────────────────────────────────────────────────────
    ensure_schema(STG_SCHEMA)
    ensure_schema(DW_SCHEMA)

    load_stage(df_posts,    "stg_li_posts")
    if not df_comments.empty:
        load_stage(df_comments, "stg_li_comments")
    if not df_followers.empty:
        load_stage(df_followers, "stg_li_followers")
    if not df_page_stats.empty:
        load_stage(df_page_stats, "stg_li_page_stats")

    # ── Upsert into DW ─────────────────────────────────────────────────────
    print("  Merging into DW …")
    run_sql(DW_UPSERT_POSTS)
    if not df_comments.empty:
        run_sql(DW_UPSERT_COMMENTS)
    if not df_followers.empty:
        run_sql(DW_UPSERT_FOLLOWERS)
    if not df_page_stats.empty:
        run_sql(DW_UPSERT_PAGE_STATS)

    print(f"  ✅  {org_label} done.\n")


def main():
    parser = argparse.ArgumentParser(description="Kavi Global LinkedIn ETL")
    parser.add_argument("--org", choices=list(ORGS.keys()), default=None,
                        help="Run only this org key. Default: all.")
    parser.add_argument("--since", default=None, metavar="YYYY-MM-DD",
                        help="Only fetch posts on/after this date (UTC).")
    parser.add_argument("--skip-analytics", action="store_true",
                        help="Skip follower stats and page analytics (faster).")
    args = parser.parse_args()

    since_ts = None
    if args.since:
        since_ts = int(datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc).timestamp())

    orgs_to_run = {args.org: ORGS[args.org]} if args.org else ORGS

    print("\n" + "=" * 60)
    print("  Kavi Global — LinkedIn ETL")
    print(f"  Orgs   : {', '.join(c['label'] for c in orgs_to_run.values())}")
    print(f"  Since  : {args.since or 'all history'}")
    print("=" * 60)

    # Refresh token once before starting
    refresh_access_token()

    for key, cfg in orgs_to_run.items():
        run_org(key, cfg, since_ts, args.skip_analytics)

    print("\n🎉  LinkedIn ETL complete.\n")


if __name__ == "__main__":
    main()
