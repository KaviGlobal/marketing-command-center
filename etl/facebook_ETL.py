"""
facebook_ETL.py
───────────────
Fetches all posts (with reactions, comments, shares) from Kavi Philippines
and Kavi Global Facebook pages, then loads them into Azure SQL — matching
the same staging → DW pattern used in youtube_ETL.py.

Required .env vars
──────────────────
  # Facebook
  FB_APP_ID                  = your app ID
  FB_APP_SECRET              = your app secret
  FB_PAGE_ID_KAVI_PHILIPPINES  = 805351729333857
  FB_PAGE_TOKEN_KAVI_PHILIPPINES = <long-lived page token>
  FB_PAGE_ID_KAVI_GLOBAL       = 119449644752808
  FB_PAGE_TOKEN_KAVI_GLOBAL    = <long-lived page token>

  # Azure SQL (same as YouTube)
  AZURE_SQL_SERVER   = mcckavi.database.windows.net
  AZURE_SQL_DB       = mcc
  AZURE_SQL_USER     = mccuser
  AZURE_SQL_PWD      = <your password>
  AZURE_SQL_DRIVER   = ODBC Driver 18 for SQL Server   # optional, this is default

Run
───
  python facebook_ETL.py                   # full backfill for all pages
  python facebook_ETL.py --page kavi_ph    # single page (kavi_ph or kavi_global)
  python facebook_ETL.py --since 2025-01-01  # only posts after this date (UTC)
"""

import os
import sys
import hmac
import hashlib
import time
import random
import argparse
import requests
import pandas as pd
import urllib.parse
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, types
from sqlalchemy.exc import DBAPIError, OperationalError


# =============================================================================
# CONFIG
# =============================================================================

load_dotenv()

GRAPH_VERSION = "v19.0"
GRAPH_BASE    = f"https://graph.facebook.com/{GRAPH_VERSION}"

STG_SCHEMA = "stg_facebook"
DW_SCHEMA  = "dw_facebook"

# How many posts to request per page (Facebook max is 100)
POSTS_PER_PAGE = 100
# How many comments to pull per post (Facebook max is 100 per call; we paginate)
COMMENTS_PER_PAGE = 100
# Throttle: seconds between Graph API calls (conservative — free tier is 200 calls/hr)
SECONDS_BETWEEN_CALLS = 0.5
_last_call_ts = 0.0

APP_SECRET = os.getenv("FB_APP_SECRET", "")

def appsecret_proof(token: str) -> str:
    """HMAC-SHA256 of the access token — required when 'Require App Secret' is on."""
    return hmac.new(APP_SECRET.encode(), token.encode(), hashlib.sha256).hexdigest()

PAGES = {
    "kavi_ph": {
        "label":    "Kavi Philippines",
        "page_id":  os.getenv("FB_PAGE_ID_KAVI_PHILIPPINES",  "805351729333857"),
        "token_env": "FB_PAGE_TOKEN_KAVI_PHILIPPINES",
    },
    "kavi_global": {
        "label":    "Kavi Global",
        "page_id":  os.getenv("FB_PAGE_ID_KAVI_GLOBAL", "119449644752808"),
        "token_env": "FB_PAGE_TOKEN_KAVI_GLOBAL",   # matches token_exchange output
    },
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
    "LoginTimeout=120;"
)

engine = create_engine(
    "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_str),
    fast_executemany=True,
    pool_pre_ping=True,
    connect_args={"timeout": 120},
)

TRANSIENT_CODES = {"40613", "40197", "40501", "10928", "10929", "10053", "10054", "10060"}

def _is_transient(exc: Exception) -> bool:
    return any(code in str(exc) for code in TRANSIENT_CODES)

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
            print(f"  ⚠️  Azure SQL transient error (attempt {attempt+1}). Sleeping {sleep:.1f}s …")
            time.sleep(sleep)

def load_stage(df: pd.DataFrame, table: str, schema: str = STG_SCHEMA, if_exists: str = "replace"):
    dtype_map = {}
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            dtype_map[col] = types.DateTime()
        elif df[col].dtype == object:
            dtype_map[col] = types.UnicodeText()
    for attempt in range(6):
        try:
            df.to_sql(table, engine, schema=schema, if_exists=if_exists, index=False, dtype=dtype_map)
            print(f"  ✅  Staged {schema}.{table} ({len(df):,} rows)")
            return
        except (DBAPIError, OperationalError) as e:
            if attempt == 5 or not _is_transient(e):
                raise
            sleep = min(120, (2 ** attempt) + random.uniform(0, 1.5))
            print(f"  ⚠️  Azure SQL transient error during load. Sleeping {sleep:.1f}s …")
            time.sleep(sleep)


# =============================================================================
# GRAPH API HELPERS
# =============================================================================

def _throttle():
    global _last_call_ts
    wait = SECONDS_BETWEEN_CALLS - (time.time() - _last_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()

def graph_get(path: str, params: dict, retries: int = 5) -> dict:
    """GET from Graph API with exponential backoff on 429 / 5xx."""
    url = f"{GRAPH_BASE}/{path.lstrip('/')}"
    # Inject appsecret_proof whenever an access_token is present
    # appsecret_proof intentionally omitted — only needed if app has
    # "Require App Secret" enabled, and causes 400 if the secret doesn't
    # match the app that issued the token (e.g. System User tokens)
    for attempt in range(retries + 1):
        _throttle()
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt == retries:
                resp.raise_for_status()
            sleep = min(120, (2 ** attempt) + random.uniform(0, 2))
            print(f"  ⚠️  HTTP {resp.status_code}. Backing off {sleep:.1f}s …")
            time.sleep(sleep)
            continue
        if not resp.ok:
            print(f"  ❌  HTTP {resp.status_code} — Facebook error detail: {resp.text}")
        resp.raise_for_status()
        return resp.json()
    return {}

def paginate(path: str, params: dict) -> list:
    """Follow Facebook's cursor-based pagination, collecting all items."""
    items = []
    while True:
        data = graph_get(path, params)
        items.extend(data.get("data", []))
        nxt = data.get("paging", {}).get("next")
        if not nxt:
            break
        # Extract cursor from 'next' URL and pass it forward
        import urllib.parse as up
        qs = up.parse_qs(up.urlparse(nxt).query)
        params = {**params, "after": qs.get("after", [None])[0]}
        if not params["after"]:
            break
    return items


# =============================================================================
# FETCH FUNCTIONS
# =============================================================================

def fetch_posts(page_id: str, token: str, since: str | None = None) -> list[dict]:
    """
    Fetch posts from a page, stopping once posts older than `since` are reached.
    Uses Python-side date filtering rather than the API `since` param, which
    conflicts with cursor-based pagination and causes 400 errors.
    """
    fields = (
        "id,message,created_time,permalink_url,"
        "reactions.limit(0).summary(true),"
        f"comments.limit({COMMENTS_PER_PAGE}){{id,message,created_time,like_count}},"
        "shares"
    )
    params: dict = {
        "fields":       fields,
        "limit":        POSTS_PER_PAGE,
        "access_token": token,
    }

    all_posts = []
    while True:
        data = graph_get(f"{page_id}/posts", params)
        batch = data.get("data", [])
        if not batch:
            break

        if since:
            # Posts arrive newest-first; collect until we pass the cutoff date
            for post in batch:
                if post.get("created_time", "")[:10] >= since:
                    all_posts.append(post)
            if any(post.get("created_time", "")[:10] < since for post in batch):
                break
        else:
            all_posts.extend(batch)

        nxt = data.get("paging", {}).get("next")
        if not nxt:
            break
        import urllib.parse as up
        qs = up.parse_qs(up.urlparse(nxt).query)
        after = qs.get("after", [None])[0]
        if not after:
            break
        params = {**params, "after": after}

    print(f"    Fetched {len(all_posts)} posts")
    return all_posts


def fetch_all_comments(post_id: str, token: str) -> list[dict]:
    """Paginate through ALL comments on a post (not just the first batch)."""
    params = {
        "fields":       "id,message,created_time,like_count,comment_count",
        "limit":        COMMENTS_PER_PAGE,
        "access_token": token,
    }
    return paginate(f"{post_id}/comments", params)


# =============================================================================
# FLATTEN TO DATAFRAMES
# =============================================================================

def flatten_posts(posts: list[dict], page_id: str, page_label: str) -> pd.DataFrame:
    rows = []
    for p in posts:
        rows.append({
            "post_id":        p["id"],
            "page_id":        page_id,
            "page_name":      page_label,
            "message":        p.get("message") or p.get("story", ""),
            "created_time":   p.get("created_time"),
            "permalink_url":  p.get("permalink_url"),
            "reaction_count": p.get("reactions", {}).get("summary", {}).get("total_count", 0),
            "share_count":    p.get("shares", {}).get("count", 0),
            "comment_count":  len(p.get("comments", {}).get("data", [])),
            "fetched_at":     datetime.now(timezone.utc).isoformat(),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["created_time"] = pd.to_datetime(df["created_time"], utc=True)
        df["fetched_at"]   = pd.to_datetime(df["fetched_at"],   utc=True)
    return df


def flatten_comments(posts: list[dict], token: str, page_id: str) -> pd.DataFrame:
    rows = []
    for p in posts:
        post_id = p["id"]
        # Use the comments already in the post payload for speed; paginate if
        # the post has more comments than the initial batch returned
        initial = p.get("comments", {}).get("data", [])
        has_more = p.get("comments", {}).get("paging", {}).get("next")

        if has_more:
            comments = fetch_all_comments(post_id, token)
        else:
            comments = initial

        for c in comments:
            rows.append({
                "comment_id":   c["id"],
                "post_id":      post_id,
                "page_id":      page_id,
                "message":      c.get("message", ""),
                "author_name":  None,
                "author_id":    None,
                "created_time": c.get("created_time"),
                "like_count":   c.get("like_count", 0),
                "reply_count":  c.get("comment_count", 0),
                "fetched_at":   datetime.now(timezone.utc).isoformat(),
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["created_time"] = pd.to_datetime(df["created_time"], utc=True)
        df["fetched_at"]   = pd.to_datetime(df["fetched_at"],   utc=True)
    return df


# =============================================================================
# STAGING → DW (UPSERT)
# =============================================================================

STAGE_POSTS_DDL = f"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{STG_SCHEMA}')
    EXEC('CREATE SCHEMA {STG_SCHEMA}');
"""

STAGE_COMMENTS_DDL = STAGE_POSTS_DDL  # same schema check

DW_UPSERT_POSTS = f"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{DW_SCHEMA}')
    EXEC('CREATE SCHEMA {DW_SCHEMA}');

IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = '{DW_SCHEMA}' AND table_name = 'posts'
)
CREATE TABLE {DW_SCHEMA}.posts (
    post_id        NVARCHAR(100)  NOT NULL PRIMARY KEY,
    page_id        NVARCHAR(50),
    page_name      NVARCHAR(200),
    message        NVARCHAR(MAX),
    created_time   DATETIMEOFFSET,
    permalink_url  NVARCHAR(500),
    reaction_count INT,
    share_count    INT,
    comment_count  INT,
    fetched_at     DATETIMEOFFSET
);

MERGE {DW_SCHEMA}.posts AS tgt
USING {STG_SCHEMA}.stg_fb_posts AS src
    ON tgt.post_id = src.post_id
WHEN MATCHED THEN UPDATE SET
    message        = src.message,
    permalink_url  = src.permalink_url,
    reaction_count = src.reaction_count,
    share_count    = src.share_count,
    comment_count  = src.comment_count,
    fetched_at     = src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT (
    post_id, page_id, page_name, message, created_time,
    permalink_url, reaction_count, share_count, comment_count, fetched_at
) VALUES (
    src.post_id, src.page_id, src.page_name, src.message, src.created_time,
    src.permalink_url, src.reaction_count, src.share_count, src.comment_count, src.fetched_at
);
"""

DW_UPSERT_COMMENTS = f"""
IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = '{DW_SCHEMA}' AND table_name = 'comments'
)
CREATE TABLE {DW_SCHEMA}.comments (
    comment_id   NVARCHAR(100)  NOT NULL PRIMARY KEY,
    post_id      NVARCHAR(100),
    page_id      NVARCHAR(50),
    message      NVARCHAR(MAX),
    author_name  NVARCHAR(300),
    author_id    NVARCHAR(100),
    created_time DATETIMEOFFSET,
    like_count   INT,
    reply_count  INT,
    fetched_at   DATETIMEOFFSET
);

MERGE {DW_SCHEMA}.comments AS tgt
USING {STG_SCHEMA}.stg_fb_comments AS src
    ON tgt.comment_id = src.comment_id
WHEN MATCHED THEN UPDATE SET
    like_count  = src.like_count,
    reply_count = src.reply_count,
    fetched_at  = src.fetched_at
WHEN NOT MATCHED BY TARGET THEN INSERT (
    comment_id, post_id, page_id, message, author_name, author_id,
    created_time, like_count, reply_count, fetched_at
) VALUES (
    src.comment_id, src.post_id, src.page_id, src.message, src.author_name, src.author_id,
    src.created_time, src.like_count, src.reply_count, src.fetched_at
);
"""


# =============================================================================
# MAIN
# =============================================================================

def run_page(key: str, cfg: dict, since: str | None):
    token = os.getenv(cfg["token_env"])
    if not token:
        print(f"  ⚠️  Skipping {cfg['label']} — {cfg['token_env']} not set in .env")
        return

    page_id    = cfg["page_id"]
    page_label = cfg["label"]

    print(f"\n{'─'*60}")
    print(f"📄  Processing: {page_label}  (ID: {page_id})")
    print(f"{'─'*60}")

    # ── Fetch ──────────────────────────────────────────────────────────────
    posts = fetch_posts(page_id, token, since=since)
    if not posts:
        print("  No posts returned.")
        return

    df_posts    = flatten_posts(posts, page_id, page_label)
    df_comments = flatten_comments(posts, token, page_id)

    print(f"    Posts    : {len(df_posts):,}")
    print(f"    Comments : {len(df_comments):,}")

    # ── Stage ──────────────────────────────────────────────────────────────
    run_sql(STAGE_POSTS_DDL)
    load_stage(df_posts, "stg_fb_posts")
    if not df_comments.empty:
        load_stage(df_comments, "stg_fb_comments")

    # ── Upsert into DW ─────────────────────────────────────────────────────
    print("  Merging into DW …")
    run_sql(DW_UPSERT_POSTS)
    if not df_comments.empty:
        run_sql(DW_UPSERT_COMMENTS)
    print(f"  ✅  {page_label} done.\n")


def main():
    parser = argparse.ArgumentParser(description="Kavi Global Facebook ETL")
    parser.add_argument(
        "--page",
        choices=list(PAGES.keys()),
        default=None,
        help="Run only this page key (kavi_ph or kavi_global). Default: all pages.",
    )
    parser.add_argument(
        "--since",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only fetch posts created on or after this date (UTC). Default: all history.",
    )
    args = parser.parse_args()

    pages_to_run = {args.page: PAGES[args.page]} if args.page else PAGES

    print("\n" + "=" * 60)
    print("  Kavi Global — Facebook ETL")
    print(f"  Pages  : {', '.join(cfg['label'] for cfg in pages_to_run.values())}")
    print(f"  Since  : {args.since or 'all history'}")
    print("=" * 60)

    for key, cfg in pages_to_run.items():
        run_page(key, cfg, since=args.since)

    print("\n🎉  Facebook ETL complete.\n")


if __name__ == "__main__":
    main()
