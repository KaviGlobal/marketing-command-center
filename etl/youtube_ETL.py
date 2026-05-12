import os
import re
import json
import time
import random
import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import date, timedelta
from dotenv import load_dotenv

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials

from sqlalchemy import create_engine, text, types
from sqlalchemy.exc import DBAPIError, OperationalError
import urllib.parse


# =============================================================================
# CONFIG
# =============================================================================

STG_SCHEMA = "stg_youtube"
DW_YT_SCHEMA = "dw_youtube"
DW_SHARED_SCHEMA = "dw_shared"

# monthly chunks (recommended)
CHUNK_BY_MONTH = True

# content length bins
SHORT_MAX = 59
MEDIUM_MAX = 600

# checkpoint file (lets you resume without re-querying)
CHECKPOINT_PATH = Path("yt_checkpoint_monthly.json")

# Bulk analytics: use dimensions=day,video → 2 API calls/month instead of 2×N_videos/month
# Set False only if you hit "not supported" 400 errors on your channel
USE_BULK_ANALYTICS = False

# YouTube Analytics throttling (start conservative; script will auto-slow further if needed)
MIN_SECONDS_BETWEEN_ANALYTICS_CALLS = 2.0  # ~30 requests/min
MAX_ANALYTICS_RETRIES = 10
_last_analytics_call_ts = 0.0

# Set to False after first 400 "not supported" for impression metrics — avoids
# burning API quota retrying a metric that will never be available on this channel
_impressions_supported = True


# =============================================================================
# ENV + DB CONNECTION
# =============================================================================

load_dotenv()

CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN")

SERVER = os.getenv("AZURE_SQL_SERVER", "mcckavi.database.windows.net")
DB     = os.getenv("AZURE_SQL_DB", "mcc")
USER   = os.getenv("AZURE_SQL_USER", "mccuser")
PWD    = os.getenv("AZURE_SQL_PWD")
DRIVER = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, PWD]):
    raise RuntimeError(
        "Missing required env vars. Ensure .env includes: "
        "YOUTUBE_CLIENT_ID, YOUTUBE_CLIENT_SECRET, YOUTUBE_REFRESH_TOKEN, AZURE_SQL_PWD"
    )

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


# =============================================================================
# AZURE SQL RETRY HELPERS (fixes 40613 etc.)
# =============================================================================

TRANSIENT_AZURE_SQL_CODES = {
    "40613", "40197", "40501", "10928", "10929", "10053", "10054", "10060"
}

def is_transient_azure_sql_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(code in msg for code in TRANSIENT_AZURE_SQL_CODES)

def run_sql(sql: str, max_retries: int = 10):
    for attempt in range(max_retries + 1):
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
            return
        except (DBAPIError, OperationalError) as e:
            if (not is_transient_azure_sql_error(e)) or attempt == max_retries:
                raise
            backoff = min(120, (2 ** attempt) + random.uniform(0, 1.5))
            print(f"⚠️ Azure SQL transient error (attempt {attempt+1}/{max_retries+1}). Sleeping {backoff:.1f}s...")
            time.sleep(backoff)

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
            print(f"✅ staged {schema}.{table} ({len(df):,} rows)")
            return
        except (DBAPIError, OperationalError) as e:
            if attempt == 5 or (not is_transient_azure_sql_error(e)):
                raise
            backoff = min(120, (2 ** attempt) + random.uniform(0, 1.5))
            print(f"⚠️ Azure SQL transient error during to_sql. Sleeping {backoff:.1f}s...")
            time.sleep(backoff)


# =============================================================================
# CHECKPOINTING
# =============================================================================

def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        return json.loads(CHECKPOINT_PATH.read_text())
    return {}

def save_checkpoint(cp: dict):
    CHECKPOINT_PATH.write_text(json.dumps(cp, indent=2))


# =============================================================================
# OAUTH HELPERS
# =============================================================================

def refresh_access_token(client_id, client_secret, refresh_token) -> str:
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def build_google_clients():
    """Build YouTube API clients with auto-refreshing OAuth2 credentials.
    The google-auth library will transparently refresh the access token when
    it expires, so long-running backfills no longer crash after ~1 hour."""
    creds = Credentials(
        token=None,
        refresh_token=REFRESH_TOKEN,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
    )
    youtube = build("youtube", "v3", credentials=creds)
    yt_analytics = build("youtubeAnalytics", "v2", credentials=creds)
    return youtube, yt_analytics


# =============================================================================
# YT ANALYTICS EXECUTE WRAPPER (handles 429 correctly)
# =============================================================================

def _throttle_analytics():
    global _last_analytics_call_ts
    now = time.time()
    wait = MIN_SECONDS_BETWEEN_ANALYTICS_CALLS - (now - _last_analytics_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_analytics_call_ts = time.time()

def execute_yt(request, label="yt_analytics"):
    """
    Execute YouTube Analytics request with:
      - global throttle
      - exponential backoff on 429 and 5xx
      - auto-increasing base throttle if we see repeated 429s
    """
    global MIN_SECONDS_BETWEEN_ANALYTICS_CALLS

    for attempt in range(MAX_ANALYTICS_RETRIES + 1):
        _throttle_analytics()
        try:
            return request.execute()

        except HttpError as e:
            status = getattr(e.resp, "status", None)
            retryable = status in (429, 500, 502, 503, 504)

            if not retryable:
                raise
            if attempt == MAX_ANALYTICS_RETRIES:
                raise

            backoff = min(180, (2 ** attempt) + random.uniform(0, 2.0))

            if status == 429:
                # slow down future requests so we stop re-triggering the per-minute wall
                MIN_SECONDS_BETWEEN_ANALYTICS_CALLS = min(10.0, MIN_SECONDS_BETWEEN_ANALYTICS_CALLS + 0.5)

            print(f"⚠️ {label}: HTTP {status} (attempt {attempt+1}/{MAX_ANALYTICS_RETRIES+1}) "
                  f"→ sleep {backoff:.1f}s; throttle={MIN_SECONDS_BETWEEN_ANALYTICS_CALLS:.1f}s")
            time.sleep(backoff)


# =============================================================================
# SMALL UTILITIES
# =============================================================================

def chunked(lst, n=50):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

ISO_DUR_RE = re.compile(
    r"^P(?:(?P<days>\d+)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?"
    r")?$"
)

def iso8601_duration_to_seconds(s: str) -> int | None:
    if not s:
        return None
    m = ISO_DUR_RE.match(s)
    if not m:
        return None
    days = int(m.group("days") or 0)
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


# =============================================================================
# EXTRACT: CHANNEL + VIDEO METADATA (YouTube Data API)
# =============================================================================

def get_my_channel(youtube) -> pd.DataFrame:
    resp = youtube.channels().list(
        part="id,snippet,statistics,brandingSettings",
        mine=True
    ).execute()

    if not resp.get("items"):
        raise RuntimeError("No channels returned. Token might be authorized for a different YouTube account.")

    ch = resp["items"][0]
    snippet = ch.get("snippet", {})
    stats = ch.get("statistics", {})
    branding = ch.get("brandingSettings", {})

    row = {
        "channel_id": ch["id"],
        "channel_name": snippet.get("title"),
        "category": None,
        "created_at": (snippet.get("publishedAt", "")[:10] or None),
        "country": snippet.get("country"),
        "subscriber_count": int(stats.get("subscriberCount", 0)) if stats.get("subscriberCount") is not None else None,
        "total_videos": int(stats.get("videoCount", 0)) if stats.get("videoCount") is not None else None,
        "is_verified": None,
        "custom_url": snippet.get("customUrl"),
        "description": snippet.get("description"),
        "banner_url": (branding.get("image", {}) or {}).get("bannerExternalUrl"),
    }
    return pd.DataFrame([row])

def get_uploads_playlist_id(youtube, channel_id: str) -> str:
    resp = youtube.channels().list(part="contentDetails", id=channel_id).execute()
    return resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

def list_all_video_ids_from_playlist(youtube, uploads_playlist_id: str, max_pages=200) -> list[str]:
    video_ids = []
    page_token = None
    for _ in range(max_pages):
        resp = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=page_token
        ).execute()

        for it in resp.get("items", []):
            vid = it["contentDetails"].get("videoId")
            if vid:
                video_ids.append(vid)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    seen = set()
    out = []
    for v in video_ids:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out

def get_videos_dim(youtube, channel_id: str, video_ids: list[str]) -> pd.DataFrame:
    rows = []
    for ids in chunked(video_ids, 50):
        resp = youtube.videos().list(
            part="id,snippet,contentDetails,status,liveStreamingDetails",
            id=",".join(ids)
        ).execute()

        for v in resp.get("items", []):
            sn = v.get("snippet", {})
            cd = v.get("contentDetails", {})
            st = v.get("status", {})
            live = v.get("liveStreamingDetails")

            dur_s = iso8601_duration_to_seconds(cd.get("duration"))

            rows.append({
                "video_id": v["id"],
                "channel_id": channel_id,
                "title": (sn.get("title") or "")[:200],
                "description": sn.get("description"),
                "publish_date": (sn.get("publishedAt", "")[:10] or None),
                "duration_seconds": dur_s,
                "tags": ",".join(sn.get("tags", [])) if sn.get("tags") else None,
                "thumbnail_url": (sn.get("thumbnails", {}) or {}).get("high", {}).get("url"),
                "language": sn.get("defaultLanguage") or sn.get("defaultAudioLanguage"),
                "is_monetized": None,
                "category_id": sn.get("categoryId"),
                "privacy_status": st.get("privacyStatus"),
                "is_live": bool(live) or (sn.get("liveBroadcastContent") in ("live", "upcoming")),
                "video_url": f"https://www.youtube.com/watch?v={v['id']}",
            })

    return pd.DataFrame(rows)


# =============================================================================
# BUILD DIMS
# =============================================================================

US_FED_HOLIDAYS_FIXED = {
    (1, 1): "New Year's Day",
    (7, 4): "Independence Day",
    (11, 11): "Veterans Day",
    (12, 25): "Christmas Day",
}

def build_dim_date(start_date: date, end_date: date) -> pd.DataFrame:
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"date": dates})

    df["date_id"] = df["date"].dt.strftime("%Y%m%d").astype("int32")
    df["year"] = df["date"].dt.year
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["month_name"] = df["date"].dt.strftime("%B")
    df["week_of_year"] = df["date"].dt.strftime("%U").astype("int32")
    df["iso_week"] = df["date"].dt.isocalendar().week.astype("int32")

    week_start = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
    df["week_start_date"] = week_start.dt.date
    df["week_end_date"] = (week_start + pd.Timedelta(days=6)).dt.date

    df["day_of_month"] = df["date"].dt.day
    df["day_of_week"] = df["date"].dt.dayofweek + 1  # isoweekday: Mon=1 … Sun=7
    df["day_name"] = df["date"].dt.strftime("%A")
    df["is_weekend"] = df["day_of_week"].isin([6, 7])

    df["holiday_name"] = [
        US_FED_HOLIDAYS_FIXED.get((m, d))
        for m, d in zip(df["date"].dt.month, df["date"].dt.day)
    ]
    df["is_holiday"] = df["holiday_name"].notna()

    df["date"] = df["date"].dt.date

    return df[[
        "date_id", "date",
        "year", "quarter", "month", "month_name",
        "week_of_year", "iso_week", "week_start_date", "week_end_date",
        "day_of_month", "day_of_week", "day_name",
        "is_weekend", "is_holiday", "holiday_name"
    ]].copy()

def build_dim_content_length() -> pd.DataFrame:
    return pd.DataFrame([
        {"content_length_category": "short",  "definition": f"<= {SHORT_MAX} seconds"},
        {"content_length_category": "medium", "definition": f"{SHORT_MAX+1}–{MEDIUM_MAX} seconds"},
        {"content_length_category": "long",   "definition": f"> {MEDIUM_MAX} seconds"},
    ])


# =============================================================================
# ANALYTICS: PER-VIDEO DAILY (NO day,video)
# =============================================================================

METRICS_MAIN = ",".join([
    "views",
    "estimatedMinutesWatched",
    "averageViewDuration",
    "averageViewPercentage",
    "videoThumbnailImpressions",
    "videoThumbnailImpressionsClickRate",
])

# Impression metrics queried separately in bulk mode because some channels/API
# versions don't support them with dimensions=day,video
METRICS_BULK_CORE = ",".join([
    "views",
    "estimatedMinutesWatched",
    "averageViewDuration",
    "averageViewPercentage",
])

METRICS_BULK_IMPRESSIONS = ",".join([
    "videoThumbnailImpressions",
    "videoThumbnailImpressionsClickRate",
])

# engagements are optional and sometimes not available on some channels
METRICS_ENG = ",".join([
    "likes",
    "comments",
    "shares",
    "subscribersGained",
])

def query_video_day_report(yt_analytics, channel_id: str, video_id: str, start_date: str, end_date: str, metrics: str) -> pd.DataFrame:
    try:
        resp = execute_yt(
            yt_analytics.reports().query(
                ids=f"channel=={channel_id}",
                startDate=start_date,
                endDate=end_date,
                metrics=metrics,
                dimensions="day",
                filters=f"video=={video_id}",
                sort="day",
                maxResults=1000,
            ),
            label=f"video_day {video_id}"
        )
    except HttpError as e:
        global _impressions_supported
        status = getattr(e.resp, "status", None)
        # Only skip truly unsupported queries (NOT quota)
        if status == 400 and "not supported" in str(e).lower():
            if metrics == METRICS_BULK_IMPRESSIONS:
                _impressions_supported = False
                print("  ℹ️ Impression metrics not supported on this channel — disabling for all remaining videos")
            else:
                print(f"  ⚠️ per-video: 400 'not supported' for video={video_id} metrics='{metrics[:50]}...', skipping")
            return pd.DataFrame()
        raise

    headers = [h["name"] for h in resp.get("columnHeaders", [])]
    rows = resp.get("rows", []) or []
    df = pd.DataFrame([dict(zip(headers, r)) for r in rows])
    if df.empty:
        return df
    df.rename(columns={"day": "date"}, inplace=True)
    df["video_id"] = video_id
    df["channel_id"] = channel_id
    return df

def get_video_daily_all_metrics_per_video(
    yt_analytics, channel_id: str, start_date: str, end_date: str, video_ids: list[str], cp: dict, chunk_key: str
) -> pd.DataFrame:
    frames = []

    done = set(cp.get(chunk_key, []))

    for i, vid in enumerate(video_ids, start=1):
        if vid in done:
            continue

        # Core metrics (views/watchtime/avd/retention) — always supported
        df_main = query_video_day_report(yt_analytics, channel_id, vid, start_date, end_date, METRICS_BULK_CORE)

        # Impression metrics — skip entirely once confirmed unsupported (flag set in query_video_day_report)
        df_imp = pd.DataFrame()
        if _impressions_supported:
            df_imp = query_video_day_report(yt_analytics, channel_id, vid, start_date, end_date, METRICS_BULK_IMPRESSIONS)

        # Engagements (optional)
        df_eng = pd.DataFrame()
        try:
            df_eng = query_video_day_report(yt_analytics, channel_id, vid, start_date, end_date, METRICS_ENG)
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status == 400 and "not supported" in str(e).lower():
                df_eng = pd.DataFrame()
            else:
                raise

        if df_main.empty:
            # even if empty, mark done so we don't re-query forever
            done.add(vid)
            cp[chunk_key] = sorted(done)
            save_checkpoint(cp)
            continue

        df = df_main.copy()

        if not df_imp.empty:
            df = df.merge(df_imp, on=["date", "video_id", "channel_id"], how="left")
        else:
            df["videoThumbnailImpressions"] = 0
            df["videoThumbnailImpressionsClickRate"] = 0.0

        if not df_eng.empty:
            df = df.merge(df_eng, on=["date", "video_id", "channel_id"], how="left")
        else:
            df["likes"] = 0
            df["comments"] = 0
            df["shares"] = 0
            df["subscribersGained"] = 0

        frames.append(df)

        # checkpoint after each video
        done.add(vid)
        cp[chunk_key] = sorted(done)
        save_checkpoint(cp)

        if i % 10 == 0:
            print(f"  ...progress {i}/{len(video_ids)} videos for {chunk_key}")

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # normalize + rename
    df["views"] = pd.to_numeric(df.get("views"), errors="coerce").fillna(0).astype("int64")
    df["watch_time_hours"] = (pd.to_numeric(df.get("estimatedMinutesWatched"), errors="coerce").fillna(0) / 60.0).round(2)
    df["avg_view_duration"] = pd.to_numeric(df.get("averageViewDuration"), errors="coerce").fillna(0).round(2)

    ret = pd.to_numeric(df.get("averageViewPercentage"), errors="coerce")
    df["retention_rate"] = ret.where(ret <= 1.0, ret / 100.0).round(4)

    df["impressions"] = pd.to_numeric(df.get("videoThumbnailImpressions"), errors="coerce").fillna(0).astype("int64")
    df["impression_ctr"] = pd.to_numeric(df.get("videoThumbnailImpressionsClickRate"), errors="coerce").fillna(0).round(4)

    df["likes"] = pd.to_numeric(df.get("likes"), errors="coerce").fillna(0).astype("int64")
    df["comments"] = pd.to_numeric(df.get("comments"), errors="coerce").fillna(0).astype("int64")
    df["shares"] = pd.to_numeric(df.get("shares"), errors="coerce").fillna(0).astype("int64")
    df["subscribers_gained"] = pd.to_numeric(df.get("subscribersGained"), errors="coerce").fillna(0).astype("int64")

    df["engagements_total"] = (df["likes"] + df["comments"] + df["shares"]).astype("int64")

    return df[[
        "video_id", "channel_id", "date",
        "views", "watch_time_hours", "avg_view_duration",
        "impressions", "impression_ctr", "retention_rate",
        "likes", "comments", "shares", "engagements_total",
        "subscribers_gained",
    ]].copy()


def query_all_videos_day_report(yt_analytics, channel_id: str, start_date: str, end_date: str, metrics: str) -> pd.DataFrame:
    """
    Fetch daily per-video metrics for the entire channel in one bulk request using
    dimensions=day,video (paginated).  Returns an empty DataFrame if the metric
    combination is unsupported.
    """
    all_rows = []
    headers = None
    page_token = None

    while True:
        kwargs = dict(
            ids=f"channel=={channel_id}",
            startDate=start_date,
            endDate=end_date,
            metrics=metrics,
            dimensions="day,video",
            sort="day",
            maxResults=200,
        )
        if page_token:
            kwargs["pageToken"] = page_token

        try:
            resp = execute_yt(
                yt_analytics.reports().query(**kwargs),
                label="bulk_video_day",
            )
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status == 400 and "not supported" in str(e).lower():
                print(f"  ⚠️ bulk day,video: 400 'not supported' for metrics='{metrics[:60]}...', returning empty")
                return pd.DataFrame()
            raise

        if headers is None:
            headers = [h["name"] for h in resp.get("columnHeaders", [])]
        rows = resp.get("rows", []) or []
        all_rows.extend(rows)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(zip(headers, r)) for r in all_rows])
    df.rename(columns={"day": "date", "video": "video_id"}, inplace=True)
    df["channel_id"] = channel_id
    return df


def get_video_daily_bulk(
    yt_analytics, channel_id: str, start_date: str, end_date: str, cp: dict, chunk_key: str
) -> pd.DataFrame:
    """
    Bulk replacement for get_video_daily_all_metrics_per_video.
    Makes 2 API calls per chunk (regardless of video count) instead of 2×N_videos.
    Falls back gracefully if dimensions=day,video is unsupported.
    """
    if cp.get(chunk_key) == "done":
        print(f"  Chunk {chunk_key} already complete (checkpoint), skipping.")
        return pd.DataFrame()

    # Core metrics (views / watchtime / retention) — these always work with day,video
    df_main = query_all_videos_day_report(yt_analytics, channel_id, start_date, end_date, METRICS_BULK_CORE)

    # Impression metrics — some channels/API configs don't support these with day,video
    df_imp = query_all_videos_day_report(yt_analytics, channel_id, start_date, end_date, METRICS_BULK_IMPRESSIONS)

    # Engagement metrics
    df_eng = pd.DataFrame()
    try:
        df_eng = query_all_videos_day_report(yt_analytics, channel_id, start_date, end_date, METRICS_ENG)
    except HttpError as e:
        status = getattr(e.resp, "status", None)
        if status == 400 and "not supported" in str(e).lower():
            df_eng = pd.DataFrame()
        else:
            raise

    if df_main.empty:
        cp[chunk_key] = "done"
        save_checkpoint(cp)
        return pd.DataFrame()

    df = df_main.copy()

    if not df_imp.empty:
        df = df.merge(df_imp, on=["date", "video_id", "channel_id"], how="left")
    else:
        df["videoThumbnailImpressions"] = 0
        df["videoThumbnailImpressionsClickRate"] = 0.0

    if not df_eng.empty:
        df = df.merge(df_eng, on=["date", "video_id", "channel_id"], how="left")
    else:
        for col in ["likes", "comments", "shares", "subscribersGained"]:
            df[col] = 0

    df["views"] = pd.to_numeric(df.get("views"), errors="coerce").fillna(0).astype("int64")
    df["watch_time_hours"] = (pd.to_numeric(df.get("estimatedMinutesWatched"), errors="coerce").fillna(0) / 60.0).round(2)
    df["avg_view_duration"] = pd.to_numeric(df.get("averageViewDuration"), errors="coerce").fillna(0).round(2)

    ret = pd.to_numeric(df.get("averageViewPercentage"), errors="coerce")
    df["retention_rate"] = ret.where(ret <= 1.0, ret / 100.0).round(4)

    df["impressions"] = pd.to_numeric(df.get("videoThumbnailImpressions"), errors="coerce").fillna(0).astype("int64")
    df["impression_ctr"] = pd.to_numeric(df.get("videoThumbnailImpressionsClickRate"), errors="coerce").fillna(0).round(4)

    df["likes"] = pd.to_numeric(df.get("likes"), errors="coerce").fillna(0).astype("int64")
    df["comments"] = pd.to_numeric(df.get("comments"), errors="coerce").fillna(0).astype("int64")
    df["shares"] = pd.to_numeric(df.get("shares"), errors="coerce").fillna(0).astype("int64")
    df["subscribers_gained"] = pd.to_numeric(df.get("subscribersGained"), errors="coerce").fillna(0).astype("int64")
    df["engagements_total"] = (df["likes"] + df["comments"] + df["shares"]).astype("int64")

    cp[chunk_key] = "done"
    save_checkpoint(cp)

    return df[[
        "video_id", "channel_id", "date",
        "views", "watch_time_hours", "avg_view_duration",
        "impressions", "impression_ctr", "retention_rate",
        "likes", "comments", "shares", "engagements_total",
        "subscribers_gained",
    ]].copy()


def get_channel_daily_analytics(yt_analytics, channel_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    metrics = "views,estimatedMinutesWatched,subscribersGained,subscribersLost"

    resp = execute_yt(
        yt_analytics.reports().query(
            ids=f"channel=={channel_id}",
            startDate=start_date,
            endDate=end_date,
            metrics=metrics,
            dimensions="day",
            sort="day",
            maxResults=1000,
        ),
        label="channel_day"
    )

    headers = [h["name"] for h in resp.get("columnHeaders", [])]
    rows = resp.get("rows", []) or []
    df = pd.DataFrame([dict(zip(headers, r)) for r in rows])
    if df.empty:
        return df

    df.rename(columns={"day": "date"}, inplace=True)
    df["channel_id"] = channel_id

    df["channel_views"] = pd.to_numeric(df["views"], errors="coerce").fillna(0).astype("int64")
    df["channel_watch_time_hours"] = (pd.to_numeric(df["estimatedMinutesWatched"], errors="coerce").fillna(0) / 60.0).round(2)
    df["channel_subscribers_gained"] = pd.to_numeric(df["subscribersGained"], errors="coerce").fillna(0).astype("int64")
    df["channel_subscribers_lost"] = pd.to_numeric(df["subscribersLost"], errors="coerce").fillna(0).astype("int64")
    df["channel_subscribers_net_change"] = (df["channel_subscribers_gained"] - df["channel_subscribers_lost"]).astype("int64")

    return df[[
        "channel_id", "date",
        "channel_views", "channel_watch_time_hours",
        "channel_subscribers_gained", "channel_subscribers_lost",
        "channel_subscribers_net_change"
    ]].copy()


# =============================================================================
# MERGE SQL (UNCHANGED from your version)
# =============================================================================

# Runs in its own batch before DIMS_MERGE_SQL so SQL Server doesn't reject the
# MERGE's reference to video_url before the column exists (batch-compile issue).
ADD_VIDEO_URL_COLUMN_SQL = f"""
IF NOT EXISTS (
  SELECT 1 FROM sys.columns
  WHERE object_id = OBJECT_ID('{DW_YT_SCHEMA}.dim_video') AND name = 'video_url'
)
  ALTER TABLE {DW_YT_SCHEMA}.dim_video ADD video_url NVARCHAR(200);
"""

DIMS_MERGE_SQL = f"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{STG_SCHEMA}')
    EXEC('CREATE SCHEMA {STG_SCHEMA}');

MERGE {DW_YT_SCHEMA}.dim_content_length AS tgt
USING {STG_SCHEMA}.dim_content_length AS src
ON tgt.content_length_category = src.content_length_category
WHEN MATCHED THEN UPDATE SET tgt.definition = src.definition
WHEN NOT MATCHED THEN
  INSERT (content_length_category, definition)
  VALUES (src.content_length_category, src.definition);

MERGE {DW_SHARED_SCHEMA}.dim_date AS tgt
USING {STG_SCHEMA}.dim_date AS src
ON tgt.date_id = src.date_id
WHEN MATCHED THEN UPDATE SET
  tgt.[date] = src.[date],
  tgt.[year] = src.[year],
  tgt.[quarter] = src.[quarter],
  tgt.[month] = src.[month],
  tgt.[month_name] = src.[month_name],
  tgt.[week_of_year] = src.[week_of_year],
  tgt.[iso_week] = src.[iso_week],
  tgt.[week_start_date] = src.[week_start_date],
  tgt.[week_end_date] = src.[week_end_date],
  tgt.[day_of_month] = src.[day_of_month],
  tgt.[day_of_week] = src.[day_of_week],
  tgt.[day_name] = src.[day_name],
  tgt.[is_weekend] = src.[is_weekend],
  tgt.[is_holiday] = src.[is_holiday],
  tgt.[holiday_name] = src.[holiday_name]
WHEN NOT MATCHED THEN
  INSERT (
    date_id, [date], [year], [quarter], [month], [month_name],
    [week_of_year], [iso_week], [week_start_date], [week_end_date],
    [day_of_month], [day_of_week], [day_name],
    [is_weekend], [is_holiday], [holiday_name]
  )
  VALUES (
    src.date_id, src.[date], src.[year], src.[quarter], src.[month], src.[month_name],
    src.[week_of_year], src.[iso_week], src.[week_start_date], src.[week_end_date],
    src.[day_of_month], src.[day_of_week], src.[day_name],
    src.[is_weekend], src.[is_holiday], src.[holiday_name]
  );

MERGE {DW_YT_SCHEMA}.dim_channel AS tgt
USING {STG_SCHEMA}.dim_channel AS src
ON tgt.channel_id = src.channel_id
WHEN MATCHED THEN UPDATE SET
  tgt.channel_name = src.channel_name,
  tgt.category = src.category,
  tgt.created_at = src.created_at,
  tgt.country = src.country,
  tgt.subscriber_count = src.subscriber_count,
  tgt.total_videos = src.total_videos,
  tgt.is_verified = src.is_verified,
  tgt.custom_url = src.custom_url,
  tgt.description = src.description,
  tgt.banner_url = src.banner_url
WHEN NOT MATCHED THEN
  INSERT (
    channel_id, channel_name, category, created_at, country,
    subscriber_count, total_videos, is_verified, custom_url, description, banner_url
  )
  VALUES (
    src.channel_id, src.channel_name, src.category, src.created_at, src.country,
    src.subscriber_count, src.total_videos, src.is_verified, src.custom_url, src.description, src.banner_url
  );

MERGE {DW_YT_SCHEMA}.dim_video AS tgt
USING {STG_SCHEMA}.dim_video AS src
ON tgt.video_id = src.video_id
WHEN MATCHED THEN UPDATE SET
  tgt.channel_id = src.channel_id,
  tgt.title = src.title,
  tgt.description = src.description,
  tgt.publish_date = src.publish_date,
  tgt.duration_seconds = src.duration_seconds,
  tgt.tags = src.tags,
  tgt.thumbnail_url = src.thumbnail_url,
  tgt.[language] = src.[language],
  tgt.is_monetized = src.is_monetized,
  tgt.category_id = src.category_id,
  tgt.privacy_status = src.privacy_status,
  tgt.is_live = src.is_live,
  tgt.video_url = src.video_url
WHEN NOT MATCHED THEN
  INSERT (
    video_id, channel_id, title, description, publish_date, duration_seconds,
    tags, thumbnail_url, [language], is_monetized, category_id, privacy_status, is_live,
    video_url
  )
  VALUES (
    src.video_id, src.channel_id, src.title, src.description, src.publish_date, src.duration_seconds,
    src.tags, src.thumbnail_url, src.[language], src.is_monetized, src.category_id, src.privacy_status, src.is_live,
    src.video_url
  );
"""

VIDEO_FACTS_MERGE_SQL = f"""
MERGE {DW_YT_SCHEMA}.fact_youtube_video_daily AS tgt
USING (
  SELECT
    f.video_id,
    f.channel_id,
    CONVERT(int, REPLACE(f.[date], '-', '')) AS date_id,
    CASE
      WHEN v.duration_seconds IS NULL THEN NULL
      WHEN v.duration_seconds <= {SHORT_MAX} THEN 'short'
      WHEN v.duration_seconds <= {MEDIUM_MAX} THEN 'medium'
      ELSE 'long'
    END AS content_length_category,
    f.views,
    NULL AS unique_views,
    f.watch_time_hours,
    f.avg_view_duration,
    f.impressions,
    f.impression_ctr,
    f.likes,
    f.comments,
    f.shares,
    f.engagements_total,
    f.retention_rate,
    f.subscribers_gained,
    v.is_live
  FROM {STG_SCHEMA}.fact_video_daily f
  LEFT JOIN {DW_YT_SCHEMA}.dim_video v
    ON v.video_id = f.video_id
) AS src
ON tgt.video_id = src.video_id AND tgt.date_id = src.date_id
WHEN MATCHED THEN UPDATE SET
  tgt.channel_id = src.channel_id,
  tgt.content_length_category = src.content_length_category,
  tgt.views = src.views,
  tgt.watch_time_hours = src.watch_time_hours,
  tgt.avg_view_duration = src.avg_view_duration,
  tgt.impressions = src.impressions,
  tgt.impression_ctr = src.impression_ctr,
  tgt.likes = src.likes,
  tgt.comments = src.comments,
  tgt.shares = src.shares,
  tgt.engagements_total = src.engagements_total,
  tgt.retention_rate = src.retention_rate,
  tgt.subscribers_gained = src.subscribers_gained,
  tgt.is_live = src.is_live
WHEN NOT MATCHED THEN
  INSERT (
    video_id, channel_id, date_id, content_length_category,
    views, unique_views, watch_time_hours, avg_view_duration,
    impressions, impression_ctr, likes, comments, shares, engagements_total,
    retention_rate, subscribers_gained, is_live
  )
  VALUES (
    src.video_id, src.channel_id, src.date_id, src.content_length_category,
    src.views, src.unique_views, src.watch_time_hours, src.avg_view_duration,
    src.impressions, src.impression_ctr, src.likes, src.comments, src.shares, src.engagements_total,
    src.retention_rate, src.subscribers_gained, src.is_live
  );
"""

CHANNEL_FACTS_MERGE_SQL = f"""
MERGE {DW_YT_SCHEMA}.fact_youtube_channel_daily AS tgt
USING (
  SELECT
    channel_id,
    CONVERT(int, REPLACE([date], '-', '')) AS date_id,
    channel_views,
    channel_watch_time_hours,
    channel_subscribers_gained,
    channel_subscribers_lost,
    channel_subscribers_net_change,
    NULL AS channel_videos_published
  FROM {STG_SCHEMA}.fact_channel_daily
) AS src
ON tgt.channel_id = src.channel_id AND tgt.date_id = src.date_id
WHEN MATCHED THEN UPDATE SET
  tgt.channel_views = src.channel_views,
  tgt.channel_watch_time_hours = src.channel_watch_time_hours,
  tgt.channel_subscribers_gained = src.channel_subscribers_gained,
  tgt.channel_subscribers_lost = src.channel_subscribers_lost,
  tgt.channel_subscribers_net_change = src.channel_subscribers_net_change
WHEN NOT MATCHED THEN
  INSERT (
    channel_id, date_id,
    channel_views, channel_watch_time_hours,
    channel_subscribers_gained, channel_subscribers_lost,
    channel_subscribers_net_change, channel_videos_published
  )
  VALUES (
    src.channel_id, src.date_id,
    src.channel_views, src.channel_watch_time_hours,
    src.channel_subscribers_gained, src.channel_subscribers_lost,
    src.channel_subscribers_net_change, src.channel_videos_published
  );
"""


# =============================================================================
# CHUNKING
# =============================================================================

def month_chunks(start_dt: date, end_dt: date):
    cur = date(start_dt.year, start_dt.month, 1)
    while cur <= end_dt:
        # first day of next month
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)

        chunk_start = max(start_dt, cur)
        chunk_end = min(end_dt, nxt - timedelta(days=1))
        yield chunk_start, chunk_end
        cur = nxt


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Kavi Global YouTube ETL")
    parser.add_argument("--since", default=None, metavar="YYYY-MM-DD",
                        help="Start date for analytics pull. Default: earliest video publish date (full backfill).")
    parser.add_argument("--until", default=None, metavar="YYYY-MM-DD",
                        help="End date. Default: today.")
    args = parser.parse_args()

    # warm up DB (handles serverless wake / transient)
    run_sql("SELECT 1;")

    # Credentials now auto-refresh — no manual token management needed
    youtube, yt_analytics = build_google_clients()

    run_sql(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{STG_SCHEMA}') EXEC('CREATE SCHEMA {STG_SCHEMA}');")

    # Dims
    df_channel = get_my_channel(youtube)
    channel_id = df_channel.loc[0, "channel_id"]

    uploads_pid = get_uploads_playlist_id(youtube, channel_id)
    video_ids = list_all_video_ids_from_playlist(youtube, uploads_pid)
    print(f"Found {len(video_ids):,} videos in uploads playlist")

    df_video = get_videos_dim(youtube, channel_id, video_ids)

    today = date.today()
    end_dt = date.fromisoformat(args.until) if args.until else today

    if args.since:
        start_dt = date.fromisoformat(args.since)
    elif not df_video.empty and df_video["publish_date"].notna().any():
        min_pub = pd.to_datetime(df_video["publish_date"], errors="coerce").min()
        start_dt = min_pub.date() if pd.notna(min_pub) else (today - timedelta(days=365))
    else:
        start_dt = today - timedelta(days=365)
    print(f"Backfill window: {start_dt.isoformat()} → {end_dt.isoformat()}")

    df_dim_date = build_dim_date(start_dt, end_dt)
    df_len = build_dim_content_length()

    load_stage(df_len, "dim_content_length", if_exists="replace")
    load_stage(df_dim_date, "dim_date", if_exists="replace")
    load_stage(df_channel, "dim_channel", if_exists="replace")
    load_stage(df_video, "dim_video", if_exists="replace")

    run_sql(ADD_VIDEO_URL_COLUMN_SQL)
    run_sql(DIMS_MERGE_SQL)
    print("✅ Dims merged (dim_date, dim_content_length, dim_channel, dim_video)")

    # Facts — each chunk is staged + merged into the DW immediately after collection.
    # This means a crash at any point only loses at most the current in-progress chunk;
    # all completed chunks are already safely in the DW.
    # Checkpoint uses "merged" sentinel to skip fully-completed chunks on restart.
    chunks = list(month_chunks(start_dt, end_dt)) if CHUNK_BY_MONTH else [(start_dt, end_dt)]
    cp = load_checkpoint()

    # Build publish-date lookup so each chunk only queries videos that existed by then
    pub_date_map = (
        df_video.dropna(subset=["publish_date"])
        .set_index("video_id")["publish_date"]
        .to_dict()
    )

    for i, (c_start, c_end) in enumerate(chunks, start=1):
        s = c_start.isoformat()
        e = c_end.isoformat()
        chunk_key = f"{s}_to_{e}"

        # Skip chunks already fully merged into the DW
        if cp.get(chunk_key) == "merged":
            print(f"\n=== FACT CHUNK {i}/{len(chunks)}: {chunk_key} — already merged, skipping ===")
            continue

        # Only query videos published on or before this chunk's end date
        chunk_video_ids = [vid for vid in video_ids if pub_date_map.get(vid, "9999-99-99") <= e]
        print(f"\n=== FACT CHUNK {i}/{len(chunks)}: {chunk_key} ({len(chunk_video_ids)} eligible videos) ===")

        if USE_BULK_ANALYTICS:
            df_fact_video = get_video_daily_bulk(yt_analytics, channel_id, s, e, cp, chunk_key)
        else:
            df_fact_video = get_video_daily_all_metrics_per_video(
                yt_analytics, channel_id, s, e, chunk_video_ids, cp, chunk_key
            )
        print(f"video_daily rows: {len(df_fact_video):,}")

        df_fact_channel = get_channel_daily_analytics(yt_analytics, channel_id, s, e)
        print(f"channel_daily rows: {len(df_fact_channel):,}")

        # Stage + merge this chunk immediately so it survives any future crash
        if not df_fact_video.empty:
            load_stage(df_fact_video, "fact_video_daily", if_exists="replace")
            run_sql(VIDEO_FACTS_MERGE_SQL)
        if not df_fact_channel.empty:
            load_stage(df_fact_channel, "fact_channel_daily", if_exists="replace")
            run_sql(CHANNEL_FACTS_MERGE_SQL)

        # Mark chunk as fully in the DW — restart will skip it entirely
        cp[chunk_key] = "merged"
        save_checkpoint(cp)

    print("🎉 Full backfill complete: dims + facts merged into dw_youtube / dw_shared")

if __name__ == "__main__":
    main()
