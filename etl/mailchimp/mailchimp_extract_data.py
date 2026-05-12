import os
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
import requests
import pandas as pd
from flask import Flask, request, session, redirect, url_for
from requests_oauthlib import OAuth2Session

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-only-change-me")

# =========================
# OAuth Config
# =========================
CLIENT_ID = os.getenv("MAILCHIMP_CLIENT_ID")
CLIENT_SECRET = os.getenv("MAILCHIMP_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    print("WARNING: Please set MAILCHIMP_CLIENT_ID and MAILCHIMP_CLIENT_SECRET env vars.")

AUTH_URL = "https://login.mailchimp.com/oauth2/authorize"
TOKEN_URL = "https://login.mailchimp.com/oauth2/token"
METADATA_URL = "https://login.mailchimp.com/oauth2/metadata"

REDIRECT_URI = os.getenv("MAILCHIMP_REDIRECT_URI", "http://127.0.0.1:8000/callback")
SCOPE: List[str] = []

EXPORT_DIR = os.getenv("MAILCHIMP_EXPORT_DIR", "export_mailchimp")
os.makedirs(EXPORT_DIR, exist_ok=True)

SINCE_SEND_TIME = os.getenv("MAILCHIMP_SINCE_SEND_TIME", "2018-01-01T00:00:00Z")
AUDIENCE_HISTORY_START_DATE = os.getenv("MAILCHIMP_AUDIENCE_HISTORY_START_DATE", "2018-01-01")
TOKEN_FILE = os.getenv("MAILCHIMP_TOKEN_FILE", "token.json")


# =========================
# Helpers
# =========================
def make_oauth(state: Optional[str] = None) -> OAuth2Session:
    return OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI, scope=SCOPE, state=state)


def auth_headers(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"OAuth {access_token}"}


def get_dc_and_api_root(access_token: str) -> Tuple[str, str]:
    r = requests.get(METADATA_URL, headers=auth_headers(access_token), timeout=30)
    r.raise_for_status()
    dc = r.json()["dc"]
    return dc, f"https://{dc}.api.mailchimp.com/3.0"


def mc_get(api_root: str, access_token: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{api_root}{path}"
    r = requests.get(url, headers=auth_headers(access_token), params=params or {}, timeout=90)
    r.raise_for_status()
    return r.json()


def paginate_offset(
    api_root: str,
    access_token: str,
    path: str,
    item_key: str,
    base_params: Optional[Dict[str, Any]] = None,
    count: int = 1000,
    max_pages: int = 10000,
    sleep_s: float = 0.12,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    params = dict(base_params or {})
    params["count"] = count

    for _ in range(max_pages):
        params["offset"] = offset
        data = mc_get(api_root, access_token, path, params=params)
        items = data.get(item_key, []) or []
        out.extend(items)

        if len(items) < count:
            break

        offset += count
        time.sleep(sleep_s)

    return out


def parse_iso_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def parse_iso_date(s: Optional[str]) -> Optional[date]:
    dt = parse_iso_datetime(s)
    return dt.date() if dt else None


def safe_get(d: Any, path: List[str], default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def month_end(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1) - timedelta(days=1)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def date_to_date_id(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def generate_dim_date(start_date: date, end_date: date) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    current = start_date

    while current <= end_date:
        date_id = int(current.strftime("%Y%m%d"))
        quarter = (current.month - 1) // 3 + 1
        iso_week = current.isocalendar()[1]
        week_of_year = int(current.strftime("%U"))
        week_start = current - timedelta(days=current.weekday())
        week_end = week_start + timedelta(days=6)

        rows.append({
            "date_id": date_id,
            "date": current.isoformat(),
            "year": current.year,
            "quarter": quarter,
            "month": current.month,
            "month_name": current.strftime("%B"),
            "week_of_year": week_of_year,
            "iso_week": iso_week,
            "week_start_date": week_start.isoformat(),
            "week_end_date": week_end.isoformat(),
            "day_of_month": current.day,
            "day_of_week": current.weekday() + 1,
            "day_name": current.strftime("%A"),
            "is_weekend": current.weekday() >= 5,
            "is_holiday": False,
            "holiday_name": None,
        })
        current += timedelta(days=1)

    return pd.DataFrame(rows)


def first_non_null_date(*vals: Optional[date]) -> Optional[date]:
    for v in vals:
        if v is not None:
            return v
    return None


def coerce_numeric(series: pd.Series, fill_value=0):
    return pd.to_numeric(series, errors="coerce").fillna(fill_value)


# =========================
# Member-based reconstruction
# =========================
def fetch_all_members_for_list(api_root: str, access_token: str, list_id: str) -> List[Dict[str, Any]]:
    params = {
        "sort_field": "last_changed",
        "sort_dir": "DESC",
    }
    return paginate_offset(
        api_root=api_root,
        access_token=access_token,
        path=f"/lists/{list_id}/members",
        item_key="members",
        base_params=params,
        count=1000,
        max_pages=10000,
        sleep_s=0.12,
    )


def build_member_dimension(all_members_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(all_members_rows)
    if df.empty:
        return pd.DataFrame(columns=[
            "audience_id",
            "member_id",
            "email_id",
            "email_address",
            "status",
            "vip",
            "language",
            "source",
            "timestamp_signup",
            "timestamp_opt",
            "last_changed",
            "member_rating",
            "avg_open_rate",
            "avg_click_rate",
            "created_at",
            "updated_at",
        ])

    now_str = datetime.utcnow().isoformat(timespec="seconds")
    out = pd.DataFrame({
        "audience_id": df["audience_id"].astype(str),
        "member_id": df.get("id"),
        "email_id": df.get("unique_email_id"),
        "email_address": df.get("email_address"),
        "status": df.get("status"),
        "vip": df.get("vip"),
        "language": df.get("language"),
        "source": df.get("source"),
        "timestamp_signup": df.get("timestamp_signup"),
        "timestamp_opt": df.get("timestamp_opt"),
        "last_changed": df.get("last_changed"),
        "member_rating": df.get("member_rating"),
        "avg_open_rate": df["stats"].apply(lambda x: x.get("avg_open_rate") if isinstance(x, dict) else None),
        "avg_click_rate": df["stats"].apply(lambda x: x.get("avg_click_rate") if isinstance(x, dict) else None),
        "created_at": now_str,
        "updated_at": now_str,
    })

    return out


def build_member_based_audience_monthly(
    dim_audience: pd.DataFrame,
    dim_member: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    months = pd.date_range(
        start=pd.Timestamp(start_date).replace(day=1),
        end=pd.Timestamp(end_date).replace(day=1),
        freq="MS"
    )

    audience_ids = dim_audience["audience_id"].dropna().astype(str).unique().tolist()

    if not audience_ids:
        return pd.DataFrame()

    grid = pd.MultiIndex.from_product(
        [audience_ids, months],
        names=["audience_id", "month_start"]
    ).to_frame(index=False)

    grid["month_start_date_id"] = grid["month_start"].dt.strftime("%Y%m%d").astype(int)
    grid["month_label"] = grid["month_start"].dt.strftime("%Y-%m")
    grid["month_end"] = grid["month_start"].dt.date.apply(month_end)

    if dim_member.empty:
        out = grid.copy()
        metric_cols = [
            "new_subscribes",
            "new_pending",
            "unsubscribes_est",
            "cleaned_est",
            "transactional_bounced_est",
            "month_end_subscribed_count_est",
            "month_end_pending_count_est",
            "month_end_unsubscribed_count_est",
            "month_end_cleaned_count_est",
            "month_end_total_known_members_est",
            "members_with_open_activity_count",
            "members_with_click_activity_count",
            "avg_open_rate_current_members",
            "avg_click_rate_current_members",
        ]
        for c in metric_cols:
            out[c] = 0

        now_str = datetime.utcnow().isoformat(timespec="seconds")
        out["created_at"] = now_str
        out["updated_at"] = now_str
        return out.drop(columns=["month_end"])

    m = dim_member.copy()

    for c in ["audience_id", "status", "email_address", "member_id", "email_id"]:
        if c in m.columns:
            m[c] = m[c].astype(str)

    m["signup_date"] = m["timestamp_signup"].apply(parse_iso_date) if "timestamp_signup" in m.columns else None
    m["opt_date"] = m["timestamp_opt"].apply(parse_iso_date) if "timestamp_opt" in m.columns else None
    m["last_changed_date"] = m["last_changed"].apply(parse_iso_date) if "last_changed" in m.columns else None

    m["subscribe_event_date"] = [
        first_non_null_date(opt_d, sign_d)
        for opt_d, sign_d in zip(m["opt_date"], m["signup_date"])
    ]

    m["subscribe_event_month"] = m["subscribe_event_date"].apply(
        lambda x: month_start(x).strftime("%Y-%m") if pd.notnull(x) else None
    )
    m["last_changed_month"] = m["last_changed_date"].apply(
        lambda x: month_start(x).strftime("%Y-%m") if pd.notnull(x) else None
    )

    subscribed_base = m[m["subscribe_event_month"].notna()].copy()
    subscribed_base["is_new_subscribe"] = (subscribed_base["status"] != "pending").astype(int)
    subscribed_base["is_new_pending"] = (subscribed_base["status"] == "pending").astype(int)

    subs_monthly = (
        subscribed_base
        .groupby(["audience_id", "subscribe_event_month"], as_index=False)
        .agg(
            new_subscribes=("is_new_subscribe", "sum"),
            new_pending=("is_new_pending", "sum")
        )
        .rename(columns={"subscribe_event_month": "month_label"})
    )

    unsubs_monthly = (
        m[(m["status"] == "unsubscribed") & (m["last_changed_month"].notna())]
        .groupby(["audience_id", "last_changed_month"], as_index=False)
        .size()
        .rename(columns={"size": "unsubscribes_est", "last_changed_month": "month_label"})
    )

    cleaned_monthly = (
        m[(m["status"] == "cleaned") & (m["last_changed_month"].notna())]
        .groupby(["audience_id", "last_changed_month"], as_index=False)
        .size()
        .rename(columns={"size": "cleaned_est", "last_changed_month": "month_label"})
    )

    bounced_monthly = (
        m[(m["status"] == "transactional") & (m["last_changed_month"].notna())]
        .groupby(["audience_id", "last_changed_month"], as_index=False)
        .size()
        .rename(columns={"size": "transactional_bounced_est", "last_changed_month": "month_label"})
    )

    audience_member_engagement = (
        m.groupby("audience_id", as_index=False)
        .agg(
            members_with_open_activity_count=("avg_open_rate", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).gt(0).sum() if "avg_open_rate" in m.columns else 0),
            members_with_click_activity_count=("avg_click_rate", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).gt(0).sum() if "avg_click_rate" in m.columns else 0),
            avg_open_rate_current_members=("avg_open_rate", lambda s: pd.to_numeric(s, errors="coerce").mean()),
            avg_click_rate_current_members=("avg_click_rate", lambda s: pd.to_numeric(s, errors="coerce").mean()),
        )
    )

    snapshot_rows = []
    metric_now = datetime.utcnow().isoformat(timespec="seconds")

    member_records = m[[
        "audience_id",
        "status",
        "subscribe_event_date",
        "last_changed_date",
    ]].to_dict("records")

    month_labels = grid["month_label"].tolist()
    month_starts = grid["month_start"].tolist()
    month_ends = grid["month_end"].tolist()
    audience_seq = grid["audience_id"].tolist()

    for audience_id, ms, me, ml in zip(audience_seq, month_starts, month_ends, month_labels):
        subscribed_count = 0
        pending_count = 0
        unsubscribed_count = 0
        cleaned_count = 0
        total_known = 0

        for r in member_records:
            if r["audience_id"] != audience_id:
                continue

            sub_dt = r["subscribe_event_date"]
            chg_dt = r["last_changed_date"]
            status = r["status"]

            if sub_dt is None or sub_dt > me:
                continue

            total_known += 1

            if status == "subscribed":
                subscribed_count += 1
            elif status == "pending":
                pending_count += 1
            elif status == "unsubscribed":
                if chg_dt is not None and chg_dt <= me:
                    unsubscribed_count += 1
                else:
                    subscribed_count += 1
            elif status == "cleaned":
                if chg_dt is not None and chg_dt <= me:
                    cleaned_count += 1
                else:
                    subscribed_count += 1

        snapshot_rows.append({
            "audience_id": audience_id,
            "month_label": ml,
            "month_start_date_id": int(ms.strftime("%Y%m%d")),
            "month_end_subscribed_count_est": subscribed_count,
            "month_end_pending_count_est": pending_count,
            "month_end_unsubscribed_count_est": unsubscribed_count,
            "month_end_cleaned_count_est": cleaned_count,
            "month_end_total_known_members_est": total_known,
            "created_at": metric_now,
            "updated_at": metric_now,
        })

    snapshots = pd.DataFrame(snapshot_rows)

    out = grid[["audience_id", "month_start", "month_start_date_id", "month_label"]].copy()
    out = out.merge(subs_monthly, on=["audience_id", "month_label"], how="left")
    out = out.merge(unsubs_monthly, on=["audience_id", "month_label"], how="left")
    out = out.merge(cleaned_monthly, on=["audience_id", "month_label"], how="left")
    out = out.merge(bounced_monthly, on=["audience_id", "month_label"], how="left")
    out = out.merge(snapshots, on=["audience_id", "month_label", "month_start_date_id"], how="left")
    out = out.merge(audience_member_engagement, on="audience_id", how="left")

    fill_zero_cols = [
        "new_subscribes",
        "new_pending",
        "unsubscribes_est",
        "cleaned_est",
        "transactional_bounced_est",
        "month_end_subscribed_count_est",
        "month_end_pending_count_est",
        "month_end_unsubscribed_count_est",
        "month_end_cleaned_count_est",
        "month_end_total_known_members_est",
        "members_with_open_activity_count",
        "members_with_click_activity_count",
    ]
    for c in fill_zero_cols:
        out[c] = coerce_numeric(out[c], 0).astype(int)

    out["avg_open_rate_current_members"] = coerce_numeric(out["avg_open_rate_current_members"], 0.0)
    out["avg_click_rate_current_members"] = coerce_numeric(out["avg_click_rate_current_members"], 0.0)

    now_str = datetime.utcnow().isoformat(timespec="seconds")
    if "created_at" not in out.columns:
        out["created_at"] = now_str
    else:
        out["created_at"] = out["created_at"].fillna(now_str)

    if "updated_at" not in out.columns:
        out["updated_at"] = now_str
    else:
        out["updated_at"] = out["updated_at"].fillna(now_str)

    out = out.drop(columns=["month_start"]).sort_values(["audience_id", "month_start_date_id"]).reset_index(drop=True)

    return out


# =========================
# Routes
# =========================
@app.route("/")
def index():
    oauth = make_oauth()
    auth_url, state = oauth.authorization_url(AUTH_URL)
    session["oauth_state"] = state
    return f"""
    <h2>Mailchimp OAuth</h2>
    <p><a href="{auth_url}">Connect Mailchimp</a></p>
    <p>Redirect URI: <code>{REDIRECT_URI}</code></p>
    <p>Export since: <code>{SINCE_SEND_TIME}</code></p>
    <p>Audience history start date: <code>{AUDIENCE_HISTORY_START_DATE}</code></p>
    <p>After auth, open: <a href="/export_star_schema">/export_star_schema</a></p>
    """


@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return f"Missing code. Full query: {request.query_string.decode()}", 400

    oauth = make_oauth(state=session.get("oauth_state"))
    token = oauth.fetch_token(
        TOKEN_URL,
        client_secret=CLIENT_SECRET,
        code=code,
        include_client_id=True,
    )

    access_token = token["access_token"]
    dc, api_root = get_dc_and_api_root(access_token)

    acct = None
    email = None
    try:
        root = mc_get(api_root, access_token, "/")
        acct = root.get("account_name")
        email = root.get("email")
    except Exception:
        pass

    session["mailchimp_account_name"] = acct
    session["mailchimp_email"] = email
    session["mailchimp_token"] = token
    session["mailchimp_dc"] = dc
    session["mailchimp_api_root"] = api_root

    import json
    token_data = {"access_token": access_token, "api_root": api_root}
    with open(TOKEN_FILE, "w") as f:
        json.dump(token_data, f)
    print(f"Token saved to {TOKEN_FILE}")

    return f"""
    <h3>Connected ✅</h3>
    <p><b>Account</b>: {acct or "(unknown)"}<br/>
       <b>Email</b>: {email or "(unknown)"}</p>
    <p><a href="/export_star_schema">Export Mailchimp schema</a></p>
    """


def run_export(access_token: str, api_root: str) -> dict:
    since_dt = parse_iso_datetime(SINCE_SEND_TIME)
    audience_history_start_date = datetime.strptime(AUDIENCE_HISTORY_START_DATE, "%Y-%m-%d").date()

    run_tag = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(EXPORT_DIR, run_tag)
    os.makedirs(out_dir, exist_ok=True)

    print("\n===== EXPORT START =====")
    print("since_send_time:", SINCE_SEND_TIME)
    print("audience_history_start_date:", AUDIENCE_HISTORY_START_DATE)
    print("out_dir:", os.path.abspath(out_dir))
    print("========================\n")

    try:
        root = mc_get(api_root, access_token, "/")
        account_name = root.get("account_name")
        account_email = root.get("email")

        dim_user = pd.DataFrame([{
            "user_id": 1,
            "user_name": account_name or "",
            "user_email": account_email or "",
        }])
        dim_user.to_csv(os.path.join(out_dir, "dim_user.csv"), index=False)

        lists = paginate_offset(api_root, access_token, "/lists", "lists", count=1000)
        print("lists:", len(lists))

        dim_audience_rows = []
        for lst in lists:
            list_id = lst.get("id")
            if not list_id:
                continue

            dim_audience_rows.append({
                "audience_id": list_id,
                "audience_name": lst.get("name"),
                "permission_reminder": lst.get("permission_reminder"),
                "email_type_option": lst.get("email_type_option"),
                "use_archive_bar": lst.get("use_archive_bar"),
                "notify_on_subscribe": lst.get("notify_on_subscribe"),
                "notify_on_unsubscribe": lst.get("notify_on_unsubscribe"),
                "list_rating": lst.get("list_rating"),
                "subscribe_url_short": lst.get("subscribe_url_short"),
                "subscribe_url_long": lst.get("subscribe_url_long"),
                "beamer_address": lst.get("beamer_address"),
                "visibility": lst.get("visibility"),
                "double_optin": lst.get("double_optin"),
                "has_welcome": lst.get("has_welcome"),
                "member_count": safe_get(lst, ["stats", "member_count"]),
                "unsubscribe_count": safe_get(lst, ["stats", "unsubscribe_count"]),
                "cleaned_count": safe_get(lst, ["stats", "cleaned_count"]),
                "created_at": lst.get("date_created"),
                "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
            })

        dim_mailchimp_audience = pd.DataFrame(dim_audience_rows)
        if not dim_mailchimp_audience.empty:
            dim_mailchimp_audience["created_year"] = pd.to_datetime(
                dim_mailchimp_audience["created_at"], errors="coerce"
            ).dt.year

        dim_mailchimp_audience.to_csv(os.path.join(out_dir, "dim_mailchimp_audience.csv"), index=False)

        all_campaigns = paginate_offset(
            api_root,
            access_token,
            "/campaigns",
            "campaigns",
            base_params={"sort_field": "send_time", "sort_dir": "DESC"},
            count=500,
            sleep_s=0.12,
        )

        print("campaigns_pulled_total:", len(all_campaigns))

        campaigns: List[Dict[str, Any]] = []
        for c in all_campaigns:
            if c.get("status") != "sent":
                continue
            send_dt = parse_iso_datetime(c.get("send_time"))
            if not send_dt:
                continue
            if since_dt and send_dt >= since_dt:
                campaigns.append(c)

        print("campaigns_after_filter:", len(campaigns))

        dim_campaign_rows = []
        for c in campaigns:
            dim_campaign_rows.append({
                "campaign_id": c.get("id"),
                "user_id": 1,
                "audience_id": safe_get(c, ["recipients", "list_id"]),
                "web_id": c.get("web_id"),
                "campaign_name": safe_get(c, ["settings", "title"]),
                "campaign_type": c.get("type"),
                "status": c.get("status"),
                "content_type": c.get("content_type"),
                "emails_sent": c.get("emails_sent"),
                "recipient_count": safe_get(c, ["recipients", "recipient_count"]),
                "send_time": c.get("send_time"),
                "create_time": c.get("create_time"),
                "archive_url": c.get("archive_url"),
                "long_archive_url": c.get("long_archive_url"),
                "subject_line": safe_get(c, ["settings", "subject_line"]),
                "preview_text": safe_get(c, ["settings", "preview_text"]),
                "from_name": safe_get(c, ["settings", "from_name"]),
                "reply_to": safe_get(c, ["settings", "reply_to"]),
                "list_name": safe_get(c, ["recipients", "list_name"]),
                "list_is_active": safe_get(c, ["recipients", "list_is_active"]),
                "segment_text": safe_get(c, ["recipients", "segment_text"]),
                "tracking_opens": safe_get(c, ["tracking", "opens"]),
                "tracking_html_clicks": safe_get(c, ["tracking", "html_clicks"]),
                "tracking_text_clicks": safe_get(c, ["tracking", "text_clicks"]),
                "tracking_google_analytics": safe_get(c, ["tracking", "google_analytics"]),
                "created_at": c.get("create_time"),
                "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
            })

        dim_mailchimp_campaign = pd.DataFrame(dim_campaign_rows)
        dim_mailchimp_campaign.to_csv(os.path.join(out_dir, "dim_mailchimp_campaign.csv"), index=False)

        reports: List[Dict[str, Any]] = []
        report_errors = 0

        for c in campaigns:
            cid = c.get("id")
            if not cid:
                continue
            try:
                rep = mc_get(api_root, access_token, f"/reports/{cid}")
                reports.append(rep)
            except Exception as e:
                report_errors += 1
                print("report_error:", cid, str(e))
            time.sleep(0.10)

        print("reports_ok:", len(reports), "reports_err:", report_errors)
        report_map = {r.get("id"): r for r in reports if r.get("id")}

        fact_campaign_rows = []
        for c in campaigns:
            cid = c.get("id")
            send_dt = parse_iso_datetime(c.get("send_time"))
            if not cid or not send_dt:
                continue

            r = report_map.get(cid)
            if not r:
                continue

            month_start_date_id = date_to_date_id(month_start(send_dt.date()))

            fact_campaign_rows.append({
                "campaign_id": cid,
                "user_id": 1,
                "audience_id": safe_get(c, ["recipients", "list_id"]),
                "month_start_date_id": month_start_date_id,
                "recipient_count": safe_get(c, ["recipients", "recipient_count"]),
                "emails_sent": int(r.get("emails_sent", 0) or 0),
                "opens_total": int(safe_get(r, ["opens", "opens_total"], 0) or 0),
                "unique_opens": int(safe_get(r, ["opens", "unique_opens"], 0) or 0),
                "open_rate": pd.to_numeric(safe_get(r, ["opens", "open_rate"], None), errors="coerce"),
                "clicks_total": int(safe_get(r, ["clicks", "clicks_total"], 0) or 0),
                "unique_clicks": int(safe_get(r, ["clicks", "unique_clicks"], 0) or 0),
                "click_rate": pd.to_numeric(safe_get(r, ["clicks", "click_rate"], None), errors="coerce"),
                "click_to_open_rate": pd.to_numeric(safe_get(r, ["clicks", "click_to_open_rate"], None), errors="coerce"),
                "hard_bounces": int(safe_get(r, ["bounces", "hard_bounces"], 0) or 0),
                "soft_bounces": int(safe_get(r, ["bounces", "soft_bounces"], 0) or 0),
                "unsubscribed": int(r.get("unsubscribed", 0) or 0),
                "abuse_reports": int(r.get("abuse_reports", 0) or 0),
                "forwards_count": int(safe_get(r, ["forwards", "forwards_count"], 0) or 0),
                "forwards_opens": int(safe_get(r, ["forwards", "forwards_opens"], 0) or 0),
                "send_time": c.get("send_time"),
                "created_at": datetime.utcnow().isoformat(timespec="seconds"),
                "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
            })

        fact_mailchimp_campaign_monthly = pd.DataFrame(fact_campaign_rows)
        if not fact_mailchimp_campaign_monthly.empty:
            fact_mailchimp_campaign_monthly = (
                fact_mailchimp_campaign_monthly
                .drop_duplicates(subset=["campaign_id", "month_start_date_id"])
                .reset_index(drop=True)
            )

        fact_mailchimp_campaign_monthly.to_csv(
            os.path.join(out_dir, "fact_mailchimp_campaign_monthly.csv"),
            index=False
        )

        all_member_rows: List[Dict[str, Any]] = []

        for lst in lists:
            list_id = lst.get("id")
            if not list_id:
                continue

            try:
                members = fetch_all_members_for_list(api_root, access_token, list_id)
                for member in members:
                    member["audience_id"] = list_id
                all_member_rows.extend(members)
                print(f"members pulled for {lst.get('name')}: {len(members)}")
            except Exception as e:
                print("member_pull_error:", list_id, str(e))

            time.sleep(0.12)

        dim_mailchimp_member = build_member_dimension(all_member_rows)
        dim_mailchimp_member.to_csv(
            os.path.join(out_dir, "dim_mailchimp_member.csv"),
            index=False
        )

        fact_mailchimp_audience_monthly_members_based = build_member_based_audience_monthly(
            dim_audience=dim_mailchimp_audience,
            dim_member=dim_mailchimp_member,
            start_date=audience_history_start_date,
            end_date=datetime.utcnow().date(),
        )

        fact_mailchimp_audience_monthly_members_based.to_csv(
            os.path.join(out_dir, "fact_mailchimp_audience_monthly_members_based.csv"),
            index=False
        )

        dim_date = generate_dim_date(audience_history_start_date, datetime.utcnow().date())
        dim_date.to_csv(os.path.join(out_dir, "dim_date.csv"), index=False)

        print("\n===== EXPORT END =====")
        print(os.path.abspath(out_dir))
        print("======================\n")

        return {
            "status": "ok",
            "output_dir": os.path.abspath(out_dir),
            "files": [
                "dim_date.csv",
                "dim_user.csv",
                "dim_mailchimp_audience.csv",
                "dim_mailchimp_campaign.csv",
                "dim_mailchimp_member.csv",
                "fact_mailchimp_campaign_monthly.csv",
                "fact_mailchimp_audience_monthly_members_based.csv",
            ],
            "counts": {
                "lists": int(len(lists)),
                "all_members_rows": int(len(dim_mailchimp_member)),
                "campaigns_pulled_total": int(len(all_campaigns)),
                "campaigns_after_filter": int(len(campaigns)),
                "dim_campaign_rows": int(len(dim_mailchimp_campaign)),
                "fact_campaign_monthly_rows": int(len(fact_mailchimp_campaign_monthly)),
                "fact_audience_monthly_members_based_rows": int(len(fact_mailchimp_audience_monthly_members_based)),
                "dim_date_rows": int(len(dim_date)),
            }
        }

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(tb)
        return {"status": "error", "error": str(e), "traceback": tb}


@app.route("/export_star_schema")
def export_star_schema():
    token = session.get("mailchimp_token")
    api_root = session.get("mailchimp_api_root")
    if not token or not api_root:
        return redirect(url_for("index"))

    result = run_export(token["access_token"], api_root)
    if isinstance(result, tuple):
        return result
    return result


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=True)