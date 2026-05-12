"""
facebook_token_exchange.py
──────────────────────────
ONE-TIME script.  Run this whenever you need to refresh your long-lived tokens
(every ~60 days) or when you first set up the integration.

What it does
────────────
  1. Takes the short-lived user access token you copy from Graph API Explorer
     and exchanges it for a 60-day long-lived user token.
  2. Uses that long-lived user token to fetch Page Access Tokens for each
     known page ID directly (no pages_show_list permission needed).
  3. Prints everything clearly so you can paste the values into your .env file.

Required .env vars (before running)
─────────────────────────────────────
  FB_APP_ID            = your Facebook App ID
  FB_APP_SECRET        = your Facebook App Secret
  FB_SHORT_LIVED_TOKEN = paste the short-lived token from Graph API Explorer

  Graph API Explorer permissions needed (nothing else):
    public_profile
    pages_read_engagement

After running, add these to your .env
──────────────────────────────────────
  FB_USER_TOKEN_LONG              (printed below — valid 60 days)
  FB_PAGE_TOKEN_KAVI_PHILIPPINES  (printed below — does NOT expire)
  FB_PAGE_TOKEN_KAVI_GLOBAL       (printed below — does NOT expire)
"""

import os
import sys
import hmac
import hashlib
import requests
from dotenv import load_dotenv

load_dotenv()

APP_ID      = os.getenv("FB_APP_ID")
APP_SECRET  = os.getenv("FB_APP_SECRET")
SHORT_TOKEN = os.getenv("FB_SHORT_LIVED_TOKEN")

if not all([APP_ID, APP_SECRET, SHORT_TOKEN]):
    sys.exit(
        "❌  Missing env vars. Make sure .env contains:\n"
        "    FB_APP_ID, FB_APP_SECRET, FB_SHORT_LIVED_TOKEN"
    )

GRAPH = "https://graph.facebook.com/v24.0"

def appsecret_proof(token: str) -> str:
    """HMAC-SHA256 of the access token, required when 'Require App Secret' is on."""
    return hmac.new(APP_SECRET.encode(), token.encode(), hashlib.sha256).hexdigest()

# Page IDs are already known — no pages_show_list permission needed
KNOWN_PAGES = {
    "KAVI_PHILIPPINES": "805351729333857",
    "KAVI_GLOBAL":      "119449644752808",
}


# ── STEP 1: Exchange short-lived → 60-day long-lived user token ───────────────

print("\n── Step 1: Exchanging short-lived token for long-lived user token …")

resp = requests.get(
    f"{GRAPH}/oauth/access_token",
    params={
        "grant_type":        "fb_exchange_token",
        "client_id":         APP_ID,
        "client_secret":     APP_SECRET,
        "fb_exchange_token": SHORT_TOKEN,
    },
    timeout=30,
)

if not resp.ok:
    print(f"\n❌  HTTP {resp.status_code} from Facebook:")
    print(resp.text)
    print("\n── Most common causes ──────────────────────────────────────────")
    print("  1. FB_SHORT_LIVED_TOKEN is expired  (they last ~1-2 hours)")
    print("     → Get a fresh token from Graph API Explorer:")
    print("       https://developers.facebook.com/tools/explorer/")
    print("       Select your app, keep only public_profile + pages_read_engagement,")
    print("       click 'Generate Access Token', paste the token into .env")
    print("  2. FB_APP_ID or FB_APP_SECRET is wrong")
    print("  3. Token was already long-lived (can't re-exchange)")
    sys.exit(1)

data = resp.json()
if "access_token" not in data:
    sys.exit(f"❌  Exchange failed: {data}")

long_user_token = data["access_token"]
expires_in      = data.get("expires_in", "unknown")
print(f"✅  Long-lived user token received  (expires_in: {expires_in} seconds ≈ 60 days)\n")


# ── STEP 2: Get Page Access Tokens via /me/accounts ───────────────────────────

print("── Step 2: Fetching Page Access Tokens via /me/accounts …")

resp = requests.get(
    f"{GRAPH}/me/accounts",
    params={
        "fields":          "id,name,access_token",
        "access_token":    long_user_token,
        "appsecret_proof": appsecret_proof(long_user_token),
    },
    timeout=30,
)

if not resp.ok:
    print(f"\n❌  HTTP {resp.status_code}: {resp.text}")
    print("\n── Checklist ───────────────────────────────────────────────────")
    print("  • In Graph Explorer, make sure BOTH of these are in your permissions")
    print("    before clicking Generate Access Token:")
    print("    - pages_show_list")
    print("    - pages_read_engagement")
    print("  • You must be an Admin (not just Editor) of the Kavi pages")
    sys.exit(1)

all_pages = resp.json().get("data", [])
if not all_pages:
    print("⚠️  /me/accounts returned no pages.")
    print("    Make sure pages_show_list was selected when you generated the token.")
    sys.exit(1)

# Match returned pages against our known page IDs
page_tokens = {}
for page in all_pages:
    for name, page_id in KNOWN_PAGES.items():
        if page["id"] == page_id:
            page_tokens[name] = page["access_token"]
            print(f"  ✅  {page['name']}  (ID: {page['id']})")

# Also print any other pages found (handy for discovery)
known_ids = set(KNOWN_PAGES.values())
extras = [p for p in all_pages if p["id"] not in known_ids]
if extras:
    print(f"\n  ℹ️  {len(extras)} other page(s) found (not included in ETL):")
    for p in extras:
        print(f"      {p['name']}  ID: {p['id']}")

if not page_tokens:
    print("\n❌  Neither Kavi page was found in /me/accounts.")
    print("    Make sure you are an Admin of both pages.")
    sys.exit(1)


# ── Summary / paste-ready .env block ──────────────────────────────────────────

print("\n" + "─" * 70)
print("Paste the following into your .env file:\n")
print(f"FB_USER_TOKEN_LONG={long_user_token}")
for name, token in page_tokens.items():
    print(f"FB_PAGE_TOKEN_{name}={token}")
    print(f"FB_PAGE_ID_{name}={KNOWN_PAGES[name]}")
print("─" * 70)
print("\n⚠️  Page tokens derived from a long-lived user token do NOT expire.")
print("   Store them securely and never commit them to version control.\n")
