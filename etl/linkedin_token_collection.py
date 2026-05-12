"""
linkedin_token_collection.py
────────────────────────────
ONE-TIME script (re-run when your refresh token expires, ~1 year).

What it does
────────────
  1. Prints an authorization URL — open it in your browser and log in
     as the account that manages the Kavi LinkedIn pages.
  2. After LinkedIn redirects you to kaviglobal.com, paste the full
     redirect URL back here.
  3. Exchanges the code for an access token + refresh token.
  4. Discovers which LinkedIn organizations your account administers.
  5. Prints a ready-to-paste .env block.

Required .env vars (before running)
─────────────────────────────────────
  LINKEDIN_CLIENT_ID     = from your app's Auth tab
  LINKEDIN_CLIENT_SECRET = from your app's Auth tab

After running, add these to your .env
──────────────────────────────────────
  LINKEDIN_ACCESS_TOKEN   (valid ~60 days)
  LINKEDIN_REFRESH_TOKEN  (valid ~1 year — use this to auto-renew)
  LINKEDIN_ORG_ID_KAVI_GLOBAL       (e.g. 12345678)
  LINKEDIN_ORG_ID_KAVI_PHILIPPINES  (if found)
"""

import os
import sys
import urllib.parse
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("LINKEDIN_CLIENT_ID")
CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")
REDIRECT_URI  = "https://www.kaviglobal.com"

AUTH_URL  = "https://www.linkedin.com/oauth/v2/authorization"
TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"

# Scopes needed for page posts, engagement, follower stats, page analytics
# offline_access gives you a refresh token so you don't need to repeat this flow
SCOPES = [
    "openid",
    "profile",
    "r_organization_social",
]

if not all([CLIENT_ID, CLIENT_SECRET]):
    sys.exit(
        "❌  Missing env vars. Make sure .env contains:\n"
        "    LINKEDIN_CLIENT_ID, LINKEDIN_CLIENT_SECRET"
    )


# ── STEP 1: Generate authorization URL ────────────────────────────────────────

import secrets
state = secrets.token_urlsafe(16)

params = {
    "response_type": "code",
    "client_id":     CLIENT_ID,
    "redirect_uri":  REDIRECT_URI,
    "scope":         " ".join(SCOPES),
    "state":         state,
}

auth_url = AUTH_URL + "?" + urllib.parse.urlencode(params)

print("\n── Step 1: Open this URL in your browser (log in as the Kavi page admin)")
print("─" * 70)
print(auth_url)
print("─" * 70)
print("\nAfter approving, you'll be redirected to kaviglobal.com.")
print("The page may not load — that's fine. Copy the full URL from the address bar.\n")


# ── STEP 2: Paste redirect URL → exchange code for tokens ─────────────────────

redirected_url = input("Paste the full redirect URL here:\n> ").strip()

parsed = urllib.parse.urlparse(redirected_url)
qs     = urllib.parse.parse_qs(parsed.query)

if "error" in qs:
    sys.exit(f"❌  LinkedIn returned an error: {qs.get('error_description', qs['error'])}")

if "code" not in qs:
    sys.exit("❌  No 'code' found in the URL. Make sure you pasted the full redirect URL.")

code           = qs["code"][0]
returned_state = qs.get("state", [""])[0]

if returned_state != state:
    print(f"⚠️  State mismatch (expected {state}, got {returned_state}). Proceeding anyway.")

print("\n── Step 2: Exchanging code for tokens …")

resp = requests.post(
    TOKEN_URL,
    data={
        "grant_type":    "authorization_code",
        "code":          code,
        "redirect_uri":  REDIRECT_URI,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    },
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    timeout=30,
)

if not resp.ok:
    print(f"❌  HTTP {resp.status_code}: {resp.text}")
    sys.exit(1)

token_data    = resp.json()
access_token  = token_data.get("access_token")
refresh_token = token_data.get("refresh_token")  # present if offline_access was granted
expires_in    = token_data.get("expires_in", "unknown")

if not access_token:
    sys.exit(f"❌  No access_token in response: {token_data}")

print(f"✅  Access token received (expires_in: {expires_in}s ≈ 60 days)")
if refresh_token:
    print("✅  Refresh token received (valid ~1 year)")
else:
    print("⚠️  No refresh token returned — offline_access scope may not be approved.")
    print("    You'll need to re-run this script every 60 days.")


# ── STEP 3: Discover org IDs ───────────────────────────────────────────────────

print("\n── Step 3: Discovering LinkedIn Organizations you administer …")

headers = {
    "Authorization":   f"Bearer {access_token}",
    "LinkedIn-Version": "202401",
    "X-Restli-Protocol-Version": "2.0.0",
}

# Method 1: organizationAcls (v2) — lists orgs where user has admin role
resp = requests.get(
    "https://api.linkedin.com/v2/organizationAcls",
    params={
        "q":              "roleAssignee",
        "role":           "ADMINISTRATOR",
        "state":          "APPROVED",
        "count":          50,
        "projection":     "(elements*(organization~(id,localizedName,vanityName),roleAssignee~(localizedFirstName)))",
    },
    headers=headers,
    timeout=30,
)

orgs = []
if resp.ok:
    for elem in resp.json().get("elements", []):
        org_info = elem.get("organization~", {})
        org_urn  = elem.get("organization", "")
        org_id   = org_urn.split(":")[-1] if org_urn else ""
        name     = org_info.get("localizedName", "Unknown")
        vanity   = org_info.get("vanityName", "")
        if org_id:
            orgs.append({"id": org_id, "name": name, "vanity": vanity})
            print(f"  ✅  {name}  (ID: {org_id}  vanity: {vanity})")
else:
    print(f"  ⚠️  Could not fetch orgs: {resp.status_code} {resp.text}")
    print("      You may need to add org IDs to .env manually.")

if not orgs:
    print("  No organizations found. Either:")
    print("  - You are not an Administrator of any LinkedIn pages")
    print("  - r_organization_social scope was not approved")


# ── Summary / paste-ready .env block ──────────────────────────────────────────

print("\n" + "─" * 70)
print("Add the following to your .env file:\n")
print(f"LINKEDIN_ACCESS_TOKEN={access_token}")
if refresh_token:
    print(f"LINKEDIN_REFRESH_TOKEN={refresh_token}")
for org in orgs:
    safe = org["name"].upper().replace(" ", "_").replace("-", "_")
    print(f"LINKEDIN_ORG_ID_{safe}={org['id']}")
print("─" * 70 + "\n")
