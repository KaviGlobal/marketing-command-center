from requests_oauthlib import OAuth2Session
import requests
import os

# ---- CONFIG ----
client_id = os.environ["FB_CLIENT_ID"]
client_secret = os.environ["FB_CLIENT_SECRET"]
redirect_uri = "http://localhost:8080/callback"

auth_base_url = "https://www.facebook.com/v20.0/dialog/oauth"
token_url = "https://graph.facebook.com/v20.0/oauth/access_token"

scopes = [
    "public_profile",
    "email",
    "pages_show_list",
    "pages_read_engagement"
]

# ---- STEP 1: AUTHORIZE ----
fb = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)
authorization_url, state = fb.authorization_url(auth_base_url)

print("\nOpen this URL in your browser:\n")
print(authorization_url)

# ---- STEP 2: PASTE REDIRECT URL ----
redirect_response = input("\nPaste the full redirect URL here:\n")

token = fb.fetch_token(
    token_url,
    client_secret=client_secret,
    authorization_response=redirect_response
)

print("\nShort-lived User Token:\n")
print(token)
