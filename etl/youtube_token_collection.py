import os
import urllib.parse
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("YOUTUBE_CLIENT_ID")
CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
REDIRECT_URI  = "https://www.kaviglobal.com"

redirected_url = input("Paste the full redirected URL here: ").strip()

parsed = urllib.parse.urlparse(redirected_url)
params = urllib.parse.parse_qs(parsed.query)

code = params["code"][0]
state_returned = params.get("state", [""])[0]

print("Code (first 15):", code[:15], "...")
print("Returned state:", state_returned)

resp = requests.post(
    "https://oauth2.googleapis.com/token",
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI,
    },
)

print("Status:", resp.status_code)
print("Body:", resp.text)
print(resp.json())
