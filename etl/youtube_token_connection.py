import secrets, urllib.parse, webbrowser

CLIENT_ID = "117013611008-d2ofmlhgk5422h8o52ig9a5bgg95lq9m.apps.googleusercontent.com"
REDIRECT_URI = "https://www.kaviglobal.com"   

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
]
 
STATE = secrets.token_urlsafe(16)

params = {
    "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI,
    "response_type": "code",
    "scope": " ".join(SCOPES),
    "access_type": "offline",
    "prompt": "consent",
    "include_granted_scopes": "true",
    "state": STATE,
}

auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
print("Open this URL:\n", auth_url)
print("\nSTATE to expect back:", STATE)

webbrowser.open(auth_url)
