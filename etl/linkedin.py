import os
from flask import Flask, redirect, request, session
from requests_oauthlib import OAuth2Session
import requests

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"


app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev_secret_key_change_me")

CLIENT_ID     = os.getenv("LINKEDIN_CLIENT_ID")
CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")

AUTH_URL = "https://www.linkedin.com/oauth/v2/authorization"
TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
REDIRECT_URI = "http://localhost:8080"
SCOPES = (
    "openid profile email "
)



@app.route("/")
def home():
    if "code" in request.args:
        try:
            linkedin = OAuth2Session(
                CLIENT_ID,
                redirect_uri=REDIRECT_URI,
                scope=SCOPES,
                state=session.get("oauth_state"),
            )

            token = linkedin.fetch_token(
                TOKEN_URL,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                code=request.args.get("code"),
                include_client_id=True,
                scope=SCOPES,
            )

            headers = {"Authorization": f"Bearer {token['access_token']}"}
            profile = requests.get("https://api.linkedin.com/v2/userinfo", headers=headers).json()
            return profile

        except Exception as e:
            return {
                "error": str(e),
                "request_url": request.url,
                "saved_state": session.get("oauth_state"),
            }, 500

    return '<a href="/login">Login with LinkedIn</a>'

@app.route("/login")
def login():
    linkedin = OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI, scope=SCOPES)
    authorization_url, state = linkedin.authorization_url(AUTH_URL)
    session["oauth_state"] = state
    return redirect(authorization_url)

if __name__ == "__main__":
    print("✅ starting flask on http://localhost:8080")
    app.run(host="localhost", port=8080, debug=False, use_reloader=False)