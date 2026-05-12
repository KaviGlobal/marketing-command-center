import os
import requests
from dotenv import load_dotenv

load_dotenv()  # loads variables from .env into environment

CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("YOUTUBE_REFRESH_TOKEN")

def refresh_access_token(client_id, client_secret, refresh_token):
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

access_token = refresh_access_token(
    CLIENT_ID,
    CLIENT_SECRET,
    REFRESH_TOKEN
)

print("Access token retrieved.")
