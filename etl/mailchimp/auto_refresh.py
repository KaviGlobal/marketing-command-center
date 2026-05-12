"""
auto_refresh.py — Pull fresh Mailchimp data and upload to Azure SQL.

First-time setup:
  1. python mailchimp_extract_data.py
  2. Open http://127.0.0.1:8000, complete OAuth → token.json is saved automatically

After that, just run:
  python auto_refresh.py
"""

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

TOKEN_FILE = os.getenv("MAILCHIMP_TOKEN_FILE", "token.json")


def load_token() -> dict:
    # In CI: bootstrap token.json from environment variables if it doesn't exist
    if not os.path.exists(TOKEN_FILE):
        access_token = os.getenv("MAILCHIMP_ACCESS_TOKEN")
        api_root = os.getenv("MAILCHIMP_API_ROOT")
        if access_token and api_root:
            with open(TOKEN_FILE, "w") as f:
                json.dump({"access_token": access_token, "api_root": api_root}, f)
        else:
            print(f"ERROR: {TOKEN_FILE} not found and MAILCHIMP_ACCESS_TOKEN/MAILCHIMP_API_ROOT env vars are not set.")
            print("Either run the Flask app first (python mailchimp_extract_data.py) or set the env vars for CI.")
            sys.exit(1)
    with open(TOKEN_FILE) as f:
        return json.load(f)


def main():
    print("=== Mailchimp Auto Refresh ===\n")

    # Step 1: load saved OAuth token
    token_data = load_token()
    access_token = token_data["access_token"]
    api_root = token_data.get("api_root")

    if not api_root:
        from mailchimp_extract_data import get_dc_and_api_root
        _, api_root = get_dc_and_api_root(access_token)
        token_data["api_root"] = api_root
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f)

    print(f"API root: {api_root}")

    # Step 2: pull fresh data from Mailchimp
    from mailchimp_extract_data import run_export
    result = run_export(access_token, api_root)

    if result.get("status") != "ok":
        print("Export failed:", result)
        sys.exit(1)

    out_dir = Path(result["output_dir"])
    print(f"\nExport complete → {out_dir}")
    print("Counts:", result["counts"])

    # Step 3: upload the new CSVs to Azure SQL
    print("\n=== Uploading to Azure SQL ===\n")
    from mailchimp_to_azure import main as azure_main
    azure_main(csv_dir=out_dir)

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
