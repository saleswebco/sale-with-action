#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# test_google_auth.py

import os
import json
import sys

REQUIRED_SCOPE = "https://www.googleapis.com/auth/spreadsheets"

def print_err(*args):
    print(*args, file=sys.stderr)

def main():
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    raw = os.environ.get("GOOGLE_CREDENTIALS")

    print("GOOGLE_CREDENTIALS_FILE set?", bool(file_env))
    print("GOOGLE_CREDENTIALS set?", bool(raw))

    info = None
    source = None

    if file_env:
        print("GOOGLE_CREDENTIALS_FILE path:", file_env)
        if not os.path.exists(file_env):
            print_err("ERROR: GOOGLE_CREDENTIALS_FILE path does not exist.")
            sys.exit(2)
        try:
            with open(file_env, "r", encoding="utf-8") as fh:
                info = json.load(fh)
            source = "file"
        except Exception as e:
            print_err("ERROR reading file JSON:", e)
            sys.exit(2)
    elif raw:
        print("GOOGLE_CREDENTIALS length:", len(raw))
        try:
            info = json.loads(raw)
            source = "env-json"
        except Exception as e:
            print_err("ERROR: GOOGLE_CREDENTIALS invalid JSON:", e)
            sys.exit(2)
    else:
        print_err("No usable credentials found in GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_FILE.")
        sys.exit(2)

    # Basic fields
    client_email = info.get("client_email")
    project_id = info.get("project_id")
    private_key_id = info.get("private_key_id")
    private_key_present = bool(info.get("private_key"))

    print("Source:", source)
    print("client_email:", client_email or "<missing>")
    print("project_id:", project_id or "<missing>")
    print("private_key_id present?", bool(private_key_id))
    print("private_key present?", private_key_present)

    # Sanity checks
    if not client_email or not private_key_present:
        print_err("ERROR: Missing client_email or private_key in credentials JSON.")
        sys.exit(2)

    # Scope note only (scope is set by your app, not in the JSON)
    print("Expected Sheets scope:", REQUIRED_SCOPE)
    print("Credentials JSON OK.")
    sys.exit(0)

if __name__ == "__main__":
    main()