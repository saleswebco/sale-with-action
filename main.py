#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main Orchestrator for Foreclosure Scraper
- Uses scraper.py for scraping
- Uses highlighting.py for sheets formatting + 30-day window filtering
- Writes results into Google Sheets
"""

import os
import sys
import asyncio
import pandas as pd
from rambow import Color

from google.oauth2 import service_account
from googleapiclient.discovery import build

from scraper import ForeclosureScraper, TARGET_COUNTIES
from highlighting import SheetsHelper, is_within_30_days, today_str

# -----------------------------
# Config
# -----------------------------
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
BASE_URL = "https://salesweb.civilview.com/"

def init_sheets_client():
    """Initialize Sheets API service."""
    creds_str = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_str:
        sys.exit(Color.red("Missing GOOGLE_CREDENTIALS env variable."))

    creds_dict = eval(creds_str) if isinstance(creds_str, str) else creds_str
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)

# -----------------------------
# Main Runner
# -----------------------------
async def run():
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        sys.exit(Color.red("Missing SPREADSHEET_ID environment variable."))

    service = init_sheets_client()
    sheets = SheetsHelper(spreadsheet_id, service)

    scraper = ForeclosureScraper(BASE_URL)
    dashboard_rows = []

    async with scraper.launch_browser() as page:
        for c in TARGET_COUNTIES:
            cname = c["county_name"]
            print(Color.cyan(f"⚡ Processing {cname}..."))

            tab = cname[:30]  # Sheets tab name limit
            sheets.ensure_sheet(tab)

            prev_keys = sheets.get_existing_keys(tab)
            records = await scraper.scrape_county_sales(page, c)

            if not records:
                print(Color.yellow(f"→ No records for {cname}"))
                continue

            # Use pandas for processing
            df = pd.DataFrame(records, columns=[
                "Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"
            ])

            # Filter 30-day window
            df = df[df["Sales Date"].apply(lambda x: is_within_30_days(x))]

            total_rows, new_count = sheets.write_snapshot(
                tab, df, prev_keys
            )

            print(Color.green(f"✓ {cname}: {total_rows} active ({new_count} new)"))
            dashboard_rows.append([cname, total_rows, new_count])

    # Write Dashboard
    if dashboard_rows:
        sheets.ensure_sheet("Dashboard")
        dashboard_df = pd.DataFrame(dashboard_rows, columns=[
            "County", "Active (30d)", "New Today"
        ])
        sheets.write_dashboard(dashboard_df)

    print(Color.magenta("🎉 Scraping + Sheets update complete."))

if __name__ == "__main__":
    asyncio.run(run())