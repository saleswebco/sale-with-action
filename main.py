#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orchestrator: runs scraper and writes to Google Sheets (per-county tabs + Dashboard)
"""

import os
import sys
import asyncio
from datetime import datetime
from scraper import ForeclosureScraper
from highlighting import SheetsClient, init_sheets_service_from_env

TARGET_COUNTIES = [
    {"county_id": "52", "county_name": "Cape May County, NJ"},
    {"county_id": "25", "county_name": "Atlantic County, NJ"},
    {"county_id": "1", "county_name": "Camden County, NJ"},
    {"county_id": "3", "county_name": "Burlington County, NJ"},
    {"county_id": "6", "county_name": "Cumberland County, NJ"},
    {"county_id": "19", "county_name": "Gloucester County, NJ"},
    {"county_id": "20", "county_name": "Salem County, NJ"},
    {"county_id": "15", "county_name": "Union County, NJ"},
    {"county_id": "7", "county_name": "Bergen County, NJ"},
    {"county_id": "2", "county_name": "Essex County, NJ"},
    {"county_id": "23", "county_name": "Montgomery County, PA"},
    {"county_id": "24", "county_name": "New Castle County, DE"},
]

POLITE_DELAY_SECONDS = 1.5

async def run():
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        sys.exit("Missing SPREADSHEET_ID env variable.")

    service = init_sheets_service_from_env()
    sheets = SheetsClient(spreadsheet_id, service)
    dashboard_rows = []

    scraper = ForeclosureScraper(headless=True)

    # Use the scraper's async context manager
    async with scraper.launch_browser() as page:
        for c in TARGET_COUNTIES:
            county_name = c["county_name"]
            print(f"→ Processing {county_name}")
            tab = county_name[:100]
            sheets.create_sheet_if_missing(tab)

            # Get previous keys BEFORE writing/clearing
            prev_keys = sheets.get_previous_keys(tab)

            # Scrape — scraper already filters to the next 30 days
            recs = await scraper.scrape_county_sales(page, c)

            if not recs:
                print(f"   No next-30-day records for {county_name}.")
                dashboard_rows.append([county_name, 0, 0])
                await asyncio.sleep(POLITE_DELAY_SECONDS)
                continue

            headers = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]
            total, new = sheets.write_snapshot(tab, headers, recs, prev_keys)

            print(f"   {county_name}: {total} active ({new} new)")
            dashboard_rows.append([county_name, total, new])
            await asyncio.sleep(POLITE_DELAY_SECONDS)

    # Write dashboard
    if dashboard_rows:
        sheets.create_sheet_if_missing("Dashboard")
        today = datetime.now().strftime("%Y-%m-%d")
        values = [["Dashboard - " + today], ["County", "Active (30d)", "New Today"]] + dashboard_rows
        sheets.clear("Dashboard")
        sheets.write_values("Dashboard", values)

    print("Done.")

if __name__ == "__main__":
    asyncio.run(run())
