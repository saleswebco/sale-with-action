#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Foreclosure Sales Scraper with:
- Daily snapshots
- 30-day rolling filter on county tabs
- New record highlighting (green)
- Robust unique key check (Property ID OR Address+Defendant)
- Google Sheets dashboards (county, all data, dashboard summary)
"""

import os
import re
import sys
import json
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, parse_qs

from google.oauth2 import service_account
from googleapiclient.discovery import build
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# -----------------------------
# Config
# -----------------------------
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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
MAX_RETRIES = 5

# -----------------------------
# Credential helpers
# -----------------------------
def load_service_account_info():
    """Load Google service account credentials from a file or environment variable."""
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if file_env and os.path.exists(file_env):
        with open(file_env, "r", encoding="utf-8") as fh:
            return json.load(fh)
    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("Missing GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_FILE")

    creds_raw = creds_raw.strip()
    if creds_raw.startswith("{"):
        return json.loads(creds_raw)
    if os.path.exists(creds_raw):
        with open(creds_raw, "r", encoding="utf-8") as fh:
            return json.load(fh)
    raise ValueError("Invalid GOOGLE_CREDENTIALS")

def init_sheets_service_from_env():
    """Initializes the Google Sheets API service."""
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

# -----------------------------
# Date utilities
# -----------------------------
def parse_sale_date(date_str):
    """Parses a sale date string into a datetime object using multiple formats."""
    if not date_str:
        return None
    date_str = date_str.strip()
    formats = [
        "%m/%d/%Y %I:%M %p", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def is_within_30_days(date_str, today=None):
    """
    Checks if a given date string falls within the next 30 days.
    The window is inclusive of today's date.
    """
    today = today or datetime.now()
    sale_date = parse_sale_date(date_str)
    if not sale_date:
        return False
    
    # Check if the sale date is between today and 30 days from now (inclusive)
    # The time part is ignored for the comparison
    window_end = today + timedelta(days=29) # This gives a full 30-day window
    return today.date() <= sale_date.date() <= window_end.date()

# -----------------------------
# Google Sheets Client
# -----------------------------
class SheetsClient:
    def __init__(self, spreadsheet_id, service):
        self.spreadsheet_id = spreadsheet_id
        self.svc = service.spreadsheets()

    def sheet_exists(self, name):
        """Checks if a sheet with the given name exists."""
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        return any(s["properties"]["title"] == name for s in info.get("sheets", []))

    def create_sheet_if_missing(self, name):
        """Creates a sheet if it doesn't already exist."""
        if not self.sheet_exists(name):
            req = {"addSheet": {"properties": {"title": name}}}
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests":[req]}).execute()

    def get_values(self, sheet, rng="A:Z"):
        """Retrieves values from a sheet."""
        try:
            res = self.svc.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet}'!{rng}"
            ).execute()
            return res.get("values", [])
        except Exception as e:
            print(f"Error getting values from sheet {sheet}: {e}")
            return []

    def clear(self, sheet, rng="A:Z"):
        """Clears all values in a sheet's range."""
        try:
            self.svc.values().clear(
                spreadsheetId=self.spreadsheet_id, range=f"'{sheet}'!{rng}"
            ).execute()
        except Exception as e:
            print(f"Error clearing sheet {sheet}: {e}")

    def write_values(self, sheet, values):
        """Writes values to a sheet starting from A1."""
        try:
            self.svc.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()
        except Exception as e:
            print(f"Error writing to sheet {sheet}: {e}")

    def get_previous_keys(self, sheet_name):
        """
        Retrieves a set of unique keys from the existing data on the sheet.
        This is crucial for identifying new records.
        """
        vals = self.get_values(sheet_name)
        if not vals or len(vals) < 2: # Need at least a header and one row
            return set()
            
        keys = set()
        # Find the header row (row index 1 in the sheet, which is list index 1)
        header_row_index = -1
        for i, row in enumerate(vals):
            if row and row[0].lower().strip() == "property id":
                header_row_index = i
                break
        
        if header_row_index == -1:
            return set() # Header not found, no previous data
            
        # Iterate over the data rows (starting after the header)
        for row in vals[header_row_index + 1:]:
            if len(row) > 2:
                # Use Property ID as key if available, otherwise use Address + Defendant
                key = row[0].strip() or (row[1].strip() + "|" + row[2].strip())
                keys.add(key)
        return keys

    def write_snapshot(self, sheet_name, headers, rows, prev_keys, filter_dates):
        """Writes a new data snapshot and applies formatting."""
        today = datetime.now()
        filtered_rows = []
        new_keys = set()

        for r in rows:
            is_valid_date = True
            if filter_dates:
                date_idx = -1
                for i, c in enumerate(headers):
                    if c.lower() in {"sales date", "sale date"}:
                        date_idx = i
                        break
                if date_idx != -1 and len(r) > date_idx:
                    is_valid_date = is_within_30_days(r[date_idx], today)
                else:
                    is_valid_date = False # No date column or malformed row
            
            if is_valid_date:
                filtered_rows.append(r)
                key = r[0].strip() or (r[1].strip() + "|" + r[2].strip())
                if key not in prev_keys:
                    new_keys.add(key)

        snapshot_header = [[f"Snapshot for {today.strftime('%A - %Y-%m-%d')}"]]
        values = snapshot_header + [headers] + filtered_rows + [[""]]
        
        self.clear(sheet_name)
        self.write_values(sheet_name, values)
        self.apply_formatting(sheet_name, headers, filtered_rows, new_keys)
        return len(filtered_rows), len(new_keys)

    def apply_formatting(self, sheet_name, headers, rows, new_keys):
        """Applies formatting to the sheet, including row highlighting."""
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        sheet_id = None
        for s in info["sheets"]:
            if s["properties"]["title"] == sheet_name:
                sheet_id = s["properties"]["sheetId"]
        if sheet_id is None:
            return

        num_cols = len(headers)
        requests = [
            # Make header bold
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold"
                }
            },
            # Freeze header row
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
                    "fields": "gridProperties.frozenRowCount"
                }
            },
            # Auto-resize columns
            {
                "autoResizeDimensions": {
                    "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": num_cols}
                }
            }
        ]
        
        # Apply green highlight to new rows
        for idx, row in enumerate(rows):
            key = row[0].strip() or (row[1].strip() + "|" + row[2].strip())
            if key in new_keys:
                sheet_row = idx + 2
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": sheet_row, "endRowIndex": sheet_row + 1,
                                 "startColumnIndex": 0, "endColumnIndex": num_cols},
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

        if requests:
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()

# -----------------------------
# Scraper Logic
# -----------------------------
def extract_property_id_from_href(href):
    """Extracts the PropertyId from a URL's query parameters."""
    try:
        q = parse_qs(urlparse(href).query)
        return q.get("PropertyId", [""])[0]
    except:
        return ""

class ForeclosureScraper:
    def __init__(self, sheets_client):
        self.sheets = sheets_client

    async def goto_with_retry(self, page, url):
        """Tries to navigate to a URL with retries and a delay."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = await page.goto(url, wait_until="networkidle", timeout=60000)
                if resp and resp.status == 200:
                    return resp
            except PlaywrightTimeoutError:
                print(f"Attempt {attempt+1}/{MAX_RETRIES}: Timeout navigating to {url}")
            except Exception as e:
                print(f"Attempt {attempt+1}/{MAX_RETRIES}: Error navigating to {url}: {e}")
            await asyncio.sleep(2**attempt)
        print(f"Failed to navigate to {url} after {MAX_RETRIES} attempts.")
        return None

    async def scrape_county_sales(self, page, county):
        """Scrapes sales data for a specific county."""
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        await self.goto_with_retry(page, url)
        try:
            # Wait for either the data table or a "no sales" message
            await page.wait_for_selector("table.table tbody tr, .no-sales, #noData", timeout=30000)
        except PlaywrightTimeoutError:
            print(f"Timeout waiting for data on {county['county_name']} page.")
            return []

        # Check for no data messages
        no_sales_text = await page.locator(".no-sales, #noData").all_text_contents()
        if no_sales_text:
            print(f"No sales found for {county['county_name']}.")
            return []

        colmap = await self.get_columns(page)
        rows = page.locator("table.table tbody tr")
        n = await rows.count()
        results = []
        for i in range(n):
            r = rows.nth(i)
            # Find the link to extract the Property ID
            a = r.locator("td.hidden-print a")
            href = (await a.get_attribute("href")) or ""
            pid = extract_property_id_from_href(href)
            
            cells = await r.locator("td").all()
            data = {
                "Property ID": pid,
                "Address": "",
                "Defendant": "",
                "Sales Date": "",
                "Approx Judgment": "",
                "County": county["county_name"]
            }
            
            # Extract data based on the column mapping
            if "sales_date" in colmap:
                data["Sales Date"] = await cells[colmap["sales_date"]].inner_text()
            if "defendant" in colmap:
                data["Defendant"] = await cells[colmap["defendant"]].inner_text()
            if "address" in colmap:
                data["Address"] = await cells[colmap["address"]].inner_text()
            
            # Append the data as a list of values
            results.append([
                data.get("Property ID", ""),
                data.get("Address", ""),
                data.get("Defendant", ""),
                data.get("Sales Date", ""),
                data.get("Approx Judgment", "")
            ])
        return results

    async def get_columns(self, page):
        """Determines the column index for each data field."""
        cols = {}
        ths = page.locator("table.table thead th")
        n = await ths.count()
        for i in range(n):
            t = (await ths.nth(i).inner_text()).lower()
            if "sale" in t and "date" in t:
                cols["sales_date"] = i
            elif "defendant" in t:
                cols["defendant"] = i
            elif "address" in t:
                cols["address"] = i
            elif "judgment" in t:
                cols["approx_judgment"] = i
        return cols

# -----------------------------
# Orchestrator
# -----------------------------
async def run():
    """Main function to run the scraping and sheet update process."""
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        sys.exit("Missing SPREADSHEET_ID environment variable.")
    
    try:
        service = init_sheets_service_from_env()
    except Exception as e:
        sys.exit(f"Failed to initialize Google Sheets service: {e}")
    
    sheets = SheetsClient(spreadsheet_id, service)
    dashboard_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        scraper = ForeclosureScraper(sheets)

        for c in TARGET_COUNTIES:
            print(f"Processing {c['county_name']}...")
            tab = c["county_name"][:30] # Truncate tab name for Sheets limit
            sheets.create_sheet_if_missing(tab)
            
            # --- FIX: Get previous keys BEFORE scraping and clearing ---
            prev_keys = sheets.get_previous_keys(tab)
            recs = await scraper.scrape_county_sales(page, c)
            
            if not recs:
                print(f"No records found for {c['county_name']}. Skipping.")
                continue

            headers = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]
            total, new = sheets.write_snapshot(tab, headers, recs, prev_keys, filter_dates=True)
            print(f"--> Found {total} records ({new} new) for {c['county_name']}")
            dashboard_data.append([c["county_name"], total, new])
            
            await asyncio.sleep(POLITE_DELAY_SECONDS)
        
        await browser.close()

    # Update Dashboard sheet
    if dashboard_data:
        sheets.create_sheet_if_missing("Dashboard")
        today = datetime.now().strftime("%Y-%m-%d")
        values = [["Dashboard - " + today], ["County", "Active (30d)", "New Today"]] + dashboard_data
        sheets.clear("Dashboard")
        sheets.write_values("Dashboard", values)
        
    print("Scraping and Sheets update complete.")

if __name__ == "__main__":
    asyncio.run(run())