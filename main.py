#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Foreclosure Sales Scraper:
- Daily snapshots
- 30-Day Filter on county tabs
- New record highlighting (green)
- Robust detection (Property ID OR Address+Defendant)
- Google Sheets output (county tabs, All Data, Dashboard summary)
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
from googleapiclient.errors import HttpError

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# -----------------------------
# Config
# -----------------------------
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

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
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)

# -----------------------------
# Date handling
# -----------------------------
def parse_sale_date(date_str):
    if not date_str:
        return None
    date_str = date_str.strip()
    formats = [
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def is_within_30_days(date_str, reference=None):
    ref = reference or datetime.now()
    sale_date = parse_sale_date(date_str)
    if not sale_date:
        return False
    end_date = ref + timedelta(days=29)
    return ref.date() <= sale_date.date() <= end_date.date()

# -----------------------------
# Sheets Client
# -----------------------------
class SheetsClient:
    def __init__(self, spreadsheet_id, service):
        self.spreadsheet_id = spreadsheet_id
        self.svc = service.spreadsheets()

    def spreadsheet_info(self):
        return self.svc.get(spreadsheetId=self.spreadsheet_id).execute()

    def sheet_exists(self, name):
        info = self.spreadsheet_info()
        return any(s['properties']['title'] == name for s in info.get('sheets', []))

    def create_sheet_if_missing(self, name):
        if not self.sheet_exists(name):
            req = {"addSheet": {"properties": {"title": name}}}
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id, body={"requests": [req]}
            ).execute()
            print(f"✓ Created sheet: {name}")

    def get_values(self, sheet, rng="A:Z"):
        res = self.svc.values().get(
            spreadsheetId=self.spreadsheet_id, range=f"'{sheet}'!{rng}"
        ).execute()
        return res.get("values", [])

    def clear(self, sheet, rng="A:Z"):
        self.svc.values().clear(
            spreadsheetId=self.spreadsheet_id, range=f"'{sheet}'!{rng}"
        ).execute()

    def write_values(self, sheet, values, start="A1"):
        self.svc.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet}'!{start}",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()

    def _get_sheet_id(self, sheet):
        info = self.spreadsheet_info()
        for s in info.get("sheets", []):
            if s["properties"]["title"] == sheet:
                return s["properties"]["sheetId"]
        return None

    def extract_previous_keys(self, sheet_name):
        vals = self.get_values(sheet_name, "A:Z")
        if not vals:
            return set()
        prev_keys = set()
        snap_count = 0
        collecting = False
        for row in vals:
            if not row:
                continue
            if row[0].startswith("Snapshot for"):
                snap_count += 1
                collecting = snap_count == 2
                continue
            if collecting and row[0].lower().startswith("property id"):
                continue
            if collecting and row[0].strip():
                key = row[0].strip() or (row[1].strip() + "|" + row[2].strip())
                prev_keys.add(key)
        return prev_keys

    def write_snapshot(self, sheet_name, header, rows, filter_30=False):
        now = datetime.now()
        snap_header = [[f"Snapshot for {now.strftime('%A - %Y-%m-%d')}"]]
        prev_keys = self.extract_previous_keys(sheet_name)

        filtered = []
        new_keys = set()
        if filter_30:
            date_idx = None
            for idx, col in enumerate(header):
                if col.lower() in {"sales date", "sale date"}:
                    date_idx = idx
            for r in rows:
                if date_idx is None or is_within_30_days(r[date_idx], now):
                    filtered.append(r)
        else:
            filtered = rows

        for r in filtered:
            key = r[0].strip() or (r[1].strip() + "|" + r[2].strip())
            if key not in prev_keys:
                new_keys.add(key)

        values = snap_header + [header] + filtered + [[""]]
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values)

        self.apply_formatting(sheet_name, header, filtered, new_keys)
        return len(filtered), len(new_keys)

    def apply_formatting(self, sheet_name, header_row, rows, new_keys):
        sheet_id = self._get_sheet_id(sheet_name)
        if sheet_id is None:
            return
        num_cols = len(header_row)
        requests = [
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": num_cols,
                    }
                }
            },
        ]
        for idx, row in enumerate(rows):
            key = row[0].strip() or (row[1].strip() + "|" + row[2].strip())
            if key in new_keys:
                sheet_row = idx + 2
                requests.append(
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": sheet_row,
                                "endRowIndex": sheet_row + 1,
                                "startColumnIndex": 0,
                                "endColumnIndex": num_cols,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    }
                )
        if requests:
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id, body={"requests": requests}
            ).execute()

# -----------------------------
# Scraper
# -----------------------------
def extract_property_id_from_href(href: str) -> str:
    try:
        q = parse_qs(urlparse(href).query)
        return q.get("PropertyId", [""])[0]
    except Exception:
        return ""

class ForeclosureScraper:
    def __init__(self, sheets_client): self.sheets_client = sheets_client

    async def goto_with_retry(self, page, url: str, max_retries=3):
        last_exc = None
        for attempt in range(max_retries):
            try:
                resp = await page.goto(url, wait_until="networkidle", timeout=60000)
                if resp and (200 <= resp.status < 300):
                    return resp
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                last_exc = e
                await asyncio.sleep(2 ** attempt)
        if last_exc: raise last_exc
        return None

    async def dismiss_banners(self, page):
        selectors = ["button:has-text('Accept')","button:has-text('I Agree')",
                     "button:has-text('Close')","button.cookie-accept",
                     "button[aria-label='Close']", ".modal-footer button:has-text('OK')"]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if await loc.count():
                    await loc.first.click(timeout=1500)
                    await page.wait_for_timeout(200)
            except Exception: pass

    async def get_details_data(self, page, details_url, list_url, county, current_data):
        extracted = {"approx_judgment":"","sale_type":"",
                     "address":current_data.get("address",""),
                     "defendant":current_data.get("defendant",""),
                     "sales_date":current_data.get("sales_date","")}
        if not details_url: return extracted
        try:
            await self.goto_with_retry(page, details_url)
            await self.dismiss_banners(page)
            await page.wait_for_selector(".sale-details-list", timeout=15000)
            items = page.locator(".sale-details-list .sale-detail-item")
            for j in range(await items.count()):
                try:
                    label = (await items.nth(j).locator(".sale-detail-label").inner_text()).strip()
                    val = (await items.nth(j).locator(".sale-detail-value").inner_text()).strip()
                    l = label.lower()
                    if "address" in l:
                        try:
                            val_html = await items.nth(j).locator(".sale-detail-value").inner_html()
                            val_html = re.sub(r"<br\s*/?>"," ", val_html)
                            val_clean = re.sub(r"<.*?>","", val_html).strip()
                            if not extracted["address"] or len(val_clean)>len(extracted["address"]):
                                extracted["address"]=val_clean
                        except: 
                            if not extracted["address"]: extracted["address"]=val
                    elif "approx" in l or "debt" in l: extracted["approx_judgment"]=val
                    elif "defendant" in l and not extracted["defendant"]: extracted["defendant"]=val
                    elif "sale" in l and "date" in l and not extracted["sales_date"]:
                        extracted["sales_date"]=val
                    elif county["county_id"]=="24" and "sale type" in l:
                        extracted["sale_type"]=val
                except: continue
        except Exception as e: print(f"⚠ Details error {county['county_name']}: {e}")
        finally:
            try:
                await self.goto_with_retry(page, list_url); await self.dismiss_banners(page)
                await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
            except: pass
        return extracted

    async def safe_get_cell_text(self,row,colmap,key):
        try:
            idx=colmap.get(key)
            if idx is None: return ""
            cells=await row.locator("td").all()
            if idx<len(cells):
                txt=await cells[idx].inner_text()
                return re.sub(r"\s+"," ",txt).strip()
            return ""
        except: return ""

    async def scrape_county_sales(self,page,county):
        url=f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        print(f"[INFO] {county['county_name']}")
        for attempt in range(MAX_RETRIES):
            try:
                await self.goto_with_retry(page,url); await self.dismiss_banners(page)
                try: await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData",timeout=30000)
                except PlaywrightTimeoutError: return []
                colmap=await self.get_table_columns(page)
                if not colmap: return []
                rows=page.locator("table.table.table-striped tbody tr")
                n=await rows.count(); results=[]
                for i in range(n):
                    row=rows.nth(i)
                    details_a=row.locator("td.hidden-print a")
                    details_href=(await details_a.get_attribute("href")) or ""
                    details_url=details_href if details_href.startswith("http") else urljoin(BASE_URL,details_href)
                    pid=extract_property_id_from_href(details_href)
                    sdate=await self.safe_get_cell_text(row,colmap,"sales_date")
                    defn=await self.safe_get_cell_text(row,colmap,"defendant")
                    addr=await self.safe_get_cell_text(row,colmap,"address")
                    curdata={"address":addr,"defendant":defn,"sales_date":sdate}
                    dd=await self.get_details_data(page,details_url,url,county,curdata)
                    rec={"Property ID":pid,"Address":dd["address"],"Defendant":dd["defendant"],
                         "Sales Date":dd["sales_date"],"Approx Judgment":dd["approx_judgment"],
                         "County":county["county_name"]}
                    if county["county_id"]=="24": rec["Sale Type"]=dd["sale_type"]
                    results.append(rec)
                return results
            except Exception as e:
                print("❌ scrape error",county['county_name'],e); await asyncio.sleep(2**attempt)
        return []

    async def get_table_columns(self,page):
        try:
            header=page.locator("table.table.table-striped thead tr th")
            if await header.count()==0:
                header=page.locator("table.table.table-striped tr").first.locator("th")
            colmap={}
            for i in range(await header.count()):
                txt=(await header.nth(i).inner_text()).strip().lower()
                if "sale" in txt and "date" in txt: colmap["sales_date"]=i
                elif "defendant" in txt: colmap["defendant"]=i
                elif "address" in txt: colmap["address"]=i
            return colmap
        except: return {}

# -----------------------------
# Orchestration
# -----------------------------
async def run():
    spreadsheet_id=os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id: sys.exit("Missing SPREADSHEET_ID")
    service=init_sheets_service_from_env()
    sheets=SheetsClient(spreadsheet_id,service)

    all_data=[]; dashboard_rows=[]
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=True)
        page=await browser.new_page(); scraper=ForeclosureScraper(sheets)
        for county in TARGET_COUNTIES:
            tab=county["county_name"][:30]
            try:
                recs=await scraper.scrape_county_sales(page,county)
                if not recs: continue
                df=pd.DataFrame(recs); headers=[c for c in df.columns if c!="County"]
                rows=df.drop(columns=["County"]).astype(str).values.tolist()
                sheets.create_sheet_if_missing(tab)
                total,new=sheets.write_snapshot(tab,headers,rows,filter_30=True)
                dashboard_rows.append([county["county_name"],total,new])
                all_data.extend(df.astype(str).values.tolist())
                await asyncio.sleep(POLITE_DELAY_SECONDS)
            except Exception as e: print("Error:",e)
        await browser.close()

    if all_data:
        header=["Property ID","Address","Defendant","Sales Date","Approx Judgment","County","Sale Type"]
        rows_out=[]
        for r in all_data: 
            while len(r)<7: r.append("")
            rows_out.append(r[:7])
        sheets.create_sheet_if_missing("All Data")
        sheets.write_snapshot("All Data",header,rows_out,filter_30=False)

    if dashboard_rows:
        sheets.create_sheet_if_missing("Dashboard")
        now=datetime.now().strftime("%Y-%m-%d")
        values=[["Dashboard - Snapshot "+now],["County","Active (30d)","New Today"]]+dashboard_rows
        sheets.clear("Dashboard"); sheets.write_values("Dashboard",values)

if __name__=="__main__": asyncio.run(run())