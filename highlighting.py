#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Sheets helpers:
- 30-day rolling window utilities
- Snapshot writing
- New-row highlighting (green)
- Safe credentials loading
"""

import os
import json
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

WINDOW_DAYS = 30
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# -----------------------------
# Date utilities
# -----------------------------
def today_dt():
    return datetime.now()

def today_str(fmt="%Y-%m-%d"):
    return today_dt().strftime(fmt)

def parse_date(date_str):
    if not date_str or not isinstance(date_str, str):
        return None
    fmts = [
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%d/%m/%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(date_str.strip(), f)
        except Exception:
            continue
    return None

def window_range(today=None):
    t = today or today_dt()
    start = t
    end = t + timedelta(days=WINDOW_DAYS - 1)  # inclusive window of WINDOW_DAYS
    return start, end

def is_within_30_days(date_str, today=None):
    dt = parse_date(date_str)
    if not dt:
        return False
    start, end = window_range(today)
    return start.date() <= dt.date() <= end.date()


# -----------------------------
# Credentials
# -----------------------------
def load_service_account_info():
    path = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not raw:
        raise ValueError("Missing GOOGLE_CREDENTIALS or GOOGLE_CREDENTIALS_FILE")
    raw = raw.strip()
    if raw.startswith("{"):
        return json.loads(raw)
    if os.path.exists(raw):
        with open(raw, "r", encoding="utf-8") as fh:
            return json.load(fh)
    raise ValueError("Invalid GOOGLE_CREDENTIALS value")


def init_sheets_service_from_env():
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    # Return spreadsheets() resource for convenience
    return build("sheets", "v4", credentials=creds).spreadsheets()


# -----------------------------
# Sheets client
# -----------------------------
class SheetsClient:
    def __init__(self, spreadsheet_id, service_resource):
        """
        service_resource: build(...).spreadsheets() resource
        """
        self.spreadsheet_id = spreadsheet_id
        self.svc = service_resource

    def sheet_exists(self, name):
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        return any(s["properties"]["title"] == name for s in info.get("sheets", []))

    def create_sheet_if_missing(self, name):
        if not self.sheet_exists(name):
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": name}}}]}
            ).execute()

    def get_values(self, sheet, rng="A:Z"):
        try:
            res = self.svc.values().get(spreadsheetId=self.spreadsheet_id, range=f"'{sheet}'!{rng}").execute()
            return res.get("values", [])
        except Exception:
            return []

    def clear(self, sheet, rng="A:Z"):
        try:
            self.svc.values().clear(spreadsheetId=self.spreadsheet_id, range=f"'{sheet}'!{rng}").execute()
        except Exception:
            pass

    def write_values(self, sheet, values):
        self.svc.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{sheet}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()

    def get_previous_keys(self, sheet_name):
        """
        Robustly find the header row (looks for 'Property ID' cell),
        then collect unique keys from rows after the header.
        Keys = Property ID if present, otherwise Address|Defendant.
        """
        vals = self.get_values(sheet_name)
        if not vals or len(vals) < 2:
            return set()

        header_idx = -1
        for i, row in enumerate(vals):
            if row and len(row) > 0 and row[0].strip().lower() == "property id":
                header_idx = i
                break
        if header_idx == -1:
            # fallback: if no header found, try to skip 2 top rows (snapshot+header)
            start_idx = 2 if len(vals) > 2 else 0
        else:
            start_idx = header_idx + 1

        keys = set()
        for row in vals[start_idx:]:
            if not row:
                continue
            try:
                pid = row[0].strip() if len(row) > 0 else ""
                if pid:
                    keys.add(pid)
                else:
                    addr = row[1].strip() if len(row) > 1 else ""
                    defn = row[2].strip() if len(row) > 2 else ""
                    if addr or defn:
                        keys.add(f"{addr}|{defn}")
            except Exception:
                continue
        return keys

    def write_snapshot(self, sheet_name, headers, rows, prev_keys):
        """
        Write snapshot and apply formatting.
        - headers: list of column headers
        - rows: list of lists (already filtered to the 30-day window)
        - prev_keys: set of keys from existing sheet (obtained BEFORE calling this)
        """
        start, end = window_range()
        start_label = start.strftime("%Y-%m-%d")
        end_label = end.strftime("%Y-%m-%d")
        snapshot_label = f"Snapshot for {start.strftime('%A')} - {start_label} → {end_label}"

        # compute new keys set
        new_keys = set()
        filtered_rows = []
        for r in rows:
            # r should be list aligned with headers
            # compute key same as get_previous_keys
            pid = r[0].strip() if len(r) > 0 and r[0] else ""
            if pid:
                key = pid
            else:
                addr = r[1].strip() if len(r) > 1 and r[1] else ""
                defn = r[2].strip() if len(r) > 2 and r[2] else ""
                key = f"{addr}|{defn}"
            filtered_rows.append(r)
            if key not in {str(k) for k in prev_keys}:
                new_keys.add(str(key))

        # assemble values: snapshot row, header row, data rows
        values = [[snapshot_label]] + [headers] + filtered_rows
        # clear & write
        self.clear(sheet_name)
        self.write_values(sheet_name, values)
        # apply formatting and highlighting
        self.apply_formatting(sheet_name, headers, filtered_rows, new_keys)
        return len(filtered_rows), len(new_keys)

    def apply_formatting(self, sheet_name, headers, rows, new_keys):
        """
        Apply header bold, freeze header rows, autocol resize, and highlight new rows.
        Note: Google Sheets uses 0-based indices.
        Snapshot row = row index 0
        Header row   = row index 1
        First data row = index 2
        """
        info = self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        sheet_id = None
        for s in info.get("sheets", []):
            if s["properties"]["title"] == sheet_name:
                sheet_id = s["properties"]["sheetId"]
                break
        if sheet_id is None:
            return

        num_cols = len(headers)
        requests = [
            # bold header row (index 1)
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold"
                }
            },
            # freeze top 2 rows (snapshot + header)
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
                    "fields": "gridProperties.frozenRowCount"
                }
            },
            # auto-resize columns
            {
                "autoResizeDimensions": {
                    "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": num_cols}
                }
            }
        ]

        # highlight new rows: each data row's sheet index = idx + 2
        for idx, row in enumerate(rows):
            pid = row[0].strip() if len(row) > 0 and row[0] else ""
            if pid:
                key = pid
            else:
                addr = row[1].strip() if len(row) > 1 and row[1] else ""
                defn = row[2].strip() if len(row) > 2 and row[2] else ""
                key = f"{addr}|{defn}"
            if str(key) in {str(k) for k in new_keys}:
                start_row = idx + 2
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": start_row,
                            "endRowIndex": start_row + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": num_cols
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}}},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

        if requests:
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
