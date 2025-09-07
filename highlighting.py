#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Sheets helpers:
- 30-day rolling window
- Highlight new rows
"""

from datetime import datetime, timedelta
from rambow import Color

# -----------------------------
# Date Helpers
# -----------------------------
def today_str():
    return datetime.now().strftime("%Y-%m-%d")

def parse_date(date_str):
    fmts = ["%m/%d/%Y %I:%M %p", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"]
    for f in fmts:
        try:
            return datetime.strptime(date_str.strip(), f)
        except Exception:
            pass
    return None

def is_within_30_days(date_str, today=None):
    today = today or datetime.now()
    dt = parse_date(date_str)
    if not dt:
        return False
    return today.date() <= dt.date() <= (today + timedelta(days=30)).date()

# -----------------------------
# Sheets Helper
# -----------------------------
class SheetsHelper:
    def __init__(self, spreadsheet_id, service):
        self.sid = spreadsheet_id
        self.svc = service.spreadsheets()

    def ensure_sheet(self, name):
        tabs = self.svc.get(spreadsheetId=self.sid).execute()["sheets"]
        if not any(s["properties"]["title"] == name for s in tabs):
            self.svc.batchUpdate(spreadsheetId=self.sid, body={
                "requests": [{"addSheet": {"properties": {"title": name}}}]
            }).execute()

    def get_existing_keys(self, sheet):
        try:
            vals = self.svc.values().get(
                spreadsheetId=self.sid, range=f"'{sheet}'!A:C"
            ).execute().get("values", [])
        except Exception:
            return set()
        keys = set()
        for row in vals[1:]:
            if row:
                keys.add(row[0] or "|".join(row[:3]))
        return keys

    def write_snapshot(self, sheet, df, prev_keys):
        values = [["Snapshot", today_str()]] + [df.columns.tolist()] + df.values.tolist()
        self.svc.values().clear(spreadsheetId=self.sid, range=f"'{sheet}'!A:Z").execute()
        self.svc.values().update(
            spreadsheetId=self.sid, range=f"'{sheet}'!A1",
            valueInputOption="USER_ENTERED", body={"values": values}
        ).execute()

        # Highlighting logic
        info = self.svc.get(spreadsheetId=self.sid).execute()
        sid = next(s["properties"]["sheetId"] for s in info["sheets"] if s["properties"]["title"] == sheet)
        new_keys = {
            (r[0] if r[0] else "|".join(r[:3]))
            for r in df.values.tolist()
            if (r[0] if r[0] else "|".join(r[:3])) not in prev_keys
        }

        requests = []
        for i, row in enumerate(df.values.tolist()):
            key = row[0] or "|".join(row[:3])
            if key in new_keys:
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": sid, "startRowIndex": i+2,
                                  "endRowIndex": i+3, "startColumnIndex": 0,
                                  "endColumnIndex": len(df.columns)},
                        "cell": {"userEnteredFormat": {
                            "backgroundColor": {"red": 0.8, "green": 1, "blue": 0.8}
                        }},
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

        if requests:
            self.svc.batchUpdate(spreadsheetId=self.sid, body={"requests": requests}).execute()

        return len(df), len(new_keys)

    def write_dashboard(self, df):
        values = [["Dashboard", today_str()]] + [df.columns.tolist()] + df.values.tolist()
        self.svc.values().clear(spreadsheetId=self.sid, range="'Dashboard'!A:Z").execute()
        self.svc.values().update(
            spreadsheetId=self.sid, range="'Dashboard'!A1",
            valueInputOption="USER_ENTERED", body={"values": values}
        ).execute()