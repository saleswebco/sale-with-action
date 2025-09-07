#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper for Tyler SalesWeb foreclosure data
"""

import asyncio
from urllib.parse import urlparse, parse_qs
from rambow import Color
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Counties to scrape
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

# -----------------------------
# Helpers
# -----------------------------
def extract_property_id(href):
    try:
        q = parse_qs(urlparse(href).query)
        return q.get("PropertyId", [""])[0]
    except Exception:
        return ""

# -----------------------------
# Scraper Class
# -----------------------------
class ForeclosureScraper:
    def __init__(self, base_url, retries=3):
        self.base_url = base_url
        self.retries = retries

    async def launch_browser(self):
        self.p = await async_playwright().start()
        self.browser = await self.p.chromium.launch(headless=True)
        return await self.browser.new_page()

    async def close(self):
        await self.browser.close()
        await self.p.stop()

    async def goto_url(self, page, url):
        for attempt in range(self.retries):
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                if resp and resp.status == 200:
                    return True
            except PlaywrightTimeoutError:
                print(Color.red(f"⏳ Timeout {attempt+1} for {url}"))
            await asyncio.sleep(2**attempt)
        return False

    async def scrape_county_sales(self, page, county):
        url = f"{self.base_url}Sales/SalesSearch?countyId={county['county_id']}"
        if not await self.goto_url(page, url):
            return []

        try:
            await page.wait_for_selector("table.table tbody tr, .no-sales, #noData", timeout=20000)
        except PlaywrightTimeoutError:
            return []

        no_data = await page.locator(".no-sales, #noData").all_text_contents()
        if no_data:
            return []

        colmap = await self.get_columns(page)
        rows = page.locator("table.table tbody tr")
        n = await rows.count()
        results = []

        for i in range(n):
            r = rows.nth(i)
            a = r.locator("td.hidden-print a")
            href = (await a.get_attribute("href")) or ""
            pid = extract_property_id(href)

            cells = await r.locator("td").all()
            data = {
                "Property ID": pid,
                "Address": await cells[colmap.get("address", 0)].inner_text(),
                "Defendant": await cells[colmap.get("defendant", 0)].inner_text(),
                "Sales Date": await cells[colmap.get("sales_date", 0)].inner_text(),
                "Approx Judgment": await cells[colmap.get("approx_judgment", 0)].inner_text(),
            }
            results.append(list(data.values()))

        return results

    async def get_columns(self, page):
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