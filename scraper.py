#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Playwright-based scraper
- includes async context manager launch_browser()
- filters each row to the next 30 days before returning
"""

import asyncio
from contextlib import asynccontextmanager
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

WINDOW_DAYS = 30
BASE_URL = "https://salesweb.civilview.com/"
MAX_RETRIES = 5

def parse_sale_date(date_str):
    if not date_str:
        return None
    fmts = [
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m-%d-%Y",
    ]
    for f in fmts:
        try:
            return datetime.strptime(date_str.strip(), f)
        except Exception:
            continue
    return None

def is_within_30_days(date_str, today=None):
    dt = parse_sale_date(date_str)
    if not dt:
        return False
    t = today or datetime.now()
    end = t + timedelta(days=WINDOW_DAYS - 1)
    return t.date() <= dt.date() <= end.date()

def extract_property_id_from_href(href):
    try:
        q = parse_qs(urlparse(href).query)
        return q.get("PropertyId", [""])[0]
    except Exception:
        return ""

class ForeclosureScraper:
    def __init__(self, headless=True, retries=MAX_RETRIES):
        self.headless = headless
        self.retries = retries
        self._play = None
        self._browser = None

    @asynccontextmanager
    async def launch_browser(self):
        """
        Yields a Playwright Page instance; cleans up on exit.
        Usage: async with scraper.launch_browser() as page:
        """
        self._play = await async_playwright().start()
        self._browser = await self._play.chromium.launch(headless=self.headless)
        page = await self._browser.new_page()
        try:
            yield page
        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await self._browser.close()
            except Exception:
                pass
            try:
                await self._play.stop()
            except Exception:
                pass

    async def goto_with_retry(self, page, url):
        for attempt in range(self.retries):
            try:
                resp = await page.goto(url, wait_until="networkidle", timeout=60000)
                if resp and resp.status == 200:
                    return resp
            except PlaywrightTimeoutError:
                print(f"Timeout attempt {attempt+1} for {url}")
            except Exception as e:
                print(f"Navigation error attempt {attempt+1}: {e}")
            await asyncio.sleep(2**attempt)
        return None

    async def get_columns(self, page):
        cols = {}
        ths = page.locator("table.table thead th")
        try:
            n = await ths.count()
        except Exception:
            return cols
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

    async def scrape_county_sales(self, page, county):
        """
        Returns only rows that fall into the next 30 days window.
        Each returned row is: [Property ID, Address, Defendant, Sales Date, Approx Judgment]
        """
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        await self.goto_with_retry(page, url)
        try:
            await page.wait_for_selector("table.table tbody tr, .no-sales, #noData", timeout=30000)
        except PlaywrightTimeoutError:
            return []
        if await page.locator(".no-sales, #noData").count() > 0:
            return []

        colmap = await self.get_columns(page)
        rows = page.locator("table.table tbody tr")
        n = await rows.count()
        out = []
        for i in range(n):
            r = rows.nth(i)
            a = r.locator("td.hidden-print a")
            href = (await a.get_attribute("href")) or ""
            pid = extract_property_id_from_href(href)

            cells = await r.locator("td").all()
            def safe(idx):
                try:
                    return (await cells[idx].inner_text()).strip()
                except Exception:
                    return ""

            sales_date = safe(colmap.get("sales_date", 0))
            # Filter here: only append rows in the next 30 days window
            if not is_within_30_days(sales_date):
                continue

            addr = safe(colmap.get("address", 0))
            defendant = safe(colmap.get("defendant", 0))
            approx_judg = safe(colmap.get("approx_judgment", 0))

            out.append([pid, addr, defendant, sales_date, approx_judg])
        return out
