import asyncio
import os
import re
import sys
from datetime import date, datetime, timedelta

from parsel import Selector
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.db.db_tool import DbTools

BASE_URL = "http://www.cffex.com.cn/ccpm/"
PAGE_TIMEOUT_MS = 15000
REQUEST_SLEEP_SECONDS = 1
LOGGER = get_logger("cffex")
PROGRESS_STORE = ProgressStore("cffex")

PRODUCTS = {
    "IF": {"name": "CSI 300 Index Futures", "listed_date": "2010-04-16"},
    "IH": {"name": "SSE 50 Index Futures", "listed_date": "2015-04-16"},
    "IC": {"name": "CSI 500 Index Futures", "listed_date": "2015-04-16"},
    "IM": {"name": "CSI 1000 Index Futures", "listed_date": "2022-07-22"},
    "TS": {"name": "2-Year Treasury Futures", "listed_date": "2018-08-17"},
    "TF": {"name": "5-Year Treasury Futures", "listed_date": "2013-09-06"},
    "T": {"name": "10-Year Treasury Futures", "listed_date": "2015-03-20"},
    "TL": {"name": "30-Year Treasury Futures", "listed_date": "2023-04-21"},
}


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def normalize_date(value):
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()


def load_progress():
    return PROGRESS_STORE.load()


def save_progress(progress_key):
    PROGRESS_STORE.append(progress_key)


def log_error(product_code, target_date, error_message):
    LOGGER.error("%s,%s,%s", product_code, target_date, error_message)


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_contract_code(contract_text, product_code):
    contract_text = clean_text(contract_text).upper()
    match = re.search(rf"{product_code}\d{{3,4}}", contract_text)
    if match:
        return match.group(0)
    return contract_text.replace("CONTRACT:", "").replace("CONTRACT", "").strip()


def parse_numeric(value):
    text = clean_text(value)
    if not text or text in {"-", "--"}:
        return None

    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def pick_rank_no(volume_rank, long_rank, short_rank):
    for value in (volume_rank, long_rank, short_rank):
        text = clean_text(value)
        if text and text not in {"-", "--"}:
            return text
    return ""


def is_summary_row(*values):
    for value in values:
        text = clean_text(value)
        if text == "合计":
            return True
    return False


def parse_trade_date_text(trade_date_text, fallback_date):
    text = clean_text(trade_date_text)
    match = re.search(r"(\d{4})[^\d]?(\d{1,2})[^\d]?(\d{1,2})", text)
    if not match:
        return fallback_date
    return f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"


def parse_html_rows(html_content, product_code, product_name, target_date):
    doc = Selector(text=html_content)
    rows_to_save = []

    contract_sections = doc.xpath('//div[contains(@class, "IF_first") and contains(@class, "clearFloat")]')
    for section in contract_sections:
        contract_text = clean_text(section.xpath(".//a/text()").get())
        contract_code = parse_contract_code(contract_text, product_code)
        if not contract_code:
            continue

        trade_date_text = section.xpath(".//p/text()").get()
        trade_date = parse_trade_date_text(trade_date_text, target_date)

        table = section.xpath('./following-sibling::div[contains(@class, "if-table")][1]//table')
        if not table:
            continue

        data_rows = table.xpath(".//tr[position() > 1]")
        for row in data_rows:
            values = [clean_text(value) for value in row.xpath(".//td/text()").getall()]
            if not values:
                continue

            while len(values) < 12:
                values.append("")

            volume_rank, volume_member, volume_value, volume_change = values[0:4]
            long_rank, long_member, long_open_interest, long_change = values[4:8]
            short_rank, short_member, short_open_interest, short_change = values[8:12]

            if is_summary_row(
                volume_rank,
                volume_member,
                long_rank,
                long_member,
                short_rank,
                short_member,
            ):
                continue

            rank_no = pick_rank_no(volume_rank, long_rank, short_rank)
            if not rank_no:
                continue

            rows_to_save.append({
                "product_code": product_code,
                "product_name": product_name,
                "contract_code": contract_code,
                "trade_date": trade_date,
                "rank_no": rank_no,
                "volume_rank": volume_rank,
                "volume_member": volume_member,
                "volume_value": parse_numeric(volume_value),
                "volume_change_value": parse_numeric(volume_change),
                "long_rank": long_rank,
                "long_member": long_member,
                "long_open_interest": parse_numeric(long_open_interest),
                "long_change_value": parse_numeric(long_change),
                "short_rank": short_rank,
                "short_member": short_member,
                "short_open_interest": parse_numeric(short_open_interest),
                "short_change_value": parse_numeric(short_change),
                "source_url": BASE_URL,
            })

    return rows_to_save


def iter_weekdays(start_date, end_date):
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


async def query_single_trade_day(page, product_code, target_date):
    product_name = PRODUCTS[product_code]["name"]
    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    await page.fill("#actualDate", target_date)
    await page.select_option("#selectSec", product_code)
    await page.click(".btn-query")

    try:
        await page.wait_for_selector(".if-table", state="visible", timeout=PAGE_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        return []

    html_content = await page.content()
    return parse_html_rows(html_content, product_code, product_name, target_date)


async def sync_rows(db_tools, rows):
    if not rows:
        return 0
    return await db_tools.upsert_cffex_member_rankings(rows)


async def process_trade_day(page, db_tools, product_code, target_date, processed):
    progress_key = f"{product_code},{target_date}"
    if progress_key in processed:
        return 0

    try:
        rows = await query_single_trade_day(page, product_code, target_date)
        inserted = await sync_rows(db_tools, rows)
        save_progress(progress_key)
        processed.add(progress_key)
        print(f"{product_code} {target_date} saved rows: {inserted}")
        await asyncio.sleep(REQUEST_SLEEP_SECONDS)
        return inserted
    except Exception as exc:
        error_message = str(exc)
        print(f"{product_code} {target_date} failed: {error_message}")
        log_error(product_code, target_date, error_message)
        await asyncio.sleep(REQUEST_SLEEP_SECONDS)
        return 0


async def backfill_all_history(headless=True, end_date=None, product_codes=None):
    selected_codes = product_codes or list(PRODUCTS.keys())
    processed = load_progress()
    db_tools = DbTools()
    await db_tools.init_pool()

    end_trade_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else datetime.now().date()
    total_rows = 0

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=headless)
            page = await browser.new_page()
            try:
                for product_code in selected_codes:
                    listed_date = datetime.strptime(PRODUCTS[product_code]["listed_date"], "%Y-%m-%d").date()
                    for trade_date in iter_weekdays(listed_date, end_trade_date):
                        total_rows += await process_trade_day(
                            page,
                            db_tools,
                            product_code,
                            normalize_date(trade_date),
                            processed,
                        )
            finally:
                await browser.close()
    finally:
        await db_tools.close()

    return total_rows


async def sync_latest_daily_data(headless=True, end_date=None, product_codes=None):
    selected_codes = product_codes or list(PRODUCTS.keys())
    processed = load_progress()
    db_tools = DbTools()
    await db_tools.init_pool()

    latest_dates = await db_tools.get_cffex_latest_trade_dates(selected_codes)
    end_trade_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else datetime.now().date()
    total_rows = 0

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=headless)
            page = await browser.new_page()
            try:
                for product_code in selected_codes:
                    latest_date = latest_dates.get(product_code)
                    if latest_date:
                        start_trade_date = datetime.strptime(latest_date, "%Y-%m-%d").date() + timedelta(days=1)
                    else:
                        start_trade_date = datetime.strptime(
                            PRODUCTS[product_code]["listed_date"],
                            "%Y-%m-%d",
                        ).date()

                    for trade_date in iter_weekdays(start_trade_date, end_trade_date):
                        total_rows += await process_trade_day(
                            page,
                            db_tools,
                            product_code,
                            normalize_date(trade_date),
                            processed,
                        )
            finally:
                await browser.close()
    finally:
        await db_tools.close()

    return total_rows


def parse_product_codes(arguments):
    if not arguments:
        return list(PRODUCTS.keys())

    selected_codes = []
    for code in arguments:
        normalized_code = code.strip().upper()
        if normalized_code not in PRODUCTS:
            raise ValueError(f"unsupported product code: {code}")
        selected_codes.append(normalized_code)
    return selected_codes


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "daily"

    if command == "backfill":
        product_codes = parse_product_codes(sys.argv[2:]) if len(sys.argv) > 2 else list(PRODUCTS.keys())
        total_rows = await backfill_all_history(headless=True, product_codes=product_codes)
        print(f"backfill finished, affected rows: {total_rows}")
        return

    if command == "daily":
        product_codes = parse_product_codes(sys.argv[2:]) if len(sys.argv) > 2 else list(PRODUCTS.keys())
        total_rows = await sync_latest_daily_data(headless=True, product_codes=product_codes)
        print(f"daily sync finished, affected rows: {total_rows}")
        return

    if command == "single":
        if len(sys.argv) < 4:
            raise ValueError("usage: py cffex_main.py single <YYYY-MM-DD> <PRODUCT_CODE>")
        target_date = sys.argv[2]
        product_code = parse_product_codes([sys.argv[3]])[0]
        db_tools = DbTools()
        await db_tools.init_pool()
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=False)
                page = await browser.new_page()
                try:
                    rows = await query_single_trade_day(page, product_code, target_date)
                    affected_rows = await sync_rows(db_tools, rows)
                    print(f"single day sync finished, affected rows: {affected_rows}")
                finally:
                    await browser.close()
        finally:
            await db_tools.close()
        return

    raise ValueError("supported commands: backfill, daily, single")


if __name__ == "__main__":
    asyncio.run(main())
