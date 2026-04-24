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
QUERY_MAX_ATTEMPTS = 2
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
CONTRACT_CODE_PATTERN = re.compile(r"(?:TL|TF|TS|IF|IH|IC|IM|T)\d{3,4}", re.IGNORECASE)
CONTRACT_PRODUCT_PATTERN = re.compile(r"^(TL|TF|TS|IF|IH|IC|IM|T)\d{3,4}$", re.IGNORECASE)
FUTURES_RADIO_VALUE = "\u671f\u8d27"


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
    contract_text = clean_text(contract_text).upper().replace("：", ":")
    match = re.search(rf"\b{re.escape(product_code)}\d{{3,4}}\b", contract_text)
    if match:
        return match.group(0)

    normalized_text = (
        contract_text
        .replace("合约:", " ")
        .replace("合约", " ")
        .replace("CONTRACT:", " ")
        .replace("CONTRACT", " ")
    )
    normalized_text = clean_text(normalized_text)
    match = re.search(rf"\b{re.escape(product_code)}\d{{3,4}}\b", normalized_text)
    if match:
        return match.group(0)

    generic_match = CONTRACT_CODE_PATTERN.search(normalized_text)
    if generic_match:
        return generic_match.group(0).upper()

    return ""


def infer_product_code_from_contract_code(contract_code):
    text = clean_text(contract_code).upper()
    match = CONTRACT_PRODUCT_PATTERN.match(text)
    if not match:
        return ""
    return match.group(1).upper()


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
    mismatch_contract_codes = []

    contract_sections = doc.xpath('//div[contains(@class, "IF_first") and contains(@class, "clearFloat")]')
    for section in contract_sections:
        contract_text = clean_text(" ".join(section.xpath(".//a//text()").getall()))
        if not contract_text:
            contract_text = clean_text(section.xpath("string(.)").get())
        contract_code = parse_contract_code(contract_text, product_code)
        if not contract_code:
            continue
        resolved_product_code = infer_product_code_from_contract_code(contract_code) or product_code
        if resolved_product_code != product_code:
            mismatch_contract_codes.append(contract_code)
            continue
        resolved_product_name = PRODUCTS.get(resolved_product_code, {}).get("name", product_name)

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
                "product_code": resolved_product_code,
                "product_name": resolved_product_name,
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

    if mismatch_contract_codes:
        preview = ",".join(mismatch_contract_codes[:5])
        LOGGER.warning(
            "cffex requested product %s but skipped mismatched contracts on %s: %s%s",
            product_code,
            target_date,
            preview,
            "..." if len(mismatch_contract_codes) > 5 else "",
        )

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
    await ensure_futures_mode(page)
    await ensure_product_option_ready(page, product_code)
    await page.fill("#actualDate", target_date)
    await page.select_option("#selectSec", product_code)
    await page.wait_for_function(
        """
        (expectedProductCode) => {
            const select = document.querySelector("#selectSec");
            if (!select) {
                return false;
            }
            return ((select.value || "").trim().toUpperCase() === expectedProductCode);
        }
        """,
        arg=product_code,
        timeout=PAGE_TIMEOUT_MS,
    )
    await page.click(".btn-query")

    try:
        await wait_for_query_result(page, product_code)
    except PlaywrightTimeoutError:
        state = await describe_query_state(page)
        raise RuntimeError(
            f"cffex query did not switch to {product_code} on {target_date}; "
            f"selected={state['selected_product'] or '-'} headings={','.join(state['headings']) or '-'}"
        )

    html_content = await page.content()
    return parse_html_rows(html_content, product_code, product_name, target_date)


async def ensure_futures_mode(page):
    futures_radio = page.locator(f'input[name="radio"][value="{FUTURES_RADIO_VALUE}"]')
    await futures_radio.wait_for(state="attached", timeout=PAGE_TIMEOUT_MS)
    if await futures_radio.is_checked():
        await futures_radio.click()
    else:
        await futures_radio.check()
    await page.wait_for_function(
        """
        () => {
            const checkedRadio = document.querySelector('input[name="radio"]:checked');
            return !!checkedRadio && (checkedRadio.value || "").trim() === "\u671f\u8d27";
        }
        """,
        timeout=PAGE_TIMEOUT_MS,
    )


async def ensure_product_option_ready(page, product_code):
    await page.wait_for_selector("#selectSec", state="visible", timeout=PAGE_TIMEOUT_MS)
    await page.wait_for_function(
        """
        (expectedProductCode) => {
            const select = document.querySelector("#selectSec");
            if (!select) {
                return false;
            }
            return Array.from(select.options).some(
                (option) => ((option.value || "").trim().toUpperCase() === expectedProductCode)
            );
        }
        """,
        arg=product_code,
        timeout=PAGE_TIMEOUT_MS,
    )


async def wait_for_query_result(page, product_code):
    await page.wait_for_function(
        """
        (expectedProductCode) => {
            const normalize = (text) => (text || "").replace(/\\s+/g, " ").trim().toUpperCase();
            const select = document.querySelector("#selectSec");
            const selectedProduct = normalize(select ? select.value : "");
            if (selectedProduct !== expectedProductCode) {
                return false;
            }

            const noDataTokens = ["\u6682\u65e0\u6570\u636e", "\u65e0\u6570\u636e", "\u672a\u67e5\u8be2\u5230", "\u6ca1\u6709\u67e5\u8be2\u5230"];
            const bodyText = normalize(document.body ? document.body.innerText : "");
            if (noDataTokens.some((token) => bodyText.includes(token))) {
                return true;
            }

            const sections = Array.from(document.querySelectorAll("div.IF_first.clearFloat"));
            if (!sections.length) {
                return false;
            }

            const contractMatches = sections
                .map((section) => {
                    const anchorText = Array.from(section.querySelectorAll("a"))
                        .map((node) => node.textContent || "")
                        .join(" ");
                    const rawText = normalize(anchorText || section.textContent || "");
                    const match = rawText.match(/(?:TL|TF|TS|IF|IH|IC|IM|T)\\d{3,4}/);
                    return match ? match[0] : "";
                })
                .filter(Boolean);

            return contractMatches.length > 0
                && contractMatches.every((contractCode) => contractCode.startsWith(expectedProductCode));
        }
        """,
        arg=product_code,
        timeout=PAGE_TIMEOUT_MS,
    )


async def describe_query_state(page):
    return await page.evaluate(
        """
        () => {
            const normalize = (text) => (text || "").replace(/\\s+/g, " ").trim().toUpperCase();
            const select = document.querySelector("#selectSec");
            const headings = Array.from(document.querySelectorAll("div.IF_first.clearFloat"))
                .map((section) => {
                    const anchorText = Array.from(section.querySelectorAll("a"))
                        .map((node) => node.textContent || "")
                        .join(" ");
                    return normalize(anchorText || section.textContent || "");
                })
                .filter(Boolean)
                .slice(0, 5);
            return {
                selected_product: normalize(select ? select.value : ""),
                headings,
            };
        }
        """
    )


async def sync_rows(db_tools, rows):
    if not rows:
        return 0
    return await db_tools.upsert_cffex_member_rankings(rows)


async def process_trade_day(page, db_tools, product_code, target_date, processed):
    progress_key = f"{product_code},{target_date}"
    if progress_key in processed:
        return 0

    last_error = None
    for attempt in range(1, QUERY_MAX_ATTEMPTS + 1):
        try:
            rows = await query_single_trade_day(page, product_code, target_date)
            inserted = await sync_rows(db_tools, rows)
            save_progress(progress_key)
            processed.add(progress_key)
            if rows:
                print(f"{product_code} {target_date} parsed rows: {len(rows)}, saved rows: {inserted}")
            else:
                print(f"{product_code} {target_date} parsed rows: 0, saved rows: 0")
            await asyncio.sleep(REQUEST_SLEEP_SECONDS)
            return inserted
        except Exception as exc:
            last_error = str(exc)
            if attempt < QUERY_MAX_ATTEMPTS:
                print(
                    f"{product_code} {target_date} attempt {attempt}/{QUERY_MAX_ATTEMPTS} failed, retrying: {last_error}"
                )
                await asyncio.sleep(REQUEST_SLEEP_SECONDS)
                continue

    print(f"{product_code} {target_date} failed: {last_error}")
    log_error(product_code, target_date, last_error)
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
    processed = set()
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
