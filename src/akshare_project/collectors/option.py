import asyncio
import re
import sys
from datetime import date, datetime, timedelta

from parsel import Selector
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.db.db_tool import DbTools

BASE_URL = "http://www.cffex.com.cn/rtj/"
PAGE_TIMEOUT_MS = 15000
REQUEST_SLEEP_SECONDS = 1
LOGGER = get_logger("option")
BACKFILL_PROGRESS_STORE = ProgressStore("option_rtj")

OPTION_PRODUCTS = {
    "HS300": {
        "index_name": "沪深300指数",
        "product_prefix": "IO",
        "listed_date": "2019-12-23",
    },
    "ZZ1000": {
        "index_name": "中证1000指数",
        "product_prefix": "MO",
        "listed_date": "2022-07-22",
    },
    "SZ50": {
        "index_name": "上证50指数",
        "product_prefix": "HO",
        "listed_date": "2022-12-19",
    },
}
PREFIX_TO_INDEX = {
    product["product_prefix"]: index_type
    for index_type, product in OPTION_PRODUCTS.items()
}
BACKFILL_START_DATE = min(
    datetime.strptime(product["listed_date"], "%Y-%m-%d").date()
    for product in OPTION_PRODUCTS.values()
)

HEADER_ALIASES = {
    "contract_code": ["合约代码", "合约"],
    "pre_settle_price": ["前结算", "昨结算", "上日结算"],
    "open_price": ["今开盘", "开盘价", "开盘"],
    "high_price": ["最高价", "最高"],
    "low_price": ["最低价", "最低"],
    "close_price": ["今收盘", "收盘价", "收盘"],
    "settle_price": ["今结算", "结算价", "今结算价"],
    "price_change_close": ["涨跌1"],
    "price_change_settle": ["涨跌2"],
    "volume": ["成交量"],
    "open_interest": ["持仓量"],
    "open_interest_change": ["持仓变化", "持仓增减"],
    "turnover": ["成交额", "成交金额"],
}
NO_DATA_PATTERNS = (
    "没有您所查询的数据",
    "没有查询到相关数据",
    "暂无数据",
)


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def load_progress():
    return BACKFILL_PROGRESS_STORE.load()


def save_progress(progress_key):
    BACKFILL_PROGRESS_STORE.append(progress_key)


def log_error(target_date, error_message):
    LOGGER.error("%s,%s", target_date, error_message)


def normalize_date(value):
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()


def normalize_trade_date(value):
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").strftime("%Y-%m-%d")


def iter_weekdays(start_date, end_date):
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def extract_row_cells(row):
    cells = row.xpath("./th | ./td")
    return [
        clean_text("".join(cell.xpath(".//text()").getall()))
        for cell in cells
    ]


def normalize_header(text):
    return re.sub(r"[\s:：()（）]", "", clean_text(text))


def parse_numeric(value):
    text = clean_text(value)
    if not text or text in {"-", "--", "---"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def detect_no_data(html_content):
    body_text = clean_text(" ".join(Selector(text=html_content).xpath("//body//text()").getall()))
    return any(pattern in body_text for pattern in NO_DATA_PATTERNS)


def extract_contract_meta(contract_code):
    text = str(contract_code or "").strip().upper()
    if not text:
        return None

    prefix_match = re.match(r"^(IO|MO|HO)", text)
    if not prefix_match:
        return None

    product_prefix = prefix_match.group(1)
    index_type = PREFIX_TO_INDEX.get(product_prefix)
    if not index_type:
        return None

    month_match = re.match(r"^(?:IO|MO|HO)(\d{4})", text)
    option_type_match = re.search(r"-(C|P)-", text)
    strike_match = re.search(r"-(\d+(?:\.\d+)?)$", text)

    return {
        "product_prefix": product_prefix,
        "index_type": index_type,
        "index_name": OPTION_PRODUCTS[index_type]["index_name"],
        "contract_month": month_match.group(1) if month_match else None,
        "option_type": (
            "CALL" if option_type_match and option_type_match.group(1) == "C"
            else "PUT" if option_type_match
            else None
        ),
        "strike_price": parse_numeric(strike_match.group(1)) if strike_match else None,
    }


def get_field_index(headers, field_name):
    normalized_headers = [normalize_header(header) for header in headers]
    for alias in HEADER_ALIASES[field_name]:
        normalized_alias = normalize_header(alias)
        for index, header in enumerate(normalized_headers):
            if header == normalized_alias or normalized_alias in header:
                return index
    return None


def score_header_row(headers):
    score = 0
    for field_name in HEADER_ALIASES:
        if get_field_index(headers, field_name) is not None:
            score += 1
    return score


def pick_best_table(doc):
    best = None
    best_score = -1
    best_header_index = None

    for table in doc.xpath("//table"):
        rows = table.xpath(".//tr")
        if not rows:
            continue

        for header_row_index in range(min(4, len(rows))):
            header_values = [value for value in extract_row_cells(rows[header_row_index]) if value]
            if not header_values:
                continue

            score = score_header_row(header_values)
            if score > best_score or (score == best_score and best is not None and len(header_values) > len(best[1])):
                best = (table, header_values)
                best_score = score
                best_header_index = header_row_index

    if best is None or best_score < 5:
        return None, None, None
    return best[0], best[1], best_header_index


def parse_rtj_option_rows(html_content, target_date):
    doc = Selector(text=html_content)
    table, headers, header_row_index = pick_best_table(doc)
    if table is None:
        return []

    field_indexes = {
        field_name: get_field_index(headers, field_name)
        for field_name in HEADER_ALIASES
    }

    rows_to_save = []
    data_rows = table.xpath(".//tr")[header_row_index + 1:]
    for row in data_rows:
        values = extract_row_cells(row)
        if not values:
            continue

        contract_index = field_indexes.get("contract_code")
        if contract_index is None or contract_index >= len(values):
            continue
        contract_code = values[contract_index].upper()
        if contract_code in {"合计", "小计"}:
            continue

        contract_meta = extract_contract_meta(contract_code)
        if not contract_meta:
            continue

        listed_date = OPTION_PRODUCTS[contract_meta["index_type"]]["listed_date"]
        if target_date < listed_date:
            continue

        def value_at(field_name):
            index = field_indexes.get(field_name)
            if index is None or index >= len(values):
                return None
            return values[index]

        rows_to_save.append({
            "index_type": contract_meta["index_type"],
            "index_name": contract_meta["index_name"],
            "product_prefix": contract_meta["product_prefix"],
            "contract_code": contract_code,
            "contract_month": contract_meta["contract_month"],
            "option_type": contract_meta["option_type"],
            "strike_price": contract_meta["strike_price"],
            "trade_date": target_date,
            "open_price": parse_numeric(value_at("open_price")),
            "high_price": parse_numeric(value_at("high_price")),
            "low_price": parse_numeric(value_at("low_price")),
            "close_price": parse_numeric(value_at("close_price")),
            "settle_price": parse_numeric(value_at("settle_price")),
            "pre_settle_price": parse_numeric(value_at("pre_settle_price")),
            "price_change_close": parse_numeric(value_at("price_change_close")),
            "price_change_settle": parse_numeric(value_at("price_change_settle")),
            "volume": parse_numeric(value_at("volume")),
            "turnover": parse_numeric(value_at("turnover")),
            "open_interest": parse_numeric(value_at("open_interest")),
            "open_interest_change": parse_numeric(value_at("open_interest_change")),
            "data_source": "cffex_rtj",
            "source_url": BASE_URL,
        })

    return rows_to_save


async def click_first_visible(page, selectors):
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if await locator.count():
                await locator.click(timeout=1200)
                return True
        except Exception:
            continue
    return False


async def switch_to_option_mode(page):
    clicked = await click_first_visible(page, [
        "text=期权",
        "label:has-text('期权')",
        "a:has-text('期权')",
        "li:has-text('期权')",
        "span:has-text('期权')",
        "button:has-text('期权')",
    ])
    if clicked:
        await page.wait_for_timeout(300)
        return

    changed = await page.evaluate(
        """
        () => {
          const normalize = (text) => (text || '').replace(/\\s+/g, '');
          const clickableNodes = Array.from(document.querySelectorAll('label, a, li, span, button, div'));
          for (const node of clickableNodes) {
            const text = normalize(node.textContent);
            if (text === '期权' || text.includes('期权')) {
              node.click();
              return true;
            }
          }
          for (const input of document.querySelectorAll('input[type="radio"], input[type="checkbox"]')) {
            let text = '';
            if (input.id) {
              const label = document.querySelector(`label[for="${input.id}"]`);
              text = normalize(label ? label.textContent : '');
            }
            if (!text) {
              text = normalize(input.parentElement ? input.parentElement.textContent : '');
            }
            if (text.includes('期权')) {
              input.click();
              input.dispatchEvent(new Event('change', { bubbles: true }));
              return true;
            }
          }
          for (const select of document.querySelectorAll('select')) {
            const option = Array.from(select.options).find(opt => normalize(opt.textContent).includes('期权'));
            if (option) {
              select.value = option.value;
              select.dispatchEvent(new Event('change', { bubbles: true }));
              return true;
            }
          }
          return false;
        }
        """
    )
    if not changed:
        raise ValueError("failed to switch rtj page to option mode")
    await page.wait_for_timeout(300)


async def select_all_contracts(page):
    changed = await page.evaluate(
        """
        () => {
          const normalize = (text) => (text || '').replace(/\\s+/g, '');
          let count = 0;
          for (const select of document.querySelectorAll('select')) {
            if (select.disabled) continue;
            const option = Array.from(select.options).find(opt => normalize(opt.textContent) === '全部' || normalize(opt.textContent).includes('全部'));
            if (!option) continue;
            if (select.value !== option.value) {
              select.value = option.value;
              select.dispatchEvent(new Event('change', { bubbles: true }));
            }
            count += 1;
          }
          return count;
        }
        """
    )
    if not changed:
        raise ValueError("failed to select 全部 on rtj option page")
    await page.wait_for_timeout(300)


async def fill_trade_date(page, target_date):
    for selector in ("#actualDate", "input[name='actualDate']", "input[name='tradeDate']", "input[type='date']"):
        locator = page.locator(selector).first
        try:
            if await locator.count():
                await locator.fill(target_date, timeout=1200)
                return
        except Exception:
            continue

    changed = await page.evaluate(
        """
        (targetDate) => {
          const inputs = Array.from(document.querySelectorAll('input'));
          for (const input of inputs) {
            const meta = `${input.id || ''} ${input.name || ''} ${input.className || ''} ${input.placeholder || ''}`.toLowerCase();
            if (meta.includes('date') || meta.includes('actual')) {
              input.removeAttribute('readonly');
              input.value = targetDate;
              input.dispatchEvent(new Event('input', { bubbles: true }));
              input.dispatchEvent(new Event('change', { bubbles: true }));
              return true;
            }
          }
          return false;
        }
        """,
        target_date,
    )
    if not changed:
        raise ValueError(f"failed to fill trade date: {target_date}")
    await page.wait_for_timeout(200)


async def click_query(page):
    clicked = await click_first_visible(page, [
        ".btn-query",
        "button:has-text('查询')",
        "input[value='查询']",
        "a:has-text('查询')",
    ])
    if clicked:
        return

    changed = await page.evaluate(
        """
        () => {
          const normalize = (text) => (text || '').replace(/\\s+/g, '');
          for (const node of document.querySelectorAll('button, input[type="button"], input[type="submit"], a, span, div')) {
            const text = normalize(node.textContent || node.value || '');
            if (text === '查询' || text.includes('查询')) {
              node.click();
              return true;
            }
          }
          return false;
        }
        """
    )
    if not changed:
        raise ValueError("failed to click query button on rtj option page")


async def query_single_trade_day(page, target_date):
    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    await switch_to_option_mode(page)
    await select_all_contracts(page)
    await fill_trade_date(page, target_date)
    await click_query(page)

    try:
        await page.wait_for_function(
            """
            () => {
              const text = (document.body.innerText || '');
              return document.querySelectorAll('table').length > 0
                || text.includes('没有您所查询的数据')
                || text.includes('没有查询到相关数据')
                || text.includes('暂无数据');
            }
            """,
            timeout=PAGE_TIMEOUT_MS,
        )
    except PlaywrightTimeoutError:
        raise ValueError(f"rtj option query timed out for {target_date}")

    await page.wait_for_timeout(500)
    html_content = await page.content()
    if detect_no_data(html_content):
        return []

    rows = parse_rtj_option_rows(html_content, target_date)
    if rows:
        return rows
    raise ValueError(f"failed to parse rtj option table for {target_date}")


async def sync_rows(db_tools, rows):
    if not rows:
        return 0
    return await db_tools.batch_option_rtj_daily_data(rows)


async def process_trade_day(page, db_tools, target_date, processed=None, save_state=False, swallow_exceptions=True):
    progress_key = normalize_trade_date(target_date)
    if save_state and processed is not None and progress_key in processed:
        return 0

    try:
        rows = await query_single_trade_day(page, progress_key)
        inserted = await sync_rows(db_tools, rows)
        if save_state and processed is not None:
            save_progress(progress_key)
            processed.add(progress_key)
        print(f"option {progress_key} saved rows: {inserted}")
        await asyncio.sleep(REQUEST_SLEEP_SECONDS)
        return inserted
    except Exception as exc:
        error_message = str(exc)
        print(f"option {progress_key} failed: {error_message}")
        log_error(progress_key, error_message)
        await asyncio.sleep(REQUEST_SLEEP_SECONDS)
        if not swallow_exceptions:
            raise
        return 0


async def backfill_history(headless=True, end_date=None):
    processed = load_progress()
    db_tools = DbTools()
    await db_tools.init_pool()

    end_trade_date = (
        datetime.strptime(end_date, "%Y-%m-%d").date()
        if end_date else datetime.now().date() - timedelta(days=1)
    )
    total_rows = 0

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=headless)
            page = await browser.new_page()
            try:
                for trade_date in iter_weekdays(BACKFILL_START_DATE, end_trade_date):
                    total_rows += await process_trade_day(
                        page,
                        db_tools,
                        normalize_date(trade_date),
                        processed=processed,
                        save_state=True,
                        swallow_exceptions=True,
                    )
            finally:
                await browser.close()
    finally:
        await db_tools.close()

    return total_rows


async def sync_daily(record_failures=False, headless=True, target_date=None):
    _ = record_failures
    trade_date = normalize_trade_date(target_date or normalize_date(datetime.now().date()))
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=headless)
            page = await browser.new_page()
            try:
                return await process_trade_day(
                    page,
                    db_tools,
                    trade_date,
                    processed=None,
                    save_state=False,
                    swallow_exceptions=False,
                )
            finally:
                await browser.close()
    finally:
        await db_tools.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "daily"

    if command == "backfill":
        end_date = sys.argv[2].strip() if len(sys.argv) > 2 else None
        total_rows = await backfill_history(headless=True, end_date=end_date)
        print(f"option backfill finished, inserted rows: {total_rows}")
        return

    if command == "daily":
        target_date = sys.argv[2].strip() if len(sys.argv) > 2 else None
        total_rows = await sync_daily(record_failures=False, headless=True, target_date=target_date)
        print(f"option daily finished, inserted rows: {total_rows}")
        return

    raise ValueError("supported commands: backfill [end_date], daily [target_date]")


if __name__ == "__main__":
    asyncio.run(main())
