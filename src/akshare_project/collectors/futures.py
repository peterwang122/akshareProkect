import asyncio
import csv
import io
import json
import re
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from urllib.parse import urljoin

import akshare as ak
import requests

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MARKET = "CFFEX"
PERIOD = "daily"
BACKFILL_START_DATE = date(2010, 4, 16)
CHUNK_DAYS = 90
LOGGER = get_logger("futures")
PROGRESS_STORE = ProgressStore("futures")
HTTP_TIMEOUT_SECONDS = 30
HTTP_RETRY_COUNT = 3
HTTP_RETRY_SLEEP_SECONDS = 2
SINA_GLOBAL_FUTURES_SOURCE = "sina_global_futures"
CME_SETTLEMENTS_SOURCE = "cme_settlements"
HKEX_SOURCE = "hkex_daily_market_report"
HK_TRADING_CALENDAR_SYMBOL = "HSI"
HK_BACKFILL_PROGRESS_EVERY = 25
SINA_GLOBAL_FUTURES_DAILY_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
    "{callback}/GlobalFuturesService.getGlobalFuturesDailyKLine"
)
CME_SETTLEMENTS_URL = "https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/{product_id}/FUT"
HKEX_DAYRPT_BASE_URL = "https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/"
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}
CME_BROWSER_HEADERS = {
    "User-Agent": HTTP_HEADERS["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": HTTP_HEADERS["Accept-Language"],
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "X-Requested-With": "XMLHttpRequest",
}
CME_BLOCKED_STATUS_CODES = {403, 429}


COL_DATE = "\u65e5\u671f"
COL_OPEN = "\u5f00\u76d8\u4ef7"
COL_HIGH = "\u6700\u9ad8\u4ef7"
COL_LOW = "\u6700\u4f4e\u4ef7"
COL_CLOSE = "\u6536\u76d8\u4ef7"
COL_VOLUME = "\u6210\u4ea4\u91cf"
COL_TURNOVER = "\u6210\u4ea4\u989d"
COL_OPEN_INTEREST = "\u6301\u4ed3\u91cf"

CONTINUOUS_SYMBOLS = {
    "ICM": {"name": "\u4e2d\u8bc1500\u80a1\u6307\u4e3b\u8fde", "variety": "IC"},
    "ICM0": {"name": "\u4e2d\u8bc1500\u80a1\u6307\u5f53\u6708\u8fde\u7eed", "variety": "IC"},
    "IFM": {"name": "\u6caa\u6df1\u4e3b\u8fde", "variety": "IF"},
    "IFM0": {"name": "\u6caa\u6df1\u5f53\u6708\u8fde\u7eed", "variety": "IF"},
    "IHM": {"name": "\u4e0a\u8bc1\u4e3b\u8fde", "variety": "IH"},
    "IHM0": {"name": "\u4e0a\u8bc1\u5f53\u6708\u8fde\u7eed", "variety": "IH"},
    "IMM": {"name": "\u4e2d\u8bc11000\u80a1\u6307\u4e3b\u8fde", "variety": "IM"},
    "IMM0": {"name": "\u4e2d\u8bc11000\u80a1\u6307\u5f53\u6708\u8fde\u7eed", "variety": "IM"},
}
DERIVED_CONTINUOUS_SYMBOLS = {
    "IF": {"main": "IFM", "month": "IFM0"},
    "IC": {"main": "ICM", "month": "ICM0"},
    "IH": {"main": "IHM", "month": "IHM0"},
    "IM": {"main": "IMM", "month": "IMM0"},
}
US_INDEX_FUTURES_PRODUCTS = {
    "ES": {
        "contract_name": "S&P 500 Index Futures Continuous",
        "exchange": "SINA",
        "start_date": date(1997, 9, 9),
    },
    "NQ": {
        "contract_name": "Nasdaq 100 Index Futures Continuous",
        "exchange": "SINA",
        "start_date": date(1999, 6, 21),
    },
}
US_INDEX_FUTURES_OFFICIAL_PRODUCTS = {
    "ES": {
        "product_id": "138",
        "contract_name": "E-mini S&P 500 Futures",
        "exchange": "CME",
        "start_date": date(1997, 9, 9),
        "referer": "https://www.cmegroup.com/markets/equities/sp/e-mini-sandp500.settlements.html",
    },
    "NQ": {
        "product_id": "146",
        "contract_name": "E-mini Nasdaq-100 Futures",
        "exchange": "CME",
        "start_date": date(1999, 6, 21),
        "referer": "https://www.cmegroup.com/markets/equities/nasdaq/e-mini-nasdaq-100.settlements.html",
    },
}
HK_INDEX_FUTURES_PRODUCTS = {
    "HSI": {
        "contract_name": "Hang Seng Index Futures",
        "exchange": "HKEX",
        "latest_page": "dmreport1.htm",
        "zip_prefix": "hsif",
        "start_date": date(1986, 1, 1),
    },
    "HHI": {
        "contract_name": "Hang Seng China Enterprises Index Futures",
        "exchange": "HKEX",
        "latest_page": "dmreport3.htm",
        "zip_prefix": "hhif",
        "start_date": date(2003, 12, 8),
    },
    "HTI": {
        "contract_name": "Hang Seng TECH Index Futures",
        "exchange": "HKEX",
        "latest_page": "dmreport4.htm",
        "zip_prefix": "htif",
        "start_date": date(2020, 11, 23),
    },
}
MONTH_CODE_BY_NUMBER = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",
}
MONTH_NUMBER_BY_ABBR = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def log_progress(task_name, start_date, end_date, inserted_rows):
    PROGRESS_STORE.append(f"{task_name},{start_date},{end_date},{inserted_rows}")


def log_error(task_name, start_date, end_date, error_message):
    LOGGER.error("%s,%s,%s,%s", task_name, start_date, end_date, error_message)


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    return shared_fetch_with_retry(
        func,
        *args,
        retries=retries,
        sleep_seconds=sleep_seconds,
        logger=LOGGER,
        caller_name=LOGGER.name,
        **kwargs,
    )


def normalize_trade_date(value):
    if value is None:
        return ""

    text = str(value).split(" ")[0].strip()
    if not text:
        return ""

    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text

    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return text


def normalize_symbol(symbol):
    return str(symbol or "").strip().upper()


def normalize_text(value):
    return str(value or "").strip()


def normalize_number_text(value):
    text = normalize_text(value)
    if not text or text in {"-", "--", "N/A", "NA"}:
        return None
    text = text.replace(",", "").replace("+", "")
    try:
        number = float(text)
    except ValueError:
        return None
    return number if number != 0 else None


def parse_signed_number(value):
    text = normalize_text(value)
    if not text or text in {"-", "--", "N/A", "NA"}:
        return None
    text = text.replace(",", "").replace("+", "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_month_contract(root_symbol, contract_month_text):
    text = normalize_text(contract_month_text).upper()
    if not text or "TOTAL" in text or "SUMMARY" in text:
        return None

    match = re.search(r"([A-Z]{3})[\s-]*(\d{2,4})", text)
    if not match:
        return None

    month_number = MONTH_NUMBER_BY_ABBR.get(match.group(1))
    if not month_number:
        return None

    year_value = int(match.group(2))
    year = 2000 + year_value if year_value < 100 else year_value
    if year < 1900 or year > 2100:
        return None

    month_code = MONTH_CODE_BY_NUMBER[month_number]
    yy = str(year)[-2:]
    source_contract_code = f"{normalize_symbol(root_symbol)}{month_code}{yy}"
    return {
        "source_contract_code": source_contract_code,
        "contract_month": f"{year:04d}-{month_number:02d}",
        "month_code": month_code,
        "year": year,
        "month": month_number,
    }


def parse_contract_year_month(symbol, variety):
    normalized_symbol = normalize_symbol(symbol)
    normalized_variety = normalize_symbol(variety)
    match = re.fullmatch(rf"{re.escape(normalized_variety)}(\d{{4}})", normalized_symbol)
    if not match:
        return None

    yy_mm = match.group(1)
    year = 2000 + int(yy_mm[:2])
    month = int(yy_mm[2:])
    if month < 1 or month > 12:
        return None
    return year, month, yy_mm


def parse_date_arg(value, default_date):
    if value is None:
        return default_date

    text = str(value).strip()
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value}")


def is_date_arg(value):
    if value is None:
        return False

    text = str(value).strip()
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) or re.fullmatch(r"\d{8}", text))


def select_roots(symbols, definitions):
    if not symbols:
        return list(definitions.keys())

    selected = []
    for symbol in symbols:
        root = normalize_symbol(symbol)
        if root in definitions:
            selected.append(root)
    return list(dict.fromkeys(selected))


def parse_range_and_symbols(args, default_start, default_end):
    remaining = list(args or [])
    start_date = default_start
    end_date = default_end

    if remaining and is_date_arg(remaining[0]):
        start_date = parse_date_arg(remaining.pop(0), default_start)
        if remaining and is_date_arg(remaining[0]):
            end_date = parse_date_arg(remaining.pop(0), default_end)
        else:
            end_date = start_date

    if start_date > end_date:
        raise ValueError("start_date cannot be greater than end_date")

    return start_date, end_date, remaining


def http_get(url, *, params=None, headers=None, timeout=HTTP_TIMEOUT_SECONDS):
    merged_headers = {**HTTP_HEADERS, **(headers or {})}
    last_error = None
    for attempt in range(1, HTTP_RETRY_COUNT + 1):
        try:
            response = requests.get(url, params=params, headers=merged_headers, timeout=timeout)
            if response.status_code == 404:
                return response
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt < HTTP_RETRY_COUNT:
                time.sleep(HTTP_RETRY_SLEEP_SECONDS * attempt)
    raise last_error


def create_cme_session(product):
    session = requests.Session()
    session.headers.update(
        {
            **HTTP_HEADERS,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.cmegroup.com/",
            "Connection": "keep-alive",
        }
    )
    try:
        response = session.get(product["referer"], timeout=HTTP_TIMEOUT_SECONDS)
        if response.status_code in CME_BLOCKED_STATUS_CODES:
            LOGGER.warning("CME settlement page warmup returned HTTP %s for %s", response.status_code, product["referer"])
    except requests.RequestException as exc:
        LOGGER.warning("CME settlement page warmup failed for %s: %s", product["referer"], exc)
    return session


def cme_get_json(session, endpoint, product, params):
    headers = {
        **CME_BROWSER_HEADERS,
        "Referer": product["referer"],
        "Origin": "https://www.cmegroup.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    last_error = None
    for attempt in range(1, HTTP_RETRY_COUNT + 1):
        try:
            response = session.get(endpoint, params=params, headers=headers, timeout=HTTP_TIMEOUT_SECONDS)
            if response.status_code == 404:
                return {}
            if response.status_code in CME_BLOCKED_STATUS_CODES and attempt < HTTP_RETRY_COUNT:
                LOGGER.warning(
                    "CME settlements API returned HTTP %s, warming session and retrying (%s/%s)",
                    response.status_code,
                    attempt,
                    HTTP_RETRY_COUNT,
                )
                try:
                    session.get(product["referer"], headers={**HTTP_HEADERS, "Referer": "https://www.cmegroup.com/"}, timeout=HTTP_TIMEOUT_SECONDS)
                except requests.RequestException:
                    pass
                time.sleep(HTTP_RETRY_SLEEP_SECONDS * attempt)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt < HTTP_RETRY_COUNT:
                time.sleep(HTTP_RETRY_SLEEP_SECONDS * attempt)
                continue
        except ValueError as exc:
            raise RuntimeError(f"CME settlements returned non-JSON response for {endpoint}") from exc
    if last_error is not None:
        response = getattr(last_error, "response", None)
        if response is not None and response.status_code in CME_BLOCKED_STATUS_CODES:
            raise RuntimeError(
                "CME settlements official API blocked the request "
                f"(HTTP {response.status_code}); retry later or run from a network allowed by CME."
            ) from last_error
        raise last_error
    raise RuntimeError("CME settlements request failed without response")


def is_http_status_error(exc, status_codes):
    response = getattr(exc, "response", None)
    return response is not None and response.status_code in status_codes


def parse_hkex_report_date(value):
    text = normalize_text(value).upper()
    match = re.search(r"(\d{1,2}\s+[A-Z]{3}\s+\d{4})", text)
    if not match:
        return ""
    try:
        return datetime.strptime(match.group(1), "%d %b %Y").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def row_first(row, keys):
    for key in keys:
        if key in row:
            value = row.get(key)
            if value not in (None, ""):
                return value
    return None


def select_hist_symbols(symbols=None):
    if not symbols:
        return list(CONTINUOUS_SYMBOLS.keys())

    selected = []
    for symbol in symbols:
        normalized = normalize_symbol(symbol)
        if normalized in CONTINUOUS_SYMBOLS:
            selected.append(normalized)
    return list(dict.fromkeys(selected))


def build_market_date_ranges(start_date, end_date, chunk_days=CHUNK_DAYS):
    ranges = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        ranges.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return ranges


def iter_weekdays(start_date, end_date):
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def format_duration(seconds):
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def fetch_hk_trading_calendar_dates():
    return fetch_with_retry(
        ak.stock_hk_index_daily_sina,
        symbol=HK_TRADING_CALENDAR_SYMBOL,
        retries=3,
        sleep_seconds=2,
        request_key=f"stock_hk_index_daily_sina:{HK_TRADING_CALENDAR_SYMBOL}:calendar",
    )


def build_hk_trading_dates(start_date, end_date):
    weekday_dates = list(iter_weekdays(start_date, end_date))
    if not weekday_dates:
        return [], "weekday", 0, 0

    try:
        calendar_df = fetch_hk_trading_calendar_dates()
    except Exception as exc:
        print(f"hk trading calendar fetch failed, fallback to weekdays: {exc}")
        return weekday_dates, "weekday", len(weekday_dates), 0

    calendar_dates = []
    if calendar_df is not None and not calendar_df.empty and "date" in calendar_df.columns:
        for value in calendar_df["date"].tolist():
            normalized = normalize_trade_date(value)
            if not normalized:
                continue
            parsed = parse_date_arg(normalized, start_date)
            if start_date <= parsed <= end_date:
                calendar_dates.append(parsed)

    calendar_dates = sorted(set(calendar_dates))
    if not calendar_dates:
        print("hk trading calendar returned no usable dates, fallback to weekdays")
        return weekday_dates, "weekday", len(weekday_dates), 0

    calendar_min = calendar_dates[0]
    calendar_max = calendar_dates[-1]
    calendar_set = set(calendar_dates)
    merged_dates = []
    fallback_count = 0
    calendar_count = 0
    for current in weekday_dates:
        if calendar_min <= current <= calendar_max:
            if current in calendar_set:
                merged_dates.append(current)
                calendar_count += 1
        else:
            merged_dates.append(current)
            fallback_count += 1

    source = (
        f"{HK_TRADING_CALENDAR_SYMBOL} calendar "
        f"{calendar_min}..{calendar_max} + weekday fallback"
    )
    return merged_dates, source, fallback_count, calendar_count


def build_hk_index_futures_backfill_work_items(selected_roots, trade_dates):
    work_items = []
    for trade_date in trade_dates:
        for root_symbol in selected_roots:
            if trade_date < HK_INDEX_FUTURES_PRODUCTS[root_symbol]["start_date"]:
                continue
            work_items.append((root_symbol, trade_date))
    return work_items


def should_print_hk_backfill_progress(processed_count, total_count, daily_count):
    return (
        processed_count == 1
        or processed_count == total_count
        or daily_count > 0
        or processed_count % HK_BACKFILL_PROGRESS_EVERY == 0
    )


def print_hk_backfill_progress(processed_count, total_count, root_symbol, trade_date, start_ts, total_rows, saved_days, empty_days, failed_days):
    elapsed_seconds = time.time() - start_ts
    progress_pct = (processed_count / total_count * 100) if total_count else 100.0
    eta_seconds = (elapsed_seconds / processed_count * (total_count - processed_count)) if processed_count else 0
    print(
        "hk index futures backfill progress: "
        f"{processed_count}/{total_count} ({progress_pct:.2f}%), "
        f"root={root_symbol}, trade_date={trade_date}, "
        f"elapsed={format_duration(elapsed_seconds)}, eta={format_duration(eta_seconds)}, "
        f"rows={total_rows}, saved_days={saved_days}, empty_days={empty_days}, failed_days={failed_days}"
    )


def get_market_daily_range(start_date, end_date):
    return fetch_with_retry(
        ak.get_futures_daily,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        market=MARKET,
    )


def build_market_rows(df):
    rows = []
    for _, row in df.iterrows():
        trade_date = normalize_trade_date(row.get("date"))
        symbol = normalize_symbol(row.get("symbol"))
        if not symbol or not trade_date:
            continue

        rows.append({
            "market": MARKET,
            "symbol": symbol,
            "variety": str(row.get("variety", "")).strip().upper() or None,
            "trade_date": trade_date,
            "open_price": row.get("open"),
            "high_price": row.get("high"),
            "low_price": row.get("low"),
            "close_price": row.get("close"),
            "volume": row.get("volume"),
            "open_interest": row.get("open_interest"),
            "turnover": row.get("turnover"),
            "settle_price": row.get("settle"),
            "pre_settle_price": row.get("pre_settle"),
            "data_source": "get_futures_daily",
        })
    return rows


def build_main_contract_sort_key(row):
    contract_year_month = parse_contract_year_month(row.get("symbol"), row.get("variety"))
    year = contract_year_month[0] if contract_year_month else 9999
    month = contract_year_month[1] if contract_year_month else 99
    volume = row.get("volume")
    open_interest = row.get("open_interest")
    return (
        -(float(volume) if volume is not None else -1.0),
        -(float(open_interest) if open_interest is not None else -1.0),
        year,
        month,
        row.get("symbol") or "",
    )


def build_month_contract_sort_key(row):
    contract_year_month = parse_contract_year_month(row.get("symbol"), row.get("variety"))
    year = contract_year_month[0] if contract_year_month else 9999
    month = contract_year_month[1] if contract_year_month else 99
    return (
        year,
        month,
        row.get("symbol") or "",
    )


def build_derived_row(source_row, derived_symbol):
    return {
        **source_row,
        "symbol": derived_symbol,
        "data_source": "get_futures_daily_derived",
    }


def build_derived_rows(market_rows):
    grouped_rows = {}
    for row in market_rows:
        variety = normalize_symbol(row.get("variety"))
        if variety not in DERIVED_CONTINUOUS_SYMBOLS:
            continue
        if not parse_contract_year_month(row.get("symbol"), variety):
            continue
        trade_date = normalize_trade_date(row.get("trade_date"))
        if not trade_date:
            continue
        grouped_rows.setdefault((trade_date, variety), []).append(row)

    derived_rows = []
    for (_, variety), rows in grouped_rows.items():
        if not rows:
            continue

        symbols = DERIVED_CONTINUOUS_SYMBOLS[variety]
        main_row = sorted(rows, key=build_main_contract_sort_key)[0]
        month_row = sorted(rows, key=build_month_contract_sort_key)[0]
        derived_rows.append(build_derived_row(main_row, symbols["main"]))
        derived_rows.append(build_derived_row(month_row, symbols["month"]))

    return derived_rows


def get_hist_daily_range(symbol, start_date, end_date):
    symbol_meta = CONTINUOUS_SYMBOLS[symbol]
    return fetch_with_retry(
        ak.futures_hist_em,
        symbol=symbol_meta["name"],
        period=PERIOD,
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
    )


def get_row_value(row, aliases, position=None):
    for alias in aliases:
        if alias in row.index:
            return row.get(alias)
    if position is not None and len(row) > position:
        return row.iloc[position]
    return None


def build_hist_rows(symbol, symbol_meta, df):
    rows = []
    for _, row in df.iterrows():
        trade_date = normalize_trade_date(
            get_row_value(row, [COL_DATE, "时间"], position=0)
        )
        if not trade_date:
            continue

        rows.append({
            "market": MARKET,
            "symbol": symbol,
            "variety": symbol_meta["variety"],
            "trade_date": trade_date,
            "open_price": get_row_value(row, [COL_OPEN, "开盘"], position=1),
            "high_price": get_row_value(row, [COL_HIGH, "最高"], position=2),
            "low_price": get_row_value(row, [COL_LOW, "最低"], position=3),
            "close_price": get_row_value(row, [COL_CLOSE, "收盘"], position=4),
            "volume": get_row_value(row, [COL_VOLUME, "成交量"], position=7),
            "open_interest": get_row_value(row, [COL_OPEN_INTEREST, "持仓量"], position=9),
            "turnover": get_row_value(row, [COL_TURNOVER, "成交额"], position=8),
            "settle_price": None,
            "pre_settle_price": None,
            "data_source": "futures_hist_em",
        })
    return rows


def fetch_sina_global_futures_history(root_symbol):
    today = datetime.now().date()
    today_marker = f"{today.year}_{today.month}_{today.day}"
    callback = f"var%20_S{today_marker}="
    params = {
        "symbol": root_symbol,
        "_": today_marker,
        "source": "web",
    }
    headers = {
        "Referer": "https://finance.sina.com.cn/money/future/hf.html",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    response = http_get(
        SINA_GLOBAL_FUTURES_DAILY_URL.format(callback=callback),
        params=params,
        headers=headers,
    )
    data_text = response.text
    json_start = data_text.find("[")
    json_end = data_text.rfind("]")
    if json_start < 0 or json_end < json_start:
        raise RuntimeError(f"Sina global futures returned no JSON array for {root_symbol}")

    rows = json.loads(data_text[json_start:json_end + 1])
    if not isinstance(rows, list):
        raise RuntimeError(f"Sina global futures returned unexpected payload for {root_symbol}")
    return rows


def format_cme_trade_date(trade_date):
    parsed_trade_date = parse_date_arg(trade_date, datetime.now().date())
    return parsed_trade_date.strftime("%m/%d/%Y")


def normalize_cme_number_text(value):
    text = normalize_text(value)
    if not text or text in {"-", "--", "N/A", "NA"}:
        return None
    text = text.replace(",", "").replace("+", "").strip()
    # CME sometimes appends settlement markers. Keep the numeric portion only.
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        number = float(match.group(0))
    except ValueError:
        return None
    return number


def normalize_cme_change_text(value):
    text = normalize_text(value).upper()
    if not text:
        return None
    if text in {"UNCH", "UNCHANGED"}:
        return 0.0
    return normalize_cme_number_text(text)


def extract_cme_settlement_rows(payload):
    if not isinstance(payload, dict):
        return []
    for key in ("settlements", "settlement", "data", "rows"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return rows
    return []


def fetch_cme_us_index_futures_settlements(root_symbol, trade_date):
    product = US_INDEX_FUTURES_OFFICIAL_PRODUCTS[root_symbol]
    trade_date_text = format_cme_trade_date(trade_date)
    endpoint = CME_SETTLEMENTS_URL.format(product_id=product["product_id"])
    params = {
        "tradeDate": trade_date_text,
        "strategy": "DEFAULT",
        "pageSize": 500,
    }
    session = create_cme_session(product)

    all_rows = []
    page_number = 1
    total_pages = 1
    while page_number <= total_pages:
        page_params = {**params, "pageNumber": page_number}
        payload = cme_get_json(session, endpoint, product, page_params)
        if not payload:
            break
        rows = extract_cme_settlement_rows(payload)
        all_rows.extend(row for row in rows if isinstance(row, dict))

        total_pages_value = payload.get("totalPages") or payload.get("total_pages")
        try:
            total_pages = max(1, int(total_pages_value or 1))
        except (TypeError, ValueError):
            total_pages = 1
        page_number += 1

    return all_rows


def build_cme_us_index_futures_rows(root_symbol, trade_date, rows):
    product = US_INDEX_FUTURES_OFFICIAL_PRODUCTS[root_symbol]
    trade_date_text = parse_date_arg(trade_date, datetime.now().date()).strftime("%Y-%m-%d")
    contract_rows = []
    daily_rows = []
    seen_contracts = set()

    for row in rows:
        month_text = row_first(
            row,
            [
                "month",
                "expirationMonth",
                "expiration_month",
                "contractMonth",
                "contract_month",
            ],
        )
        month_text = normalize_text(month_text)
        if not month_text or month_text.upper() in {"TOTAL", "SUMMARY"}:
            continue

        contract_meta = parse_month_contract(root_symbol, month_text)
        if not contract_meta:
            continue

        source_contract_code = contract_meta["source_contract_code"]
        contract_name = f"{product['contract_name']} {contract_meta['month_code']}{str(contract_meta['year'])[-2:]}"
        contract_row = {
            "root_symbol": root_symbol,
            "source_contract_code": source_contract_code,
            "contract_name": contract_name,
            "contract_month": contract_meta["contract_month"],
            "exchange": product["exchange"],
            "data_source": CME_SETTLEMENTS_SOURCE,
            "first_seen_trade_date": trade_date_text,
            "last_seen_trade_date": trade_date_text,
        }

        last_price = normalize_cme_number_text(row_first(row, ["last", "lastPrice", "last_price"]))
        settle_price = normalize_cme_number_text(row_first(row, ["settle", "settlement", "settlePrice", "settle_price"]))
        close_price = settle_price if settle_price is not None else last_price
        daily_row = {
            **contract_row,
            "trade_date": trade_date_text,
            "open_price": normalize_cme_number_text(row_first(row, ["open", "openPrice", "open_price"])),
            "high_price": normalize_cme_number_text(row_first(row, ["high", "highPrice", "high_price"])),
            "low_price": normalize_cme_number_text(row_first(row, ["low", "lowPrice", "low_price"])),
            "last_price": last_price,
            "close_price": close_price,
            "settle_price": settle_price,
            "price_change": normalize_cme_change_text(row_first(row, ["change", "priceChange", "price_change"])),
            "volume": normalize_cme_number_text(row_first(row, ["volume", "tradeVolume", "trade_volume"])),
            "open_interest": normalize_cme_number_text(
                row_first(row, ["openInterest", "open_interest", "openInterestQty"])
            ),
            "raw_payload_json": row,
            "data_source": CME_SETTLEMENTS_SOURCE,
        }
        if not any(daily_row.get(key) is not None for key in ("close_price", "settle_price", "last_price")):
            continue

        if source_contract_code not in seen_contracts:
            contract_rows.append(contract_row)
            seen_contracts.add(source_contract_code)
        daily_rows.append(daily_row)

    return contract_rows, daily_rows


def build_sina_us_index_futures_rows(root_symbol, rows, start_date=None, end_date=None, latest_only=False):
    product = US_INDEX_FUTURES_PRODUCTS[root_symbol]
    daily_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        trade_date = normalize_trade_date(row_first(row, ["date", "trade_date"]))
        if not trade_date:
            continue
        parsed_trade_date = parse_date_arg(trade_date, datetime.now().date())
        if start_date and parsed_trade_date < start_date:
            continue
        if end_date and parsed_trade_date > end_date:
            continue

        daily_row = {
            "root_symbol": root_symbol,
            "source_contract_code": root_symbol,
            "contract_name": product["contract_name"],
            "contract_month": "CONTINUOUS",
            "exchange": product["exchange"],
            "data_source": SINA_GLOBAL_FUTURES_SOURCE,
            "first_seen_trade_date": trade_date,
            "last_seen_trade_date": trade_date,
            "trade_date": trade_date,
            "open_price": normalize_number_text(row_first(row, ["open"])),
            "high_price": normalize_number_text(row_first(row, ["high"])),
            "low_price": normalize_number_text(row_first(row, ["low"])),
            "close_price": normalize_number_text(row_first(row, ["close"])),
            "closing_range_raw": None,
            "volume": normalize_number_text(row_first(row, ["volume"])),
            "open_interest": normalize_number_text(row_first(row, ["position", "open_interest"])),
            "settle_price": normalize_number_text(row_first(row, ["settlement", "settle"])),
            "pre_settle_price": None,
        }
        if not any(daily_row.get(key) is not None for key in ("open_price", "high_price", "low_price", "close_price")):
            continue
        daily_rows.append(daily_row)

    daily_rows.sort(key=lambda item: item["trade_date"])
    if latest_only and daily_rows:
        daily_rows = [daily_rows[-1]]
    if not daily_rows:
        return [], []

    first_trade_date = daily_rows[0]["trade_date"]
    last_trade_date = daily_rows[-1]["trade_date"]
    contract_row = {
        "root_symbol": root_symbol,
        "source_contract_code": root_symbol,
        "contract_name": product["contract_name"],
        "contract_month": "CONTINUOUS",
        "exchange": product["exchange"],
        "data_source": SINA_GLOBAL_FUTURES_SOURCE,
        "first_seen_trade_date": first_trade_date,
        "last_seen_trade_date": last_trade_date,
    }
    for daily_row in daily_rows:
        daily_row["first_seen_trade_date"] = first_trade_date
        daily_row["last_seen_trade_date"] = last_trade_date
    return [contract_row], daily_rows


def fetch_hkex_latest_zip(root_symbol):
    product = HK_INDEX_FUTURES_PRODUCTS[root_symbol]
    page_url = urljoin(HKEX_DAYRPT_BASE_URL, product["latest_page"])
    response = http_get(page_url, headers={"Referer": "https://www.hkex.com.hk/"})
    if response.status_code == 404:
        return None

    match = re.search(r'href=["\']([^"\']+\.zip)["\']', response.text, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError(f"HKEX latest zip link not found for {root_symbol}")
    zip_url = urljoin(page_url, match.group(1))
    zip_response = http_get(zip_url, headers={"Referer": page_url})
    if zip_response.status_code == 404:
        return None
    return zip_response.content


def fetch_hkex_archive_zip(root_symbol, trade_date):
    product = HK_INDEX_FUTURES_PRODUCTS[root_symbol]
    yymmdd = trade_date.strftime("%y%m%d")
    zip_url = urljoin(HKEX_DAYRPT_BASE_URL, f"{product['zip_prefix']}{yymmdd}.zip")
    response = http_get(zip_url, headers={"Referer": "https://www.hkex.com.hk/"})
    if response.status_code == 404:
        return None
    return response.content


def read_hkex_zip_csv_rows(zip_content):
    if not zip_content:
        return []
    with zipfile.ZipFile(io.BytesIO(zip_content)) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            return []
        raw = archive.read(csv_names[0])

    for encoding in ("utf-8-sig", "big5", "latin1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            text = ""
    if not text:
        return []
    return list(csv.reader(io.StringIO(text)))


def build_hkex_index_futures_rows(root_symbol, csv_rows):
    product = HK_INDEX_FUTURES_PRODUCTS[root_symbol]
    trade_date = ""
    header_index = None
    for idx, row in enumerate(csv_rows):
        row_text = " ".join(normalize_text(cell) for cell in row)
        if "TRADING DAY OF THE EXCHANGE" in row_text.upper():
            for next_row in csv_rows[idx + 1: idx + 4]:
                candidates = [parse_hkex_report_date(cell) for cell in next_row]
                candidates = [candidate for candidate in candidates if candidate]
                if candidates:
                    trade_date = candidates[-1]
                    break
        if row and normalize_text(row[0]).upper() == "CONTRACT MONTH":
            header_index = idx
            break

    if not trade_date or header_index is None:
        return [], []

    contract_rows = []
    daily_rows = []
    for row in csv_rows[header_index + 1:]:
        if not row or not normalize_text(row[0]):
            if contract_rows:
                break
            continue

        contract_month_text = normalize_text(row[0])
        row_text = " ".join(normalize_text(cell) for cell in row).upper()
        if any(marker in row_text for marker in ("STRATEGY", "SPREAD", "TOTAL", "TAILOR")):
            break

        contract_meta = parse_month_contract(root_symbol, contract_month_text)
        if not contract_meta:
            continue

        source_contract_code = contract_meta["source_contract_code"]
        contract_name = f"{product['contract_name']} {contract_meta['month_code']}{str(contract_meta['year'])[-2:]}"
        contract_row = {
            "root_symbol": root_symbol,
            "source_contract_code": source_contract_code,
            "contract_name": contract_name,
            "contract_month": contract_meta["contract_month"],
            "exchange": product["exchange"],
            "data_source": HKEX_SOURCE,
            "first_seen_trade_date": trade_date,
            "last_seen_trade_date": trade_date,
        }

        day_open = normalize_number_text(row[6] if len(row) > 6 else None)
        day_high = normalize_number_text(row[7] if len(row) > 7 else None)
        day_low = normalize_number_text(row[8] if len(row) > 8 else None)
        aht_open = normalize_number_text(row[1] if len(row) > 1 else None)
        aht_high = normalize_number_text(row[2] if len(row) > 2 else None)
        aht_low = normalize_number_text(row[3] if len(row) > 3 else None)
        contract_high = normalize_number_text(row[12] if len(row) > 12 else None)
        contract_low = normalize_number_text(row[13] if len(row) > 13 else None)
        settle_price = normalize_number_text(row[10] if len(row) > 10 else None)

        high_candidates = [value for value in (contract_high, day_high, aht_high) if value is not None]
        low_candidates = [value for value in (contract_low, day_low, aht_low) if value is not None]
        daily_row = {
            **contract_row,
            "trade_date": trade_date,
            "open_price": day_open if day_open is not None else aht_open,
            "high_price": max(high_candidates) if high_candidates else None,
            "low_price": min(low_candidates) if low_candidates else None,
            "close_price": settle_price,
            "volume": normalize_number_text(row[14] if len(row) > 14 else None),
            "open_interest": normalize_number_text(row[15] if len(row) > 15 else None),
            "settle_price": settle_price,
            "pre_settle_price": None,
        }
        contract_rows.append(contract_row)
        daily_rows.append(daily_row)

    return contract_rows, daily_rows


async def ingest_market_range(db_tools, start_date, end_date):
    try:
        df = await asyncio.to_thread(get_market_daily_range, start_date, end_date)
        if df is None or df.empty:
            print(f"get_futures_daily {start_date} -> {end_date}: no data")
            log_progress("market", start_date, end_date, 0)
            return 0

        market_rows = build_market_rows(df)
        derived_rows = build_derived_rows(market_rows)
        inserted_market = await db_tools.batch_futures_daily_data(market_rows)
        inserted_derived = await db_tools.batch_futures_daily_data(derived_rows)
        inserted_total = inserted_market + inserted_derived

        log_progress("market", start_date, end_date, inserted_total)
        print(
            f"get_futures_daily {start_date} -> {end_date}: "
            f"market_inserted={inserted_market}, derived_inserted={inserted_derived}, total_inserted={inserted_total}"
        )
        return inserted_total
    except Exception as exc:
        error_message = str(exc)
        print(f"get_futures_daily {start_date} -> {end_date} failed: {error_message}")
        log_error("market", start_date, end_date, error_message)
        return 0


async def ingest_hist_symbol_range(db_tools, symbol, start_date, end_date):
    symbol_meta = CONTINUOUS_SYMBOLS[symbol]

    try:
        df = await asyncio.to_thread(get_hist_daily_range, symbol, start_date, end_date)
        if df is None or df.empty:
            print(f"futures_hist_em {symbol} {start_date} -> {end_date}: no data")
            log_progress(symbol, start_date, end_date, 0)
            return 0

        rows = build_hist_rows(symbol, symbol_meta, df)
        inserted = await db_tools.batch_futures_daily_data(rows)
        log_progress(symbol, start_date, end_date, inserted)
        print(f"futures_hist_em {symbol} {start_date} -> {end_date}: inserted {inserted}")
        return inserted
    except Exception as exc:
        error_message = str(exc)
        print(f"futures_hist_em {symbol} {start_date} -> {end_date} failed: {error_message}")
        log_error(symbol, start_date, end_date, error_message)
        return 0


async def sync_market_range(start_date, end_date):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        total_inserted = 0
        for range_start, range_end in build_market_date_ranges(start_date, end_date):
            total_inserted += await ingest_market_range(db_tools, range_start, range_end)

        print(
            "get_futures_daily sync finished, "
            f"start_date={start_date}, "
            f"end_date={end_date}, "
            f"inserted_rows={total_inserted}"
        )
        return total_inserted
    finally:
        await db_tools.close()


async def sync_hist_range(start_date, end_date, symbols=None):
    selected_symbols = select_hist_symbols(symbols)
    if not selected_symbols:
        print("No valid continuous futures symbols selected.")
        return 0

    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        total_inserted = 0
        for symbol in selected_symbols:
            total_inserted += await ingest_hist_symbol_range(db_tools, symbol, start_date, end_date)

        print(
            "futures_hist_em sync finished, "
            f"symbols={len(selected_symbols)}, "
            f"start_date={start_date}, "
            f"end_date={end_date}, "
            f"inserted_rows={total_inserted}"
        )
        return total_inserted
    finally:
        await db_tools.close()


async def backfill_market_history(start_date=None, end_date=None):
    actual_start = start_date or BACKFILL_START_DATE
    actual_end = end_date or datetime.now().date()
    return await sync_market_range(actual_start, actual_end)


async def sync_market_today(start_date=None, end_date=None):
    today = datetime.now().date()
    actual_start = start_date or today
    actual_end = end_date or actual_start
    return await sync_market_range(actual_start, actual_end)


async def backfill_hist_history(start_date=None, end_date=None, symbols=None):
    actual_start = start_date or BACKFILL_START_DATE
    actual_end = end_date or datetime.now().date()
    return await sync_hist_range(actual_start, actual_end, symbols=symbols)


async def sync_hist_today(start_date=None, end_date=None, symbols=None):
    today = datetime.now().date()
    actual_start = start_date or today
    actual_end = end_date or actual_start
    return await sync_hist_range(actual_start, actual_end, symbols=symbols)


async def backfill_history(start_date=None, end_date=None, symbols=None):
    _ = symbols
    actual_start = start_date or BACKFILL_START_DATE
    actual_end = end_date or datetime.now().date()
    total_inserted = await backfill_market_history(actual_start, actual_end)
    print(
        "futures backfill finished, "
        f"inserted_rows={total_inserted}"
    )
    return total_inserted


async def sync_today(start_date=None, end_date=None, symbols=None):
    _ = symbols
    today = datetime.now().date()
    actual_start = start_date or today
    actual_end = end_date or actual_start
    total_inserted = await sync_market_today(actual_start, actual_end)
    print(
        "futures daily finished, "
        f"inserted_rows={total_inserted}"
    )
    return total_inserted


async def sync_trade_date(trade_date):
    actual_trade_date = parse_date_arg(trade_date, datetime.now().date())
    return await sync_today(start_date=actual_trade_date, end_date=actual_trade_date)


async def persist_index_futures_rows(db_tools, contract_table, daily_table, contract_rows, daily_rows):
    contract_count = await db_tools.batch_index_futures_contract_info(contract_table, contract_rows)
    daily_count = await db_tools.batch_index_futures_daily_data(daily_table, daily_rows)
    return contract_count, daily_count


async def persist_us_index_official_futures_rows(db_tools, contract_rows, daily_rows):
    await db_tools.ensure_us_index_official_futures_tables()
    contract_count = await db_tools.batch_index_futures_contract_info(
        "futures_us_index_official_contract_info",
        contract_rows,
    )
    daily_count = await db_tools.batch_us_index_official_futures_daily_data(daily_rows)
    return contract_count, daily_count


async def sync_us_index_futures_daily(trade_date=None, roots=None):
    selected_roots = select_roots(roots, US_INDEX_FUTURES_PRODUCTS)
    if not selected_roots:
        print("No valid US index futures roots selected.")
        return 0
    target_date = parse_date_arg(trade_date, datetime.now().date()) if trade_date else None
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        total_contracts = 0
        total_rows = 0
        for root_symbol in selected_roots:
            try:
                history_rows = await asyncio.to_thread(fetch_sina_global_futures_history, root_symbol)
                contract_rows, daily_rows = build_sina_us_index_futures_rows(
                    root_symbol,
                    history_rows,
                    start_date=target_date,
                    end_date=target_date,
                    latest_only=target_date is None,
                )
                contract_count, daily_count = await persist_index_futures_rows(
                    db_tools,
                    "futures_us_index_contract_info",
                    "futures_us_index_daily_data",
                    contract_rows,
                    daily_rows,
                )
                total_contracts += contract_count
                total_rows += daily_count
                print(
                    f"us index futures {root_symbol} daily saved contracts={contract_count}, rows={daily_count}"
                )
            except Exception as exc:
                print(f"us index futures {root_symbol} daily failed: {exc}")
        print(f"us index futures daily finished, contracts={total_contracts}, rows={total_rows}")
        return total_rows
    finally:
        await db_tools.close()


async def backfill_us_index_futures(start_date=None, end_date=None, roots=None):
    selected_roots = select_roots(roots, US_INDEX_FUTURES_PRODUCTS)
    if not selected_roots:
        print("No valid US index futures roots selected.")
        return 0
    default_start = min(US_INDEX_FUTURES_PRODUCTS[root]["start_date"] for root in selected_roots)
    actual_start = start_date or default_start
    actual_end = end_date or datetime.now().date()
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        total_rows = 0
        for root_symbol in selected_roots:
            try:
                history_rows = await asyncio.to_thread(fetch_sina_global_futures_history, root_symbol)
                contract_rows, daily_rows = build_sina_us_index_futures_rows(
                    root_symbol,
                    history_rows,
                    start_date=actual_start,
                    end_date=actual_end,
                )
                _, daily_count = await persist_index_futures_rows(
                    db_tools,
                    "futures_us_index_contract_info",
                    "futures_us_index_daily_data",
                    contract_rows,
                    daily_rows,
                )
                total_rows += daily_count
                print(f"us index futures {root_symbol} backfill saved rows={daily_count}")
            except Exception as exc:
                print(f"us index futures {root_symbol} backfill failed: {exc}")
        print(
            "us index futures backfill finished, "
            f"roots={','.join(selected_roots)}, start_date={actual_start}, end_date={actual_end}, rows={total_rows}"
        )
        return total_rows
    finally:
        await db_tools.close()


async def sync_us_index_futures_official_daily(trade_date=None, roots=None):
    selected_roots = select_roots(roots, US_INDEX_FUTURES_OFFICIAL_PRODUCTS)
    if not selected_roots:
        print("No valid official US index futures roots selected.")
        return 0
    target_date = parse_date_arg(trade_date, datetime.now().date()) if trade_date else datetime.now().date()
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        total_contracts = 0
        total_rows = 0
        for root_symbol in selected_roots:
            rows = await asyncio.to_thread(fetch_cme_us_index_futures_settlements, root_symbol, target_date)
            contract_rows, daily_rows = build_cme_us_index_futures_rows(root_symbol, target_date, rows)
            contract_count, daily_count = await persist_us_index_official_futures_rows(
                db_tools,
                contract_rows,
                daily_rows,
            )
            total_contracts += contract_count
            total_rows += daily_count
            print(
                "official us index futures "
                f"{root_symbol} {target_date} saved contracts={contract_count}, rows={daily_count}"
            )

        if total_rows <= 0:
            raise RuntimeError(
                "CME official settlements returned no contract rows for "
                f"roots={','.join(selected_roots)}, trade_date={target_date}"
            )
        print(f"official us index futures daily finished, contracts={total_contracts}, rows={total_rows}")
        return total_rows
    finally:
        await db_tools.close()


async def backfill_us_index_futures_official(start_date=None, end_date=None, roots=None):
    selected_roots = select_roots(roots, US_INDEX_FUTURES_OFFICIAL_PRODUCTS)
    if not selected_roots:
        print("No valid official US index futures roots selected.")
        return 0
    default_start = min(US_INDEX_FUTURES_OFFICIAL_PRODUCTS[root]["start_date"] for root in selected_roots)
    actual_start = start_date or default_start
    actual_end = end_date or datetime.now().date()
    if actual_start > actual_end:
        raise ValueError("start_date cannot be greater than end_date")

    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        total_rows = 0
        saved_days = 0
        empty_days = 0
        failed_days = 0
        current_date = actual_start
        while current_date <= actual_end:
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            day_rows = 0
            for root_symbol in selected_roots:
                try:
                    rows = await asyncio.to_thread(fetch_cme_us_index_futures_settlements, root_symbol, current_date)
                    contract_rows, daily_rows = build_cme_us_index_futures_rows(root_symbol, current_date, rows)
                    _, daily_count = await persist_us_index_official_futures_rows(
                        db_tools,
                        contract_rows,
                        daily_rows,
                    )
                    day_rows += daily_count
                    total_rows += daily_count
                except Exception as exc:
                    failed_days += 1
                    print(f"official us index futures {root_symbol} {current_date} failed: {exc}")

            if day_rows:
                saved_days += 1
                print(f"official us index futures {current_date} saved rows={day_rows}")
            else:
                empty_days += 1

            current_date += timedelta(days=1)

        print(
            "official us index futures backfill finished, "
            f"roots={','.join(selected_roots)}, start_date={actual_start}, end_date={actual_end}, "
            f"rows={total_rows}, saved_days={saved_days}, empty_days={empty_days}, failed_days={failed_days}"
        )
        return total_rows
    finally:
        await db_tools.close()


async def sync_hk_index_futures_daily(trade_date=None, roots=None):
    selected_roots = select_roots(roots, HK_INDEX_FUTURES_PRODUCTS)
    if not selected_roots:
        print("No valid HK index futures roots selected.")
        return 0
    target_date = parse_date_arg(trade_date, datetime.now().date()) if trade_date else None
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        total_contracts = 0
        total_rows = 0
        for root_symbol in selected_roots:
            try:
                if target_date:
                    zip_content = await asyncio.to_thread(fetch_hkex_archive_zip, root_symbol, target_date)
                else:
                    zip_content = await asyncio.to_thread(fetch_hkex_latest_zip, root_symbol)
                csv_rows = read_hkex_zip_csv_rows(zip_content)
                contract_rows, daily_rows = build_hkex_index_futures_rows(root_symbol, csv_rows)
                contract_count, daily_count = await persist_index_futures_rows(
                    db_tools,
                    "futures_hk_index_contract_info",
                    "futures_hk_index_daily_data",
                    contract_rows,
                    daily_rows,
                )
                total_contracts += contract_count
                total_rows += daily_count
                print(
                    f"hk index futures {root_symbol} daily saved contracts={contract_count}, rows={daily_count}"
                )
            except Exception as exc:
                print(f"hk index futures {root_symbol} daily failed: {exc}")
        print(f"hk index futures daily finished, contracts={total_contracts}, rows={total_rows}")
        return total_rows
    finally:
        await db_tools.close()


async def backfill_hk_index_futures(start_date=None, end_date=None, roots=None):
    selected_roots = select_roots(roots, HK_INDEX_FUTURES_PRODUCTS)
    if not selected_roots:
        print("No valid HK index futures roots selected.")
        return 0
    default_start = min(HK_INDEX_FUTURES_PRODUCTS[root]["start_date"] for root in selected_roots)
    actual_start = start_date or default_start
    actual_end = end_date or datetime.now().date()
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        total_rows = 0
        trade_dates, calendar_source, fallback_count, calendar_count = await asyncio.to_thread(
            build_hk_trading_dates,
            actual_start,
            actual_end,
        )
        work_items = build_hk_index_futures_backfill_work_items(selected_roots, trade_dates)
        print(
            "hk index futures backfill prepared: "
            f"roots={','.join(selected_roots)}, start_date={actual_start}, end_date={actual_end}, "
            f"trade_dates={len(trade_dates)}, work_items={len(work_items)}, "
            f"calendar_source={calendar_source}, calendar_dates={calendar_count}, "
            f"weekday_fallback_dates={fallback_count}"
        )
        if not work_items:
            print(
                "hk index futures backfill finished, "
                f"roots={','.join(selected_roots)}, start_date={actual_start}, end_date={actual_end}, rows=0"
            )
            return 0

        start_ts = time.time()
        saved_days = 0
        empty_days = 0
        failed_days = 0
        total_items = len(work_items)
        for processed_count, (root_symbol, trade_date) in enumerate(work_items, start=1):
            daily_count = 0
            try:
                zip_content = await asyncio.to_thread(fetch_hkex_archive_zip, root_symbol, trade_date)
                if zip_content:
                    csv_rows = read_hkex_zip_csv_rows(zip_content)
                    contract_rows, daily_rows = build_hkex_index_futures_rows(root_symbol, csv_rows)
                    _, daily_count = await persist_index_futures_rows(
                        db_tools,
                        "futures_hk_index_contract_info",
                        "futures_hk_index_daily_data",
                        contract_rows,
                        daily_rows,
                    )
                    total_rows += daily_count
                    if daily_count:
                        saved_days += 1
                        print(f"hk index futures {root_symbol} {trade_date} saved rows={daily_count}")
                    else:
                        empty_days += 1
                else:
                    empty_days += 1
            except Exception as exc:
                if is_http_status_error(exc, {401, 403}):
                    raise
                failed_days += 1
                print(f"hk index futures {root_symbol} {trade_date} skipped: {exc}")

            if should_print_hk_backfill_progress(processed_count, total_items, daily_count):
                print_hk_backfill_progress(
                    processed_count,
                    total_items,
                    root_symbol,
                    trade_date,
                    start_ts,
                    total_rows,
                    saved_days,
                    empty_days,
                    failed_days,
                )
        print(
            "hk index futures backfill finished, "
            f"roots={','.join(selected_roots)}, start_date={actual_start}, end_date={actual_end}, "
            f"trade_dates={len(trade_dates)}, work_items={len(work_items)}, rows={total_rows}, "
            f"saved_days={saved_days}, empty_days={empty_days}, failed_days={failed_days}"
        )
        return total_rows
    finally:
        await db_tools.close()


async def main():
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "backfill"
    args = sys.argv[2:]
    default_end = datetime.now().date()

    if mode == "backfill":
        start_date, end_date, symbol_args = parse_range_and_symbols(args, BACKFILL_START_DATE, default_end)
        await backfill_history(start_date=start_date, end_date=end_date, symbols=symbol_args)
        return

    if mode == "daily":
        today = datetime.now().date()
        start_date, end_date, symbol_args = parse_range_and_symbols(args, today, today)
        await sync_today(start_date=start_date, end_date=end_date, symbols=symbol_args)
        return

    if mode == "trade-date":
        if not args:
            raise ValueError("usage: python run.py futures trade-date YYYY-MM-DD")
        await sync_trade_date(args[0])
        return

    if mode == "market-backfill":
        start_date, end_date, _ = parse_range_and_symbols(args, BACKFILL_START_DATE, default_end)
        await backfill_market_history(start_date=start_date, end_date=end_date)
        return

    if mode == "market-daily":
        today = datetime.now().date()
        start_date, end_date, _ = parse_range_and_symbols(args, today, today)
        await sync_market_today(start_date=start_date, end_date=end_date)
        return

    if mode == "hist-backfill":
        start_date, end_date, symbol_args = parse_range_and_symbols(args, BACKFILL_START_DATE, default_end)
        await backfill_hist_history(start_date=start_date, end_date=end_date, symbols=symbol_args)
        return

    if mode == "hist-daily":
        today = datetime.now().date()
        start_date, end_date, symbol_args = parse_range_and_symbols(args, today, today)
        await sync_hist_today(start_date=start_date, end_date=end_date, symbols=symbol_args)
        return

    if mode == "daily-us-index":
        today = datetime.now().date()
        start_date, end_date, symbol_args = parse_range_and_symbols(args, today, today)
        trade_date = start_date if args and is_date_arg(args[0]) and start_date == end_date else None
        await sync_us_index_futures_daily(trade_date=trade_date, roots=symbol_args)
        return

    if mode == "backfill-us-index":
        default_start = min(meta["start_date"] for meta in US_INDEX_FUTURES_PRODUCTS.values())
        start_date, end_date, symbol_args = parse_range_and_symbols(args, default_start, default_end)
        await backfill_us_index_futures(start_date=start_date, end_date=end_date, roots=symbol_args)
        return

    if mode == "daily-us-index-official":
        today = datetime.now().date()
        start_date, end_date, symbol_args = parse_range_and_symbols(args, today, today)
        trade_date = start_date if args and is_date_arg(args[0]) and start_date == end_date else None
        await sync_us_index_futures_official_daily(trade_date=trade_date, roots=symbol_args)
        return

    if mode == "backfill-us-index-official":
        default_start = min(meta["start_date"] for meta in US_INDEX_FUTURES_OFFICIAL_PRODUCTS.values())
        start_date, end_date, symbol_args = parse_range_and_symbols(args, default_start, default_end)
        await backfill_us_index_futures_official(start_date=start_date, end_date=end_date, roots=symbol_args)
        return

    if mode == "daily-hk-index":
        today = datetime.now().date()
        start_date, end_date, symbol_args = parse_range_and_symbols(args, today, today)
        trade_date = start_date if args and is_date_arg(args[0]) and start_date == end_date else None
        await sync_hk_index_futures_daily(trade_date=trade_date, roots=symbol_args)
        return

    if mode == "backfill-hk-index":
        default_start = min(meta["start_date"] for meta in HK_INDEX_FUTURES_PRODUCTS.values())
        start_date, end_date, symbol_args = parse_range_and_symbols(args, default_start, default_end)
        await backfill_hk_index_futures(start_date=start_date, end_date=end_date, roots=symbol_args)
        return

    raise ValueError(
        "usage: python run.py futures backfill [start_date] [end_date] [HIST_SYMBOL ...]\n"
        "   or: python run.py futures daily [trade_date|start_date end_date] [HIST_SYMBOL ...]\n"
        "   or: python run.py futures trade-date YYYY-MM-DD\n"
        "   or: python run.py futures market-backfill [start_date] [end_date]\n"
        "   or: python run.py futures market-daily [trade_date|start_date end_date]\n"
        "   or: python run.py futures hist-backfill [start_date] [end_date] [HIST_SYMBOL ...]\n"
        "   or: python run.py futures hist-daily [trade_date|start_date end_date] [HIST_SYMBOL ...]\n"
        "   or: python run.py futures daily-us-index [trade_date] [ES|NQ ...]\n"
        "   or: python run.py futures backfill-us-index [start_date] [end_date] [ES|NQ ...]\n"
        "   or: python run.py futures daily-us-index-official [trade_date] [ES|NQ ...]\n"
        "   or: python run.py futures backfill-us-index-official [start_date] [end_date] [ES|NQ ...]\n"
        "   or: python run.py futures daily-hk-index [trade_date] [HSI|HHI|HTI ...]\n"
        "   or: python run.py futures backfill-hk-index [start_date] [end_date] [HSI|HHI|HTI ...]"
    )


if __name__ == "__main__":
    asyncio.run(main())
