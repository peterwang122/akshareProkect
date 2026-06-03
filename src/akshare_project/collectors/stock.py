import asyncio
import json
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd
import requests

from akshare_project.core.ak_scheduler_client import SchedulerContext
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.paths import get_state_path
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
HISTORY_FALLBACK_START_DATE = date(1991, 1, 1)
MAX_HISTORY_CONCURRENCY = 8
STOCK_HIST_TX_REQUEST_KEY_VERSION = "v4"

LOGGER = get_logger("stock")
STOCK_INFO_SYNC_STATE_PATH = get_state_path("stock_info_all", suffix="daily-sync")

SH_SOURCES = ["主板A股", "科创板"]
SZ_SOURCES = ["A股列表"]
SH_ALLOWED_BOARDS = {"主板A股", "科创板"}
SZ_ALLOWED_BOARDS = {"主板", "创业板"}
OFFICIAL_EXCHANGE_REQUEST_INTERVAL_SECONDS = 2.0
SSE_OFFICIAL_DAILY_SOURCE = "sse_official_daily"
SZSE_OFFICIAL_DAILY_SOURCE = "szse_official_daily"
SSE_OFFICIAL_DAILY_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SSE_OFFICIAL_REFERER_URL = "https://www.sse.com.cn/assortment/stock/list/info/company/index.shtml"
SZSE_OFFICIAL_HISTORY_URL = "https://www.szse.cn/api/market/ssjjhq/getHistoryData"
SZSE_OFFICIAL_KEY_INDEX_URL = "https://www.szse.cn/api/report/index/stockKeyIndexGeneralization"
SZSE_OFFICIAL_REFERER_URL = "https://www.szse.cn/certificate/individual/index.html"
OFFICIAL_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def fetch_with_retry(
    func,
    *args,
    retries=API_RETRY_COUNT,
    sleep_seconds=API_RETRY_SLEEP_SECONDS,
    scheduler_context=None,
    return_scheduler_meta=False,
    request_key=None,
    **kwargs,
):
    return shared_fetch_with_retry(
        func,
        *args,
        retries=retries,
        sleep_seconds=sleep_seconds,
        logger=LOGGER,
        scheduler_context=scheduler_context,
        return_scheduler_meta=return_scheduler_meta,
        caller_name=LOGGER.name,
        request_key=request_key,
        **kwargs,
    )


def json_safe(value):
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def normalize_stock_code(value):
    matched = re.search(r"(\d{6})", str(value or ""))
    return matched.group(1) if matched else ""


def infer_market_prefix(stock_code):
    code = normalize_stock_code(stock_code)
    if not code:
        return ""
    if code.startswith("92"):
        return "bj"
    if code.startswith(("4", "8")):
        return "bj"
    if code.startswith(("5", "6", "9")):
        return "sh"
    return "sz"


def build_prefixed_code(stock_code, market_prefix=None):
    code = normalize_stock_code(stock_code)
    if not code:
        return ""
    prefix = str(market_prefix or "").strip().lower() or infer_market_prefix(code)
    return f"{prefix}{code}" if prefix else ""


def build_stock_info_map(rows):
    stock_info_map = {}
    for row in rows or []:
        stock_code = normalize_stock_code(row.get("stock_code"))
        prefixed_code = normalize_prefixed_code(row.get("prefixed_code"))
        if stock_code:
            stock_info_map[stock_code] = row
        if prefixed_code:
            stock_info_map[prefixed_code] = row
    return stock_info_map


def resolve_prefixed_code(stock_code, stock_info_map=None):
    normalized_code = normalize_stock_code(stock_code)
    if stock_info_map:
        info_row = stock_info_map.get(normalized_code) or stock_info_map.get(normalized_code.lower())
        prefixed_code = normalize_prefixed_code((info_row or {}).get("prefixed_code"))
        if prefixed_code:
            return prefixed_code
    return build_prefixed_code(normalized_code)


def normalize_prefixed_code(value):
    text = str(value or "").strip().lower()
    matched = re.search(r"(sh|sz|bj)(\d{6})$", text)
    if matched:
        return f"{matched.group(1)}{matched.group(2)}"
    return build_prefixed_code(text)


def normalize_text(value):
    if value is None or (hasattr(pd, "isna") and pd.isna(value)):
        return None
    text = str(value).strip()
    return text or None


def normalize_numeric(value):
    if value is None or (hasattr(pd, "isna") and pd.isna(value)):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def calculate_price_change_metrics(previous_close, current_close):
    previous_close_value = normalize_numeric(previous_close)
    current_close_value = normalize_numeric(current_close)
    if previous_close_value in (None, 0) or current_close_value is None:
        return None, None

    price_change_amount = current_close_value - previous_close_value
    price_change_rate = price_change_amount / previous_close_value * 100
    return price_change_amount, price_change_rate


def round_metric_value(value, scale=4):
    numeric_value = normalize_numeric(value)
    if numeric_value is None:
        return None
    return round(float(numeric_value), scale)


def normalize_trade_date_text(value):
    if value is None or (hasattr(pd, "isna") and pd.isna(value)):
        return None
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip().split(" ")[0]
    if not text:
        return None
    text = text.replace("/", "-").replace(".", "-")
    for pattern in ("%Y-%m-%d", "%Y%m%d", "%Y年%m月%d日"):
        try:
            return datetime.strptime(text, pattern).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def parse_trade_date(value):
    normalized = normalize_trade_date_text(value)
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, "%Y-%m-%d").date()
    except ValueError:
        return None


def format_ak_date(value):
    if isinstance(value, str):
        parsed = parse_trade_date(value)
        if parsed:
            value = parsed
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return str(value or "").replace("-", "")


def format_official_date(value):
    parsed = parse_trade_date(value)
    if not parsed:
        return ""
    return parsed.strftime("%Y%m%d")


def normalize_percent_number(value):
    numeric_value = normalize_numeric(value)
    return numeric_value


def multiply_metric(value, multiplier):
    numeric_value = normalize_numeric(value)
    if numeric_value is None:
        return None
    return numeric_value * multiplier


def subtract_metric(left, right):
    left_value = normalize_numeric(left)
    right_value = normalize_numeric(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def divide_metric(left, right):
    left_value = normalize_numeric(left)
    right_value = normalize_numeric(right)
    if left_value is None or right_value in (None, 0):
        return None
    return left_value / right_value


def resolve_scheduler_bucket_date_text(value=None):
    parsed = parse_trade_date(value)
    return parsed.strftime("%Y-%m-%d") if parsed else date.today().strftime("%Y-%m-%d")


def parse_required_trade_dates(values):
    normalized_dates = []
    for value in values or []:
        normalized = normalize_trade_date_text(value)
        if not normalized or not parse_trade_date(normalized):
            raise ValueError(f"invalid trade date: {value}")
        normalized_dates.append(normalized)
    unique_dates = sorted(set(normalized_dates))
    if not unique_dates:
        raise ValueError("at least one trade date is required")
    return unique_dates


def parse_repair_daily_dates_cli_args(args):
    args = list(args or [])
    selected_codes = None
    if "--codes" in args:
        separator_index = args.index("--codes")
        date_args = args[:separator_index]
        code_args = args[separator_index + 1:]
        selected_codes = [
            normalize_stock_code(code)
            for code in code_args
            if normalize_stock_code(code)
        ]
        if code_args and not selected_codes:
            raise ValueError("repair-daily-dates --codes requires at least one valid stock code")
    else:
        date_args = args
    trade_dates = parse_required_trade_dates(date_args)
    return trade_dates, (selected_codes or None)


def parse_cli_date_text(value, field_name="date"):
    normalized = normalize_trade_date_text(value)
    if not normalized or not parse_trade_date(normalized):
        raise ValueError(f"invalid {field_name}: {value}")
    return normalized


def parse_hist_metric_cli_args(args):
    args = list(args or [])
    selected_codes = None
    if "--codes" in args:
        separator_index = args.index("--codes")
        date_args = args[:separator_index]
        code_args = args[separator_index + 1:]
        selected_codes = [
            normalize_stock_code(code)
            for code in code_args
            if normalize_stock_code(code)
        ]
        if code_args and not selected_codes:
            raise ValueError("repair-hist-metrics --codes requires at least one valid stock code")
    else:
        date_args = args

    if len(date_args) > 2:
        raise ValueError("repair-hist-metrics accepts at most 2 date arguments")

    if not date_args:
        return None, None, (selected_codes or None)

    if len(date_args) == 1:
        single_date = parse_cli_date_text(date_args[0], "date")
        return single_date, single_date, (selected_codes or None)

    start_date = parse_cli_date_text(date_args[0], "start_date")
    end_date = parse_cli_date_text(date_args[1], "end_date")
    if parse_trade_date(start_date) > parse_trade_date(end_date):
        raise ValueError("start_date can not be later than end_date")
    return start_date, end_date, (selected_codes or None)


def normalize_snapshot_time(value):
    if value is None or (hasattr(pd, "isna") and pd.isna(value)):
        return datetime.now()
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    text = str(value).strip()
    if not text:
        return datetime.now()

    try:
        numeric = float(text)
        if numeric > 1_000_000_000_000:
            return datetime.fromtimestamp(numeric / 1000.0)
        if numeric > 1_000_000_000:
            return datetime.fromtimestamp(numeric)
    except ValueError:
        pass

    for pattern in ("%Y-%m-%d %H:%M:%S", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, pattern)
            if pattern == "%H:%M:%S":
                now = datetime.now()
                return now.replace(hour=parsed.hour, minute=parsed.minute, second=parsed.second, microsecond=0)
            return parsed
        except ValueError:
            continue
    return datetime.now()


def pick_first(*values):
    for value in values:
        normalized = normalize_text(value)
        if normalized is not None:
            return normalized
    return None


def normalize_sh_board_name(symbol):
    return "科创板" if normalize_text(symbol) == "科创板" else "主板A股"


def normalize_sz_board_name(board, stock_code, symbol):
    normalized_board = normalize_text(board)
    if normalized_board in {"创业板", "创业板A股"}:
        return "创业板"
    if normalized_board in {"主板", "主板A股", "深市主板", "中小板"}:
        return "主板"

    code = normalize_stock_code(stock_code)
    if code.startswith("300"):
        return "创业板"
    if code.startswith(("000", "001", "002", "003")):
        return "主板"

    if normalize_text(symbol) == "A股列表":
        return "主板"
    return normalized_board


def is_target_stock_info_record(record):
    exchange = str(record.get("exchange", "")).strip().upper()
    board = normalize_text(record.get("board"))
    security_type = str(record.get("security_type", "")).strip().upper()

    if exchange == "SH":
        return security_type == "A" and board in SH_ALLOWED_BOARDS
    if exchange == "SZ":
        return security_type == "A" and board in SZ_ALLOWED_BOARDS
    if exchange == "BJ":
        return True
    return False


def stock_info_rows_match_target_universe(rows):
    normalized_rows = [row for row in (rows or []) if row]
    if not normalized_rows:
        return False
    summary = summarize_stock_info_rows(normalized_rows)
    return all(is_target_stock_info_record(row) for row in normalized_rows) and all(summary.values())


def summarize_stock_info_rows(rows):
    summary = {"SH": 0, "SZ": 0, "BJ": 0}
    for row in rows or []:
        exchange = str((row or {}).get("exchange", "")).strip().upper()
        if exchange in summary:
            summary[exchange] += 1
    return summary


def build_target_stock_rows(info_rows, selected_codes=None, listed_on_or_before=None):
    selected = {
        normalize_stock_code(code)
        for code in (selected_codes or [])
        if normalize_stock_code(code)
    }
    cutoff_date = parse_trade_date(listed_on_or_before) if listed_on_or_before else None
    target_rows = []
    for row in info_rows or []:
        stock_code = normalize_stock_code((row or {}).get("stock_code"))
        prefixed_code = normalize_prefixed_code((row or {}).get("prefixed_code"))
        if not stock_code or not prefixed_code:
            continue
        if selected and stock_code not in selected:
            continue
        if cutoff_date:
            list_date = parse_trade_date((row or {}).get("list_date")) or HISTORY_FALLBACK_START_DATE
            if list_date > cutoff_date:
                continue
        target_rows.append(row)
    return target_rows


def resolve_stock_history_start_date(stock_row, stock_info_map=None):
    stock_code = normalize_stock_code((stock_row or {}).get("stock_code"))
    prefixed_code = normalize_prefixed_code((stock_row or {}).get("prefixed_code"))
    info_row = None
    if stock_info_map:
        info_row = stock_info_map.get(stock_code) or stock_info_map.get(prefixed_code)
    return parse_trade_date((info_row or stock_row or {}).get("list_date")) or HISTORY_FALLBACK_START_DATE


def load_stock_info_sync_marker():
    if not Path(STOCK_INFO_SYNC_STATE_PATH).exists():
        return None
    try:
        return Path(STOCK_INFO_SYNC_STATE_PATH).read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def save_stock_info_sync_marker(marker):
    Path(STOCK_INFO_SYNC_STATE_PATH).write_text(str(marker or "").strip(), encoding="utf-8")


def get_stock_info_sh(symbol, return_scheduler_meta=False):
    return fetch_with_retry(
        ak.stock_info_sh_name_code,
        symbol=symbol,
        return_scheduler_meta=return_scheduler_meta,
        request_key=f"stock_info_sh_name_code:{symbol}",
    )


def get_stock_info_sz(symbol, return_scheduler_meta=False):
    return fetch_with_retry(
        ak.stock_info_sz_name_code,
        symbol=symbol,
        return_scheduler_meta=return_scheduler_meta,
        request_key=f"stock_info_sz_name_code:{symbol}",
    )


def get_stock_info_bj(return_scheduler_meta=False):
    return fetch_with_retry(
        ak.stock_info_bj_name_code,
        return_scheduler_meta=return_scheduler_meta,
        request_key="stock_info_bj_name_code",
    )


def get_stock_spot(return_scheduler_meta=False, bucket_date=None):
    request_bucket_date = resolve_scheduler_bucket_date_text(bucket_date)
    return fetch_with_retry(
        ak.stock_zh_a_spot,
        return_scheduler_meta=return_scheduler_meta,
        request_key=f"stock_zh_a_spot:all:{request_bucket_date}",
    )


def get_stock_history_tx(prefixed_code, start_date, end_date, scheduler_context=None):
    request_key = (
        f"stock_zh_a_hist_tx:{normalize_prefixed_code(prefixed_code)}:"
        f"{format_ak_date(start_date)}:{format_ak_date(end_date)}:{STOCK_HIST_TX_REQUEST_KEY_VERSION}"
    )
    return fetch_with_retry(
        ak.stock_zh_a_hist_tx,
        symbol=normalize_prefixed_code(prefixed_code),
        start_date=format_ak_date(start_date),
        end_date=format_ak_date(end_date),
        adjust="",
        scheduler_context=scheduler_context,
        request_key=request_key,
    )


def get_stock_hfq_daily(prefixed_code, start_date, end_date):
    request_key = f"stock_zh_a_daily:{normalize_prefixed_code(prefixed_code)}:{format_ak_date(start_date)}:{format_ak_date(end_date)}:hfq"
    return fetch_with_retry(
        ak.stock_zh_a_daily,
        symbol=normalize_prefixed_code(prefixed_code),
        start_date=format_ak_date(start_date),
        end_date=format_ak_date(end_date),
        adjust="hfq",
        request_key=request_key,
    )


def build_info_record(
    stock_code,
    market_prefix,
    exchange,
    board,
    security_type,
    stock_name=None,
    security_full_name=None,
    company_abbr=None,
    company_full_name=None,
    list_date=None,
    industry=None,
    region=None,
    total_share_capital=None,
    circulating_share_capital=None,
    source_name=None,
    raw_record=None,
):
    code = normalize_stock_code(stock_code)
    prefixed_code = build_prefixed_code(code, market_prefix)
    if not code or not prefixed_code:
        return None
    return {
        "stock_code": code,
        "prefixed_code": prefixed_code,
        "exchange": exchange,
        "market_prefix": market_prefix,
        "board": board,
        "security_type": security_type,
        "stock_name": normalize_text(stock_name),
        "security_full_name": normalize_text(security_full_name),
        "company_abbr": normalize_text(company_abbr),
        "company_full_name": normalize_text(company_full_name),
        "list_date": normalize_trade_date_text(list_date),
        "industry": normalize_text(industry),
        "region": normalize_text(region),
        "total_share_capital": normalize_numeric(total_share_capital),
        "circulating_share_capital": normalize_numeric(circulating_share_capital),
        "_source_variant": {
            "source_name": source_name,
            "exchange": exchange,
            "market_prefix": market_prefix,
            "board": board,
            "security_type": security_type,
        },
        "_raw_record": {
            "source_name": source_name,
            "record": json_safe(raw_record or {}),
        },
    }


def build_sh_records(df, symbol):
    records = []
    for _, row in df.iterrows():
        record = build_info_record(
            stock_code=row.get("证券代码"),
            market_prefix="sh",
            exchange="SH",
            board=normalize_sh_board_name(symbol),
            security_type="A",
            stock_name=row.get("证券简称"),
            security_full_name=row.get("证券全称"),
            company_abbr=row.get("公司简称"),
            company_full_name=row.get("公司全称"),
            list_date=row.get("上市日期"),
            source_name=f"stock_info_sh_name_code:{symbol}",
            raw_record=row.to_dict(),
        )
        if record:
            records.append(record)
    return records


def build_sz_variant_record(row, symbol, security_label):
    code = row.get(f"{security_label}代码")
    if not normalize_stock_code(code):
        return None
    security_type = "CDR" if security_label == "CDR" else ("B" if security_label.startswith("B") else "A")
    return build_info_record(
        stock_code=code,
        market_prefix="sz",
        exchange="SZ",
        board=normalize_sz_board_name(row.get("板块"), code, symbol),
        security_type=security_type,
        stock_name=row.get(f"{security_label}简称"),
        list_date=row.get(f"{security_label}上市日期"),
        industry=row.get("所属行业"),
        total_share_capital=row.get(f"{security_label}总股本"),
        circulating_share_capital=row.get(f"{security_label}流通股本"),
        source_name=f"stock_info_sz_name_code:{symbol}",
        raw_record=row.to_dict(),
    )


def build_sz_records(df, symbol):
    records = []
    for _, row in df.iterrows():
        if symbol == "A股列表":
            record = build_sz_variant_record(row, symbol, "A股")
        else:
            record = None

        if record and is_target_stock_info_record(record):
            records.append(record)
            continue

        generic_record = build_info_record(
            stock_code=pick_first(row.get("证券代码"), row.get("代码"), row.get("股票代码")),
            market_prefix="sz",
            exchange="SZ",
            board=normalize_sz_board_name(
                pick_first(row.get("板块"), symbol),
                pick_first(row.get("证券代码"), row.get("代码"), row.get("股票代码")),
                symbol,
            ),
            security_type="A",
            stock_name=pick_first(row.get("证券简称"), row.get("简称"), row.get("名称")),
            list_date=pick_first(row.get("上市日期"), row.get("A股上市日期"), row.get("CDR上市日期")),
            industry=row.get("所属行业"),
            total_share_capital=pick_first(row.get("总股本"), row.get("A股总股本")),
            circulating_share_capital=pick_first(row.get("流通股本"), row.get("A股流通股本")),
            source_name=f"stock_info_sz_name_code:{symbol}",
            raw_record=row.to_dict(),
        )
        if generic_record and is_target_stock_info_record(generic_record):
            records.append(generic_record)
    return records


def build_bj_records(df):
    records = []
    for _, row in df.iterrows():
        record = build_info_record(
            stock_code=row.get("证券代码"),
            market_prefix="bj",
            exchange="BJ",
            board="北交所",
            security_type="A",
            stock_name=row.get("证券简称"),
            list_date=row.get("上市日期"),
            industry=row.get("所属行业"),
            region=row.get("地区"),
            total_share_capital=row.get("总股本"),
            circulating_share_capital=row.get("流通股本"),
            source_name="stock_info_bj_name_code",
            raw_record=row.to_dict(),
        )
        if record:
            records.append(record)
    return records


def merge_stock_info_records(records):
    merged = {}
    for record in records:
        prefixed_code = record["prefixed_code"]
        current = merged.get(prefixed_code)
        if current is None:
            merged[prefixed_code] = {
                "stock_code": record["stock_code"],
                "prefixed_code": prefixed_code,
                "exchange": record.get("exchange"),
                "market_prefix": record.get("market_prefix"),
                "board": record.get("board"),
                "security_type": record.get("security_type"),
                "stock_name": record.get("stock_name"),
                "security_full_name": record.get("security_full_name"),
                "company_abbr": record.get("company_abbr"),
                "company_full_name": record.get("company_full_name"),
                "list_date": record.get("list_date"),
                "industry": record.get("industry"),
                "region": record.get("region"),
                "total_share_capital": record.get("total_share_capital"),
                "circulating_share_capital": record.get("circulating_share_capital"),
                "source_variants_json": [record["_source_variant"]],
                "raw_records_json": [record["_raw_record"]],
            }
            continue

        for field in (
            "exchange",
            "market_prefix",
            "board",
            "security_type",
            "stock_name",
            "security_full_name",
            "company_abbr",
            "company_full_name",
            "list_date",
            "industry",
            "region",
        ):
            if not current.get(field) and record.get(field):
                current[field] = record[field]

        for numeric_field in ("total_share_capital", "circulating_share_capital"):
            if current.get(numeric_field) is None and record.get(numeric_field) is not None:
                current[numeric_field] = record[numeric_field]

        current["source_variants_json"].append(record["_source_variant"])
        current["raw_records_json"].append(record["_raw_record"])

    return [row for row in merged.values() if is_target_stock_info_record(row)]


async def load_all_stock_info_records():
    all_records = []
    for symbol in SH_SOURCES:
        df = await asyncio.to_thread(get_stock_info_sh, symbol)
        all_records.extend(build_sh_records(df, symbol))
    for symbol in SZ_SOURCES:
        df = await asyncio.to_thread(get_stock_info_sz, symbol)
        all_records.extend(build_sz_records(df, symbol))
    bj_df = await asyncio.to_thread(get_stock_info_bj)
    all_records.extend(build_bj_records(bj_df))
    return merge_stock_info_records(all_records)


def build_spot_snapshot_rows(spot_df, selected_codes=None, stock_info_map=None):
    selected = {
        normalize_stock_code(code)
        for code in (selected_codes or [])
        if normalize_stock_code(code)
    }
    rows = []
    for _, row in spot_df.iterrows():
        stock_code = normalize_stock_code(row.get("代码"))
        if not stock_code or (selected and stock_code not in selected):
            continue
        prefixed_code = resolve_prefixed_code(stock_code, stock_info_map=stock_info_map)
        snapshot_time = normalize_snapshot_time(row.get("时间戳"))
        trade_date = snapshot_time.date().strftime("%Y-%m-%d") if snapshot_time else date.today().strftime("%Y-%m-%d")
        rows.append({
            "stock_code": stock_code,
            "prefixed_code": prefixed_code,
            "stock_name": normalize_text(row.get("名称")),
            "trade_date": trade_date,
            "open_price": row.get("今开"),
            "close_price": row.get("最新价"),
            "high_price": row.get("最高"),
            "low_price": row.get("最低"),
            "latest_price": row.get("最新价"),
            "pre_close_price": row.get("昨收"),
            "buy_price": row.get("买入"),
            "sell_price": row.get("卖出"),
            "price_change_amount": row.get("涨跌额"),
            "price_change_rate": row.get("涨跌幅"),
            "volume": row.get("成交量"),
            "turnover_amount": row.get("成交额"),
            "data_source": "stock_zh_a_spot",
            "snapshot_time": snapshot_time,
        })
    return rows


def build_hist_tx_rows(prefixed_code, stock_name, history_df):
    rows = []
    stock_code = normalize_stock_code(prefixed_code)
    for _, row in history_df.iterrows():
        trade_date = normalize_trade_date_text(row.get("date") or getattr(row, "name", None))
        if not trade_date:
            continue
        rows.append({
            "stock_code": stock_code,
            "prefixed_code": normalize_prefixed_code(prefixed_code),
            "stock_name": stock_name,
            "trade_date": trade_date,
            "open_price": row.get("open"),
            "close_price": row.get("close"),
            "high_price": row.get("high"),
            "low_price": row.get("low"),
            "latest_price": row.get("close"),
            "pre_close_price": None,
            "buy_price": None,
            "sell_price": None,
            "price_change_amount": None,
            "price_change_rate": None,
            "volume": row.get("volume"),
            "turnover_amount": row.get("amount"),
            "data_source": "stock_zh_a_hist_tx",
            "snapshot_time": None,
        })
    return rows


def build_hfq_rows(prefixed_code, stock_name, request_start_date, request_end_date, refresh_batch_id, hfq_df):
    rows = []
    stock_code = normalize_stock_code(prefixed_code)
    normalized_prefixed_code = normalize_prefixed_code(prefixed_code)
    request_start = normalize_trade_date_text(request_start_date)
    request_end = normalize_trade_date_text(request_end_date)
    dated_rows = []
    for _, row in hfq_df.iterrows():
        trade_date = normalize_trade_date_text(row.get("date") or getattr(row, "name", None))
        if not trade_date:
            continue
        dated_rows.append((trade_date, row))

    dated_rows.sort(key=lambda item: item[0])
    previous_close = None
    for trade_date, row in dated_rows:
        current_close = normalize_numeric(row.get("close"))
        price_change_amount, price_change_rate = calculate_price_change_metrics(previous_close, current_close)
        rows.append({
            "stock_code": stock_code,
            "prefixed_code": normalized_prefixed_code,
            "stock_name": stock_name,
            "trade_date": trade_date,
            "open_price": row.get("open"),
            "close_price": row.get("close"),
            "high_price": row.get("high"),
            "low_price": row.get("low"),
            "price_change_amount": price_change_amount,
            "price_change_rate": price_change_rate,
            "volume": row.get("volume"),
            "turnover_amount": row.get("amount"),
            "outstanding_share": row.get("outstanding_share"),
            "turnover_rate": row.get("turnover"),
            "data_source": "stock_zh_a_daily_hfq",
            "request_start_date": request_start,
            "request_end_date": request_end,
            "refresh_batch_id": refresh_batch_id,
        })
        if current_close is not None:
            previous_close = current_close
    return rows


def build_hist_metric_update_rows(prefixed_code, history_rows, repair_start_date=None, repair_end_date=None):
    normalized_prefixed_code = normalize_prefixed_code(prefixed_code)
    start_boundary = parse_trade_date(repair_start_date) if repair_start_date else None
    end_boundary = parse_trade_date(repair_end_date) if repair_end_date else None
    updates = []
    previous_close = None

    for row in history_rows or []:
        trade_date = normalize_trade_date_text((row or {}).get("trade_date"))
        trade_date_obj = parse_trade_date(trade_date)
        if not trade_date or trade_date_obj is None:
            continue

        current_close = normalize_numeric((row or {}).get("close_price"))
        pre_close_price = round_metric_value(previous_close, scale=4)
        price_change_amount, price_change_rate = calculate_price_change_metrics(previous_close, current_close)

        if (
            (start_boundary is None or trade_date_obj >= start_boundary)
            and (end_boundary is None or trade_date_obj <= end_boundary)
        ):
            updates.append({
                "prefixed_code": normalized_prefixed_code,
                "trade_date": trade_date,
                "pre_close_price": pre_close_price,
                "price_change_amount": round_metric_value(price_change_amount, scale=4),
                "price_change_rate": round_metric_value(price_change_rate, scale=4),
            })

        if current_close is not None:
            previous_close = current_close

    return updates


class OfficialExchangeRateLimiter:
    def __init__(self, interval_seconds=OFFICIAL_EXCHANGE_REQUEST_INTERVAL_SECONDS, sleep_func=asyncio.sleep, monotonic_func=time.monotonic):
        self.interval_seconds = max(0.0, float(interval_seconds or 0))
        self.sleep_func = sleep_func
        self.monotonic_func = monotonic_func
        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0

    async def wait(self):
        async with self._lock:
            now = self.monotonic_func()
            sleep_seconds = self._next_allowed_at - now
            if sleep_seconds > 0:
                await self.sleep_func(sleep_seconds)
                now = self.monotonic_func()
            self._next_allowed_at = now + self.interval_seconds


class OfficialExchangeHttpClient:
    def __init__(self, exchange, limiter=None, timeout=30):
        self.exchange = str(exchange or "").strip().upper()
        self.limiter = limiter or OfficialExchangeRateLimiter()
        self.timeout = timeout
        self.session = requests.Session()
        self.session.trust_env = False

    def close(self):
        self.session.close()

    async def get_text(self, url, params=None, headers=None):
        await self.limiter.wait()
        return await asyncio.to_thread(self._get_text_sync, url, params or {}, headers or {})

    def _get_text_sync(self, url, params, headers):
        request_headers = dict(OFFICIAL_HTTP_HEADERS)
        request_headers.update(headers or {})
        response = self.session.get(
            url,
            params=params,
            headers=request_headers,
            timeout=self.timeout,
            proxies={"http": None, "https": None},
        )
        response.raise_for_status()
        return response.text


def parse_json_or_jsonp(text):
    raw_text = str(text or "").strip()
    if not raw_text:
        raise ValueError("official response is empty")
    if raw_text[0] != "{":
        start = raw_text.find("(")
        end = raw_text.rfind(")")
        if start >= 0 and end > start:
            raw_text = raw_text[start + 1:end].strip()
    return json.loads(raw_text)


def get_target_official_stock_rows(info_rows, exchange, selected_codes=None):
    normalized_exchange = str(exchange or "").strip().upper()
    selected = {
        normalize_stock_code(code)
        for code in (selected_codes or [])
        if normalize_stock_code(code)
    }
    rows = []
    for row in info_rows or []:
        stock_code = normalize_stock_code(row.get("stock_code"))
        if not stock_code or (selected and stock_code not in selected):
            continue
        if str(row.get("exchange") or "").strip().upper() != normalized_exchange:
            continue
        if str(row.get("security_type") or "").strip().upper() != "A":
            continue
        prefixed_code = normalize_prefixed_code(row.get("prefixed_code"))
        if normalized_exchange == "SH" and not prefixed_code.startswith("sh"):
            continue
        if normalized_exchange == "SZ" and not prefixed_code.startswith("sz"):
            continue
        rows.append(row)
    return rows


def build_sse_official_row(stock_row, target_date, result_row):
    trade_date = normalize_trade_date_text((result_row or {}).get("TX_DATE"))
    target_date_text = normalize_trade_date_text(target_date)
    if trade_date != target_date_text:
        return None

    close_price = normalize_numeric((result_row or {}).get("CLOSE_PRICE"))
    price_change_amount = normalize_numeric((result_row or {}).get("CHANGE_PRICE"))
    pre_close_price = subtract_metric(close_price, price_change_amount)
    total_market_value = multiply_metric((result_row or {}).get("TOTAL_VALUE"), 10000)
    circulating_market_value = multiply_metric((result_row or {}).get("NEGO_VALUE"), 10000)
    total_share_capital = divide_metric(total_market_value, close_price)
    circulating_share_capital = divide_metric(circulating_market_value, close_price)

    return {
        "exchange": "SH",
        "stock_code": normalize_stock_code(stock_row.get("stock_code")),
        "prefixed_code": normalize_prefixed_code(stock_row.get("prefixed_code")),
        "stock_name": normalize_text((result_row or {}).get("SEC_NAME")) or normalize_text(stock_row.get("stock_name")),
        "trade_date": trade_date,
        "open_price": (result_row or {}).get("OPEN_PRICE"),
        "close_price": close_price,
        "high_price": (result_row or {}).get("HIGH_PRICE"),
        "low_price": (result_row or {}).get("LOW_PRICE"),
        "pre_close_price": pre_close_price,
        "price_change_amount": price_change_amount,
        "price_change_rate": normalize_percent_number((result_row or {}).get("CHANGE_RATE")),
        "volume": multiply_metric((result_row or {}).get("TRADE_VOL"), 10000),
        "turnover_amount": multiply_metric((result_row or {}).get("TRADE_AMT"), 10000),
        "total_market_value": total_market_value,
        "circulating_market_value": circulating_market_value,
        "total_share_capital": total_share_capital,
        "circulating_share_capital": circulating_share_capital,
        "pe_rate": (result_row or {}).get("PE_RATE"),
        "turnover_rate": (result_row or {}).get("TO_RATE"),
        "amplitude": (result_row or {}).get("SWING_RATE"),
        "data_source": SSE_OFFICIAL_DAILY_SOURCE,
        "raw_trading_json": result_row,
        "raw_metrics_json": result_row,
    }


def find_sse_daily_result(payload, target_date):
    target_compact = format_official_date(target_date)
    for row in (payload or {}).get("result") or []:
        if str(row.get("DATE_TYPE") or "").strip().upper() != "D":
            continue
        if format_official_date(row.get("TX_DATE")) == target_compact:
            return row
    return None


def normalize_szse_history_row(raw_row):
    if not isinstance(raw_row, (list, tuple)) or len(raw_row) < 9:
        return None
    return {
        "trade_date": normalize_trade_date_text(raw_row[0]),
        "open_price": raw_row[1],
        "close_price": raw_row[2],
        "low_price": raw_row[3],
        "high_price": raw_row[4],
        "price_change_amount": raw_row[5],
        "price_change_rate": raw_row[6],
        "volume_lot": raw_row[7],
        "turnover_amount": raw_row[8],
        "raw": list(raw_row),
    }


def find_szse_daily_result(payload, target_date):
    target_date_text = normalize_trade_date_text(target_date)
    for raw_row in ((payload or {}).get("data") or {}).get("picupdata") or []:
        normalized_row = normalize_szse_history_row(raw_row)
        if normalized_row and normalized_row.get("trade_date") == target_date_text:
            return normalized_row
    return None


def extract_szse_now_metrics(metrics_payload):
    now_metrics = {}
    for item in (metrics_payload or {}).get("data") or []:
        if isinstance(item, dict) and any(str(key).startswith("now_") for key in item):
            now_metrics.update(item)
    return now_metrics


def build_szse_official_row(stock_row, target_date, daily_row, metrics_payload):
    trade_date = normalize_trade_date_text(daily_row.get("trade_date") if daily_row else None)
    target_date_text = normalize_trade_date_text(target_date)
    if trade_date != target_date_text:
        return None

    metrics_date = normalize_trade_date_text((metrics_payload or {}).get("lastDate"))
    if metrics_date != target_date_text:
        raise ValueError(f"SZSE metrics latest date {metrics_date or '-'} does not match target {target_date_text}")

    now_metrics = extract_szse_now_metrics(metrics_payload)
    close_price = normalize_numeric(daily_row.get("close_price"))
    price_change_amount = normalize_numeric(daily_row.get("price_change_amount"))
    pre_close_price = subtract_metric(close_price, price_change_amount)
    amplitude = None
    if pre_close_price not in (None, 0):
        high_value = normalize_numeric(daily_row.get("high_price"))
        low_value = normalize_numeric(daily_row.get("low_price"))
        if high_value is not None and low_value is not None:
            amplitude = (high_value - low_value) / pre_close_price * 100

    return {
        "exchange": "SZ",
        "stock_code": normalize_stock_code(stock_row.get("stock_code")),
        "prefixed_code": normalize_prefixed_code(stock_row.get("prefixed_code")),
        "stock_name": normalize_text(stock_row.get("stock_name")),
        "trade_date": trade_date,
        "open_price": daily_row.get("open_price"),
        "close_price": close_price,
        "high_price": daily_row.get("high_price"),
        "low_price": daily_row.get("low_price"),
        "pre_close_price": pre_close_price,
        "price_change_amount": price_change_amount,
        "price_change_rate": normalize_percent_number(daily_row.get("price_change_rate")),
        "volume": multiply_metric(daily_row.get("volume_lot"), 100),
        "turnover_amount": daily_row.get("turnover_amount"),
        "total_market_value": multiply_metric(now_metrics.get("now_sjzz"), 100000000),
        "circulating_market_value": multiply_metric(now_metrics.get("now_ltsz"), 100000000),
        "total_share_capital": multiply_metric(now_metrics.get("now_zgb"), 100000000),
        "circulating_share_capital": multiply_metric(now_metrics.get("now_ltgb"), 100000000),
        "pe_rate": now_metrics.get("now_syl"),
        "turnover_rate": now_metrics.get("now_hsl"),
        "amplitude": amplitude,
        "data_source": SZSE_OFFICIAL_DAILY_SOURCE,
        "raw_trading_json": daily_row.get("raw"),
        "raw_metrics_json": metrics_payload,
    }


async def fetch_sse_official_daily_row(client, stock_row, target_date):
    stock_code = normalize_stock_code(stock_row.get("stock_code"))
    params = {
        "jsonCallBack": "jsonpCallback",
        "isPagination": "false",
        "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_CJGK_MRGK_C",
        "SEC_CODE": stock_code,
        "TX_DATE": format_official_date(target_date),
        "TX_DATE_MON": "",
        "TX_DATE_YEAR": "",
    }
    text = await client.get_text(
        SSE_OFFICIAL_DAILY_URL,
        params=params,
        headers={"Referer": f"{SSE_OFFICIAL_REFERER_URL}?COMPANY_CODE={stock_code}"},
    )
    payload = parse_json_or_jsonp(text)
    result_row = find_sse_daily_result(payload, target_date)
    if not result_row:
        return None, payload
    return build_sse_official_row(stock_row, target_date, result_row), payload


async def fetch_szse_official_daily_row(client, stock_row, target_date):
    stock_code = normalize_stock_code(stock_row.get("stock_code"))
    history_text = await client.get_text(
        SZSE_OFFICIAL_HISTORY_URL,
        params={"cycleType": 32, "marketId": 1, "code": stock_code},
        headers={"Referer": f"{SZSE_OFFICIAL_REFERER_URL}?code={stock_code}"},
    )
    history_payload = parse_json_or_jsonp(history_text)
    daily_row = find_szse_daily_result(history_payload, target_date)
    if not daily_row:
        return None, history_payload, None

    metrics_text = await client.get_text(
        SZSE_OFFICIAL_KEY_INDEX_URL,
        params={"secCode": stock_code},
        headers={"Referer": f"{SZSE_OFFICIAL_REFERER_URL}?code={stock_code}"},
    )
    metrics_payload = parse_json_or_jsonp(metrics_text)
    return build_szse_official_row(stock_row, target_date, daily_row, metrics_payload), history_payload, metrics_payload


async def collect_exchange_official_rows(
    exchange,
    stock_rows,
    target_date,
    db_tools=None,
    request_interval_seconds=OFFICIAL_EXCHANGE_REQUEST_INTERVAL_SECONDS,
):
    limiter = OfficialExchangeRateLimiter(interval_seconds=request_interval_seconds)
    client = OfficialExchangeHttpClient(exchange=exchange, limiter=limiter)
    missing_codes = []
    failed_items = []
    latest_source_date = None
    row_count = 0
    upserted_count = 0
    turnover_amount_count = 0
    try:
        for index, stock_row in enumerate(stock_rows, start=1):
            stock_code = normalize_stock_code(stock_row.get("stock_code"))
            try:
                if exchange == "SH":
                    row, payload = await fetch_sse_official_daily_row(client, stock_row, target_date)
                    for item in (payload or {}).get("result") or []:
                        candidate_date = normalize_trade_date_text(item.get("TX_DATE"))
                        if candidate_date and (latest_source_date is None or candidate_date > latest_source_date):
                            latest_source_date = candidate_date
                elif exchange == "SZ":
                    row, history_payload, _metrics_payload = await fetch_szse_official_daily_row(client, stock_row, target_date)
                    for raw in ((history_payload or {}).get("data") or {}).get("picupdata") or []:
                        candidate = normalize_szse_history_row(raw)
                        candidate_date = candidate.get("trade_date") if candidate else None
                        if candidate_date and (latest_source_date is None or candidate_date > latest_source_date):
                            latest_source_date = candidate_date
                else:
                    raise ValueError(f"unsupported exchange: {exchange}")

                if row:
                    if db_tools is not None:
                        upserted_count += await db_tools.upsert_stock_exchange_official_daily_data([row])
                    row_count += 1
                    if normalize_numeric(row.get("turnover_amount")) is not None:
                        turnover_amount_count += 1
                    if row_count % 100 == 0:
                        LOGGER.info(
                            "stock exchange official daily progress: "
                            f"target_date={target_date}, exchange={exchange}, "
                            f"rows={row_count}/{len(stock_rows)}, upserted={upserted_count}, "
                            f"processed={index}, missing={len(missing_codes)}, failed={len(failed_items)}"
                        )
                else:
                    missing_codes.append(stock_code)
            except Exception as exc:
                failed_items.append({"stock_code": stock_code, "error": str(exc)})
    finally:
        client.close()

    return {
        "exchange": exchange,
        "target_count": len(stock_rows),
        "row_count": row_count,
        "upserted_count": upserted_count,
        "turnover_amount_count": turnover_amount_count,
        "missing_codes": missing_codes,
        "missing_count": len(missing_codes),
        "failed_items": failed_items,
        "failed_count": len(failed_items),
        "latest_source_date": latest_source_date,
    }


async def sync_stock_info_all(db_tools=None, force=False):
    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    try:
        today_marker = date.today().strftime("%Y-%m-%d")
        existing_rows = await db_tools.get_all_stock_info_rows()
        if not force and load_stock_info_sync_marker() == today_marker:
            if stock_info_rows_match_target_universe(existing_rows):
                print(f"stock info already synced today ({today_marker}), skipping refresh")
                return existing_rows
            print(
                f"stock info marker exists for {today_marker}, "
                "but current table contains rows outside the target universe; forcing refresh"
            )

        merged_rows = await load_all_stock_info_records()
        if not merged_rows:
            raise ValueError("stock info refresh returned no rows")

        stale_prefixed_codes = sorted(
            {
                normalize_prefixed_code(row.get("prefixed_code"))
                for row in existing_rows
                if normalize_prefixed_code(row.get("prefixed_code"))
            }
            - {
                normalize_prefixed_code(row.get("prefixed_code"))
                for row in merged_rows
                if normalize_prefixed_code(row.get("prefixed_code"))
            }
        )
        affected = await db_tools.upsert_stock_info_all(merged_rows)
        deleted = 0
        if stale_prefixed_codes:
            deleted = await db_tools.delete_stock_info_all_by_prefixed_codes(stale_prefixed_codes)
        save_stock_info_sync_marker(today_marker)
        summary = summarize_stock_info_rows(merged_rows)
        print(
            "stock info synced: "
            f"rows={len(merged_rows)}, affected={affected}, deleted={deleted}, "
            f"sh={summary['SH']}, sz={summary['SZ']}, bj={summary['BJ']}"
        )
        return merged_rows
    finally:
        if own_db:
            await db_tools.close()


async def sync_daily(selected_codes=None, db_tools=None):
    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    try:
        info_rows = await sync_stock_info_all(db_tools=db_tools)
        target_rows = build_target_stock_rows(info_rows, selected_codes=selected_codes)
        target_codes = [row["stock_code"] for row in target_rows]
        target_prefixed_codes = {
            normalize_prefixed_code(row.get("prefixed_code"))
            for row in target_rows
            if normalize_prefixed_code(row.get("prefixed_code"))
        }
        stock_info_map = build_stock_info_map(info_rows)
        today_text = date.today().strftime("%Y-%m-%d")
        spot_result = await asyncio.to_thread(get_stock_spot, True, today_text)
        spot_df = spot_result.value
        rows = build_spot_snapshot_rows(spot_df, selected_codes=target_codes, stock_info_map=stock_info_map)
        if not rows:
            raise ValueError("stock daily returned no spot rows for the selected universe")
        trade_dates = sorted(
            {
                normalize_trade_date_text(row.get("trade_date"))
                for row in rows
                if normalize_trade_date_text(row.get("trade_date"))
            }
        )
        if len(trade_dates) != 1:
            raise ValueError(f"stock spot returned inconsistent trade dates: {trade_dates}")
        derived_trade_date = trade_dates[0]
        if derived_trade_date != today_text:
            raise ValueError(
                f"stock spot trade_date mismatch: expected {today_text}, got {derived_trade_date}"
            )
        affected = await db_tools.upsert_stock_daily_data(rows)
        deleted_today = 0
        if selected_codes is None:
            existing_today_codes = set(await db_tools.get_stock_daily_prefixed_codes_by_date(derived_trade_date))
            extra_today_codes = sorted(existing_today_codes - target_prefixed_codes)
            if extra_today_codes:
                deleted_today = await db_tools.delete_stock_daily_data_by_trade_date_and_prefixed_codes(
                    derived_trade_date,
                    extra_today_codes,
                )

        print(
            "stock daily finished: "
            f"trade_date={derived_trade_date}, rows={len(rows)}, affected={affected}, "
            f"deleted_today={deleted_today}, target_universe={len(target_rows)}"
        )
        quant_affected = await refresh_quant_index_dashboard_range(
            db_tools,
            start_date=derived_trade_date,
            end_date=derived_trade_date,
        )
        print(
            "stock daily quant-index refresh: "
            f"start_date={derived_trade_date}, end_date={derived_trade_date}, affected={quant_affected}"
        )
        return affected
    finally:
        if own_db:
            await db_tools.close()


async def sync_exchange_official_daily(selected_codes=None, db_tools=None, target_date=None, request_interval_seconds=OFFICIAL_EXCHANGE_REQUEST_INTERVAL_SECONDS):
    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    started = time.perf_counter()
    try:
        target_date_text = normalize_trade_date_text(target_date) or date.today().strftime("%Y-%m-%d")
        await db_tools.ensure_stock_exchange_official_daily_table()
        info_rows = await db_tools.get_all_stock_info_rows()
        if not info_rows:
            info_rows = await sync_stock_info_all(db_tools=db_tools, force=True)

        sh_rows = get_target_official_stock_rows(info_rows, "SH", selected_codes=selected_codes)
        sz_rows = get_target_official_stock_rows(info_rows, "SZ", selected_codes=selected_codes)
        if not sh_rows and not sz_rows:
            raise ValueError("stock_exchange_official_daily found no SH/SZ stock_info_all rows")

        sh_result, sz_result = await asyncio.gather(
            collect_exchange_official_rows(
                "SH",
                sh_rows,
                target_date_text,
                db_tools=db_tools,
                request_interval_seconds=request_interval_seconds,
            ),
            collect_exchange_official_rows(
                "SZ",
                sz_rows,
                target_date_text,
                db_tools=db_tools,
                request_interval_seconds=request_interval_seconds,
            ),
        )
        upserted = sh_result["upserted_count"] + sz_result["upserted_count"]

        exchange_failures = []
        for result in (sh_result, sz_result):
            if result["target_count"] > 0 and result["row_count"] == 0:
                exchange_failures.append(
                    f"{result['exchange']}目标日{target_date_text}无数据"
                    f"（源最新{result.get('latest_source_date') or '-'}，失败{result['failed_count']}，缺失{result['missing_count']}）"
                )

        summary = {
            "target_date": target_date_text,
            "upserted": upserted,
            "sh": {
                "target_count": sh_result["target_count"],
                "row_count": sh_result["row_count"],
                "missing_count": sh_result["missing_count"],
                "failed_count": sh_result["failed_count"],
                "turnover_amount_count": sh_result["turnover_amount_count"],
                "latest_source_date": sh_result.get("latest_source_date"),
                "missing_samples": sh_result["missing_codes"][:10],
                "failed_samples": sh_result["failed_items"][:5],
            },
            "sz": {
                "target_count": sz_result["target_count"],
                "row_count": sz_result["row_count"],
                "missing_count": sz_result["missing_count"],
                "failed_count": sz_result["failed_count"],
                "turnover_amount_count": sz_result["turnover_amount_count"],
                "latest_source_date": sz_result.get("latest_source_date"),
                "missing_samples": sz_result["missing_codes"][:10],
                "failed_samples": sz_result["failed_items"][:5],
            },
            "turnover_amount_count": sh_result["turnover_amount_count"] + sz_result["turnover_amount_count"],
            "duration_seconds": round(time.perf_counter() - started, 3),
        }
        print(
            "stock exchange official daily finished: "
            f"target_date={target_date_text}, upserted={upserted}, "
            f"sh={sh_result['row_count']}/{sh_result['target_count']}, "
            f"sz={sz_result['row_count']}/{sz_result['target_count']}"
        )
        if exchange_failures:
            raise RuntimeError(
                "stock exchange official daily incomplete: "
                + "；".join(exchange_failures)
                + f"；summary={json.dumps(summary, ensure_ascii=False, default=str)}"
            )
        return summary
    finally:
        if own_db:
            await db_tools.close()


async def _backfill_single_stock(db_tools, stock_row, stock_info_map, end_date, scheduler_context=None, semaphore=None):
    async with semaphore:
        stock_code = stock_row["stock_code"]
        prefixed_code = stock_row["prefixed_code"]
        stock_name = stock_row.get("stock_name")
        info_row = stock_info_map.get(stock_code) or stock_info_map.get(prefixed_code)
        list_date = resolve_stock_history_start_date(stock_row, stock_info_map=stock_info_map)
        if list_date > end_date:
            return 0

        history_df = await asyncio.to_thread(
            get_stock_history_tx,
            prefixed_code,
            list_date,
            end_date,
            scheduler_context,
        )
        if history_df is None or history_df.empty:
            raise ValueError(f"empty history returned for {prefixed_code}")
        rows = build_hist_tx_rows(prefixed_code, stock_name or (info_row or {}).get("stock_name"), history_df)
        if not rows:
            raise ValueError(f"history rows could not be built for {prefixed_code}")
        return await db_tools.upsert_stock_daily_data(rows)


async def _repair_daily_dates_single_stock(
    db_tools,
    stock_row,
    stock_info_map,
    requested_trade_dates,
    repair_end_date,
    scheduler_context=None,
    semaphore=None,
):
    async with semaphore:
        stock_code = stock_row["stock_code"]
        prefixed_code = stock_row["prefixed_code"]
        stock_name = stock_row.get("stock_name")
        info_row = stock_info_map.get(stock_code) or stock_info_map.get(prefixed_code)
        list_date = resolve_stock_history_start_date(stock_row, stock_info_map=stock_info_map)
        repair_start_date = parse_trade_date(min(requested_trade_dates))
        effective_start_date = max(list_date, repair_start_date)
        if effective_start_date > repair_end_date:
            return 0

        history_df = await asyncio.to_thread(
            get_stock_history_tx,
            prefixed_code,
            effective_start_date,
            repair_end_date,
            scheduler_context,
        )
        if history_df is None or history_df.empty:
            raise ValueError(f"empty history returned for {prefixed_code}")
        rows = build_hist_tx_rows(prefixed_code, stock_name or (info_row or {}).get("stock_name"), history_df)
        if not rows:
            raise ValueError(f"history rows could not be built for {prefixed_code}")
        filtered_rows = [
            row for row in rows
            if normalize_trade_date_text(row.get("trade_date")) in requested_trade_dates
        ]
        if not filtered_rows:
            raise ValueError(f"requested trade dates missing from history for {prefixed_code}")
        return await db_tools.upsert_stock_daily_data(filtered_rows)


async def refresh_quant_index_dashboard_range(db_tools, start_date, end_date):
    from akshare_project.collectors.quant_index import compute_and_upsert_range

    start_text = normalize_trade_date_text(start_date)
    end_text = normalize_trade_date_text(end_date)
    if not start_text or not end_text:
        return 0
    if parse_trade_date(start_text) > parse_trade_date(end_text):
        return 0
    return await compute_and_upsert_range(db_tools, start_text, end_text)


async def backfill_history(selected_codes=None, db_tools=None):
    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    try:
        info_rows = await sync_stock_info_all(db_tools=db_tools)
        spot_result = await asyncio.to_thread(get_stock_spot, True)
        spot_df = spot_result.value
        stock_info_map = build_stock_info_map(info_rows)
        target_rows = build_target_stock_rows(info_rows, selected_codes=selected_codes)
        target_codes = [row["stock_code"] for row in target_rows]
        universe_rows = build_spot_snapshot_rows(
            spot_df,
            selected_codes=target_codes,
            stock_info_map=stock_info_map,
        )
        if not universe_rows:
            print("stock backfill finished: no stocks matched current spot universe")
            return 0

        scheduler_context = None
        if getattr(spot_result, "job_id", None):
            scheduler_context = SchedulerContext(
                parent_job_id=int(spot_result.job_id),
                root_job_id=int(spot_result.root_job_id or spot_result.job_id),
                workflow_name="stock_backfill",
            )

        semaphore = asyncio.Semaphore(MAX_HISTORY_CONCURRENCY)
        end_date = date.today() - timedelta(days=1)
        tasks = [
            _backfill_single_stock(
                db_tools=db_tools,
                stock_row=row,
                stock_info_map=stock_info_map,
                end_date=end_date,
                scheduler_context=scheduler_context,
                semaphore=semaphore,
            )
            for row in universe_rows
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        affected = 0
        failed = 0
        for row, result in zip(universe_rows, results):
            if isinstance(result, Exception):
                failed += 1
                print(f"stock history failed for {row['prefixed_code']}: {result}")
                continue
            affected += int(result or 0)

        print(
            "stock backfill finished: "
            f"stocks={len(universe_rows)}, affected={affected}, failed={failed}"
        )
        refresh_start = min(
            resolve_stock_history_start_date(row, stock_info_map=stock_info_map)
            for row in universe_rows
        )
        quant_affected = await refresh_quant_index_dashboard_range(
            db_tools,
            start_date=refresh_start.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
        print(
            "stock backfill quant-index refresh: "
            f"start_date={refresh_start.strftime('%Y-%m-%d')}, "
            f"end_date={end_date.strftime('%Y-%m-%d')}, affected={quant_affected}"
        )
        return affected
    finally:
        if own_db:
            await db_tools.close()


async def repair_backfill_missing_history(selected_codes=None, db_tools=None):
    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    try:
        info_rows = await sync_stock_info_all(db_tools=db_tools)
        end_date = date.today() - timedelta(days=1)
        target_rows = build_target_stock_rows(
            info_rows,
            selected_codes=selected_codes,
            listed_on_or_before=end_date,
        )
        if not target_rows:
            print("stock repair-backfill finished: no stocks matched stock_info_all")
            return 0

        hist_prefixed_codes = set(await db_tools.get_stock_daily_hist_prefixed_codes())
        missing_rows = [
            row for row in target_rows
            if normalize_prefixed_code(row.get("prefixed_code")) not in hist_prefixed_codes
        ]
        if not missing_rows:
            print(
                "stock repair-backfill finished: "
                f"target_stocks={len(target_rows)}, missing=0"
            )
            return 0

        scheduler_context = SchedulerContext(workflow_name="stock_repair_backfill")
        stock_info_map = build_stock_info_map(info_rows)
        semaphore = asyncio.Semaphore(MAX_HISTORY_CONCURRENCY)
        tasks = [
            _backfill_single_stock(
                db_tools=db_tools,
                stock_row=row,
                stock_info_map=stock_info_map,
                end_date=end_date,
                scheduler_context=scheduler_context,
                semaphore=semaphore,
            )
            for row in missing_rows
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        affected = 0
        failed = 0
        for row, result in zip(missing_rows, results):
            if isinstance(result, Exception):
                failed += 1
                print(f"stock repair-backfill failed for {row['prefixed_code']}: {result}")
                continue
            affected += int(result or 0)

        print(
            "stock repair-backfill finished: "
            f"target_stocks={len(target_rows)}, missing={len(missing_rows)}, "
            f"affected={affected}, failed={failed}"
        )
        return affected
    finally:
        if own_db:
            await db_tools.close()


async def repair_daily_dates(trade_dates, selected_codes=None, db_tools=None):
    requested_trade_dates = parse_required_trade_dates(trade_dates)
    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    try:
        info_rows = await sync_stock_info_all(db_tools=db_tools)
        repair_end_date = parse_trade_date(max(requested_trade_dates))
        target_rows = build_target_stock_rows(
            info_rows,
            selected_codes=selected_codes,
            listed_on_or_before=repair_end_date,
        )
        if not target_rows:
            print("stock repair-daily-dates finished: no stocks matched stock_info_all")
            return 0

        stock_info_map = build_stock_info_map(info_rows)
        scheduler_context = SchedulerContext(workflow_name="stock_repair_daily_dates")
        semaphore = asyncio.Semaphore(MAX_HISTORY_CONCURRENCY)
        tasks = [
            _repair_daily_dates_single_stock(
                db_tools=db_tools,
                stock_row=row,
                stock_info_map=stock_info_map,
                requested_trade_dates=requested_trade_dates,
                repair_end_date=repair_end_date,
                scheduler_context=scheduler_context,
                semaphore=semaphore,
            )
            for row in target_rows
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        affected = 0
        failed = 0
        failed_codes = []
        for row, result in zip(target_rows, results):
            if isinstance(result, Exception):
                failed += 1
                failed_codes.append(row["prefixed_code"])
                print(f"stock repair-daily-dates failed for {row['prefixed_code']}: {result}")
                continue
            affected += int(result or 0)

        print(
            "stock repair-daily-dates finished: "
            f"trade_dates={','.join(requested_trade_dates)}, "
            f"target_stocks={len(target_rows)}, affected={affected}, failed={failed}"
        )
        if failed_codes:
            normalized_failed_codes = [normalize_prefixed_code(code) for code in failed_codes if normalize_prefixed_code(code)]
            failed_stock_codes = [normalize_stock_code(code) for code in normalized_failed_codes if normalize_stock_code(code)]
            print(
                "stock repair-daily-dates failed codes: "
                f"{','.join(normalized_failed_codes)}"
            )
            print(
                "stock repair-daily-dates rerun hint: "
                f"python run.py stock repair-daily-dates {' '.join(requested_trade_dates)} --codes {' '.join(failed_stock_codes)}"
            )
        quant_affected = await refresh_quant_index_dashboard_range(
            db_tools,
            start_date=min(requested_trade_dates),
            end_date=max(requested_trade_dates),
        )
        print(
            "stock repair-daily-dates quant-index refresh: "
            f"start_date={min(requested_trade_dates)}, end_date={max(requested_trade_dates)}, affected={quant_affected}"
        )
        return affected
    finally:
        if own_db:
            await db_tools.close()


async def repair_hist_metrics(start_date=None, end_date=None, selected_codes=None, db_tools=None):
    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    try:
        target_rows = await db_tools.get_stock_daily_hist_metric_targets(
            stock_codes=selected_codes,
            start_date=start_date,
            end_date=end_date,
        )
        if not target_rows:
            print("stock repair-hist-metrics finished: no stock_zh_a_hist_tx rows matched")
            return 0

        processed_rows = 0
        affected_rows = 0
        failed = 0
        failed_codes = []
        total_targets = len(target_rows)

        for index, target_row in enumerate(target_rows, start=1):
            prefixed_code = normalize_prefixed_code((target_row or {}).get("prefixed_code"))
            stock_code = normalize_stock_code((target_row or {}).get("stock_code"))
            try:
                history_rows = await db_tools.get_stock_daily_hist_rows_for_metric_repair(
                    prefixed_code=prefixed_code,
                    end_date=end_date,
                )
                if not history_rows:
                    continue

                update_rows = build_hist_metric_update_rows(
                    prefixed_code=prefixed_code,
                    history_rows=history_rows,
                    repair_start_date=start_date,
                    repair_end_date=end_date,
                )
                if not update_rows:
                    continue

                processed_rows += len(update_rows)
                affected_rows += await db_tools.update_stock_daily_hist_metrics(update_rows)
            except Exception as exc:
                failed += 1
                failed_codes.append(prefixed_code or stock_code)
                print(f"stock repair-hist-metrics failed for {prefixed_code or stock_code}: {exc}")
                continue

            if index % 200 == 0 or index == total_targets:
                print(
                    "stock repair-hist-metrics progress: "
                    f"stocks={index}/{total_targets}, processed_rows={processed_rows}, "
                    f"affected_rows={affected_rows}, failed={failed}"
                )

        print(
            "stock repair-hist-metrics finished: "
            f"start_date={start_date or '-'}, end_date={end_date or '-'}, "
            f"target_stocks={total_targets}, processed_rows={processed_rows}, "
            f"affected_rows={affected_rows}, failed={failed}"
        )
        if failed_codes:
            print(
                "stock repair-hist-metrics failed codes: "
                f"{','.join(code for code in failed_codes if code)}"
            )
        return affected_rows
    finally:
        if own_db:
            await db_tools.close()


async def collect_hfq_for_request(stock_code, start_date=None, end_date=None, db_tools=None):
    normalized_code = normalize_stock_code(stock_code)
    if not normalized_code:
        raise ValueError("stock_code must be a 6-digit code")

    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    try:
        info_rows = await db_tools.get_stock_info_rows_by_codes([normalized_code])
        info_row = info_rows[0] if info_rows else {}
        prefixed_code = normalize_prefixed_code(info_row.get("prefixed_code")) or build_prefixed_code(normalized_code)
        stock_name = normalize_text(info_row.get("stock_name"))
        list_date = parse_trade_date(info_row.get("list_date")) or HISTORY_FALLBACK_START_DATE

        request_start = parse_trade_date(start_date) or list_date
        effective_start = max(request_start, list_date, HISTORY_FALLBACK_START_DATE)
        effective_end = parse_trade_date(end_date) or date.today()
        if effective_start > effective_end:
            raise ValueError("start_date can not be later than end_date")

        existing_window = await db_tools.get_stock_hfq_request_window(prefixed_code)
        effective_start_text = effective_start.strftime("%Y-%m-%d")
        effective_end_text = effective_end.strftime("%Y-%m-%d")
        if (
            existing_window
            and existing_window.get("request_start_date") == effective_start_text
            and existing_window.get("request_end_date") == effective_end_text
        ):
            return {
                "status": "UNCHANGED",
                "stock_code": normalized_code,
                "prefixed_code": prefixed_code,
                "effective_start_date": effective_start_text,
                "effective_end_date": effective_end_text,
                "refreshed": False,
                "unchanged": True,
                "deleted_rows": 0,
                "written_rows": 0,
            }

        hfq_df = await asyncio.to_thread(
            get_stock_hfq_daily,
            prefixed_code,
            effective_start,
            effective_end,
        )
        if hfq_df is None or hfq_df.empty:
            raise ValueError(f"no hfq data returned for {prefixed_code}")

        refresh_batch_id = uuid.uuid4().hex
        rows = build_hfq_rows(
            prefixed_code=prefixed_code,
            stock_name=stock_name,
            request_start_date=effective_start_text,
            request_end_date=effective_end_text,
            refresh_batch_id=refresh_batch_id,
            hfq_df=hfq_df,
        )
        deleted_rows, written_rows = await db_tools.replace_stock_hfq_daily_data(prefixed_code, rows)
        return {
            "status": "SUCCESS",
            "stock_code": normalized_code,
            "prefixed_code": prefixed_code,
            "effective_start_date": effective_start_text,
            "effective_end_date": effective_end_text,
            "refreshed": True,
            "unchanged": False,
            "deleted_rows": deleted_rows,
            "written_rows": written_rows,
        }
    finally:
        if own_db:
            await db_tools.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "backfill"
    args = sys.argv[2:]

    if command == "backfill":
        await backfill_history(selected_codes=args or None)
        return
    if command == "daily":
        await sync_daily(selected_codes=args or None)
        return
    if command == "exchange-official-daily":
        await sync_exchange_official_daily(selected_codes=args or None)
        return
    if command == "repair-backfill":
        await repair_backfill_missing_history(selected_codes=args or None)
        return
    if command == "repair-daily-dates":
        trade_dates, selected_codes = parse_repair_daily_dates_cli_args(args)
        await repair_daily_dates(trade_dates, selected_codes=selected_codes)
        return
    if command == "repair-hist-metrics":
        start_date, end_date, selected_codes = parse_hist_metric_cli_args(args)
        await repair_hist_metrics(start_date=start_date, end_date=end_date, selected_codes=selected_codes)
        return

    raise ValueError(
        "stock supports: backfill [stock_code ...] | daily [stock_code ...] | "
        "exchange-official-daily [stock_code ...] | "
        "repair-backfill [stock_code ...] | "
        "repair-daily-dates <YYYY-MM-DD> [YYYY-MM-DD ...] [--codes <stock_code ...>] | "
        "repair-hist-metrics [start_date] [end_date] [--codes <stock_code ...>]"
    )


if __name__ == "__main__":
    asyncio.run(main())
