import asyncio
import json
import re
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd

from akshare_project.core.ak_scheduler_client import SchedulerContext
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.paths import get_state_path
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
HISTORY_FALLBACK_START_DATE = date(1991, 1, 1)
MAX_HISTORY_CONCURRENCY = 8

LOGGER = get_logger("stock")
STOCK_INFO_SYNC_STATE_PATH = get_state_path("stock_info_all", suffix="daily-sync")

SH_SOURCES = ["主板A股", "主板B股", "科创板"]
SZ_SOURCES = ["A股列表", "B股列表", "CDR列表", "AB股列表"]


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


def get_stock_spot(return_scheduler_meta=False):
    return fetch_with_retry(
        ak.stock_zh_a_spot,
        return_scheduler_meta=return_scheduler_meta,
        request_key="stock_zh_a_spot:all",
    )


def get_stock_history_tx(prefixed_code, start_date, end_date, scheduler_context=None):
    request_key = f"stock_zh_a_hist_tx:{normalize_prefixed_code(prefixed_code)}:{format_ak_date(start_date)}:{format_ak_date(end_date)}"
    return fetch_with_retry(
        ak.stock_zh_a_hist_tx,
        symbol=normalize_prefixed_code(prefixed_code),
        start_date=format_ak_date(start_date),
        end_date=format_ak_date(end_date),
        adjust="",
        scheduler_context=scheduler_context,
        request_key=request_key,
    )


def get_stock_qfq_daily(prefixed_code, start_date, end_date):
    request_key = f"stock_zh_a_daily:{normalize_prefixed_code(prefixed_code)}:{format_ak_date(start_date)}:{format_ak_date(end_date)}:qfq"
    return fetch_with_retry(
        ak.stock_zh_a_daily,
        symbol=normalize_prefixed_code(prefixed_code),
        start_date=format_ak_date(start_date),
        end_date=format_ak_date(end_date),
        adjust="qfq",
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
    security_type = "B" if "B股" in symbol else "A"
    for _, row in df.iterrows():
        record = build_info_record(
            stock_code=row.get("证券代码"),
            market_prefix="sh",
            exchange="SH",
            board=symbol,
            security_type=security_type,
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
        board=pick_first(row.get("板块"), symbol),
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
        if symbol == "AB股列表":
            for security_label in ("A股", "B股"):
                record = build_sz_variant_record(row, symbol, security_label)
                if record:
                    records.append(record)
            continue

        if symbol == "A股列表":
            record = build_sz_variant_record(row, symbol, "A股")
        elif symbol == "B股列表":
            record = build_sz_variant_record(row, symbol, "B股")
        elif symbol == "CDR列表":
            record = build_sz_variant_record(row, symbol, "CDR")
        else:
            record = None

        if record:
            records.append(record)
            continue

        generic_record = build_info_record(
            stock_code=pick_first(row.get("证券代码"), row.get("代码"), row.get("股票代码")),
            market_prefix="sz",
            exchange="SZ",
            board=pick_first(row.get("板块"), symbol),
            security_type="A",
            stock_name=pick_first(row.get("证券简称"), row.get("简称"), row.get("名称")),
            list_date=pick_first(row.get("上市日期"), row.get("A股上市日期"), row.get("CDR上市日期")),
            industry=row.get("所属行业"),
            total_share_capital=pick_first(row.get("总股本"), row.get("A股总股本")),
            circulating_share_capital=pick_first(row.get("流通股本"), row.get("A股流通股本")),
            source_name=f"stock_info_sz_name_code:{symbol}",
            raw_record=row.to_dict(),
        )
        if generic_record:
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

    return list(merged.values())


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
    trade_date = datetime.now().strftime("%Y-%m-%d")
    rows = []
    for _, row in spot_df.iterrows():
        stock_code = normalize_stock_code(row.get("代码"))
        if not stock_code or (selected and stock_code not in selected):
            continue
        prefixed_code = resolve_prefixed_code(stock_code, stock_info_map=stock_info_map)
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
            "snapshot_time": normalize_snapshot_time(row.get("时间戳")),
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


def build_qfq_rows(prefixed_code, stock_name, request_start_date, request_end_date, refresh_batch_id, qfq_df):
    rows = []
    stock_code = normalize_stock_code(prefixed_code)
    normalized_prefixed_code = normalize_prefixed_code(prefixed_code)
    request_start = normalize_trade_date_text(request_start_date)
    request_end = normalize_trade_date_text(request_end_date)
    for _, row in qfq_df.iterrows():
        trade_date = normalize_trade_date_text(row.get("date") or getattr(row, "name", None))
        if not trade_date:
            continue
        rows.append({
            "stock_code": stock_code,
            "prefixed_code": normalized_prefixed_code,
            "stock_name": stock_name,
            "trade_date": trade_date,
            "open_price": row.get("open"),
            "close_price": row.get("close"),
            "high_price": row.get("high"),
            "low_price": row.get("low"),
            "volume": row.get("volume"),
            "turnover_amount": row.get("amount"),
            "outstanding_share": row.get("outstanding_share"),
            "turnover_rate": row.get("turnover"),
            "data_source": "stock_zh_a_daily_qfq",
            "request_start_date": request_start,
            "request_end_date": request_end,
            "refresh_batch_id": refresh_batch_id,
        })
    return rows


async def sync_stock_info_all(db_tools=None, force=False):
    own_db = db_tools is None
    db_tools = db_tools or DbTools()
    if own_db:
        await db_tools.init_pool()
    try:
        today_marker = date.today().strftime("%Y-%m-%d")
        if not force and load_stock_info_sync_marker() == today_marker:
            existing_rows = await db_tools.get_all_stock_info_rows()
            if existing_rows:
                print(f"stock info already synced today ({today_marker}), skipping refresh")
                return existing_rows

        merged_rows = await load_all_stock_info_records()
        affected = await db_tools.upsert_stock_info_all(merged_rows)
        save_stock_info_sync_marker(today_marker)
        print(f"stock info synced: rows={len(merged_rows)}, affected={affected}")
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
        stock_info_map = build_stock_info_map(info_rows)
        spot_df = await asyncio.to_thread(get_stock_spot)
        rows = build_spot_snapshot_rows(spot_df, selected_codes=selected_codes, stock_info_map=stock_info_map)
        affected = await db_tools.upsert_stock_daily_data(rows)
        print(f"stock daily finished: rows={len(rows)}, affected={affected}")
        return affected
    finally:
        if own_db:
            await db_tools.close()


async def _backfill_single_stock(db_tools, stock_row, stock_info_map, end_date, scheduler_context=None, semaphore=None):
    async with semaphore:
        stock_code = stock_row["stock_code"]
        prefixed_code = stock_row["prefixed_code"]
        stock_name = stock_row.get("stock_name")
        info_row = stock_info_map.get(stock_code) or stock_info_map.get(prefixed_code)
        list_date = parse_trade_date((info_row or {}).get("list_date")) or HISTORY_FALLBACK_START_DATE
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
            return 0
        rows = build_hist_tx_rows(prefixed_code, stock_name or (info_row or {}).get("stock_name"), history_df)
        return await db_tools.upsert_stock_daily_data(rows)


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
        universe_rows = build_spot_snapshot_rows(
            spot_df,
            selected_codes=selected_codes,
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
        return affected
    finally:
        if own_db:
            await db_tools.close()


async def collect_qfq_for_request(stock_code, start_date=None, end_date=None, db_tools=None):
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

        existing_window = await db_tools.get_stock_qfq_request_window(prefixed_code)
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

        qfq_df = await asyncio.to_thread(
            get_stock_qfq_daily,
            prefixed_code,
            effective_start,
            effective_end,
        )
        if qfq_df is None or qfq_df.empty:
            raise ValueError(f"no qfq data returned for {prefixed_code}")

        refresh_batch_id = uuid.uuid4().hex
        rows = build_qfq_rows(
            prefixed_code=prefixed_code,
            stock_name=stock_name,
            request_start_date=effective_start_text,
            request_end_date=effective_end_text,
            refresh_batch_id=refresh_batch_id,
            qfq_df=qfq_df,
        )
        deleted_rows, written_rows = await db_tools.replace_stock_qfq_daily_data(prefixed_code, rows)
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
    selected_codes = sys.argv[2:]

    if command == "backfill":
        await backfill_history(selected_codes=selected_codes or None)
        return
    if command == "daily":
        await sync_daily(selected_codes=selected_codes or None)
        return

    raise ValueError("stock supports: backfill [stock_code ...] | daily [stock_code ...]")


if __name__ == "__main__":
    asyncio.run(main())
