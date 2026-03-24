import asyncio
import sys
from datetime import date, datetime, timedelta

import akshare as ak
import pandas as pd

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 8
BACKFILL_START_DATE = date(2005, 2, 23)
HIST_ADJUST = "qfq"
LOGGER = get_logger("etf")
PROGRESS_STORE = ProgressStore("etf")

COL_CODE = "\u4ee3\u7801"
COL_NAME = "\u540d\u79f0"
COL_LATEST = "\u6700\u65b0\u4ef7"
COL_IOPV = "IOPV\u5b9e\u65f6\u4f30\u503c"
COL_DISCOUNT_RATE = "\u57fa\u91d1\u6298\u4ef7\u7387"
COL_CHANGE_AMOUNT = "\u6da8\u8dcc\u989d"
COL_CHANGE_RATE = "\u6da8\u8dcc\u5e45"
COL_VOLUME = "\u6210\u4ea4\u91cf"
COL_TURNOVER = "\u6210\u4ea4\u989d"
COL_OPEN = "\u5f00\u76d8\u4ef7"
COL_HIGH = "\u6700\u9ad8\u4ef7"
COL_LOW = "\u6700\u4f4e\u4ef7"
COL_PRE_CLOSE = "\u6628\u6536"
COL_AMPLITUDE = "\u632f\u5e45"
COL_TURNOVER_RATE = "\u6362\u624b\u7387"
COL_VOLUME_RATIO = "\u91cf\u6bd4"
COL_CURRENT_HAND = "\u73b0\u624b"
COL_BID1 = "\u4e70\u4e00"
COL_ASK1 = "\u5356\u4e00"
COL_OUTER = "\u5916\u76d8"
COL_INNER = "\u5185\u76d8"
COL_MAIN_NET_INFLOW = "\u4e3b\u529b\u51c0\u6d41\u5165-\u51c0\u989d"
COL_MAIN_NET_INFLOW_RATIO = "\u4e3b\u529b\u51c0\u6d41\u5165-\u51c0\u5360\u6bd4"
COL_EXTRA_LARGE_NET_INFLOW = "\u8d85\u5927\u5355\u51c0\u6d41\u5165-\u51c0\u989d"
COL_EXTRA_LARGE_NET_INFLOW_RATIO = "\u8d85\u5927\u5355\u51c0\u6d41\u5165-\u51c0\u5360\u6bd4"
COL_LARGE_NET_INFLOW = "\u5927\u5355\u51c0\u6d41\u5165-\u51c0\u989d"
COL_LARGE_NET_INFLOW_RATIO = "\u5927\u5355\u51c0\u6d41\u5165-\u51c0\u5360\u6bd4"
COL_MEDIUM_NET_INFLOW = "\u4e2d\u5355\u51c0\u6d41\u5165-\u51c0\u989d"
COL_MEDIUM_NET_INFLOW_RATIO = "\u4e2d\u5355\u51c0\u6d41\u5165-\u51c0\u5360\u6bd4"
COL_SMALL_NET_INFLOW = "\u5c0f\u5355\u51c0\u6d41\u5165-\u51c0\u989d"
COL_SMALL_NET_INFLOW_RATIO = "\u5c0f\u5355\u51c0\u6d41\u5165-\u51c0\u5360\u6bd4"
COL_LATEST_SHARE = "\u6700\u65b0\u4efd\u989d"
COL_CIRCULATING_MARKET_VALUE = "\u6d41\u901a\u5e02\u503c"
COL_TOTAL_MARKET_VALUE = "\u603b\u5e02\u503c"
COL_SPOT_DATA_DATE = "\u6570\u636e\u65e5\u671f"
COL_SPOT_UPDATE_TIME = "\u66f4\u65b0\u65f6\u95f4"

COL_HIST_DATE = "\u65e5\u671f"
COL_HIST_OPEN = "\u5f00\u76d8"
COL_HIST_CLOSE = "\u6536\u76d8"
COL_HIST_HIGH = "\u6700\u9ad8"
COL_HIST_LOW = "\u6700\u4f4e"
COL_HIST_VOLUME = "\u6210\u4ea4\u91cf"
COL_HIST_TURNOVER = "\u6210\u4ea4\u989d"
COL_HIST_AMPLITUDE = "\u632f\u5e45"
COL_HIST_CHANGE_RATE = "\u6da8\u8dcc\u5e45"
COL_HIST_CHANGE_AMOUNT = "\u6da8\u8dcc\u989d"
COL_HIST_TURNOVER_RATE = "\u6362\u624b\u7387"


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def save_progress_batch(progress_lines):
    PROGRESS_STORE.append_lines(progress_lines)


def log_error(etf_code, trade_date, error_message):
    LOGGER.error("%s,%s,%s", etf_code, trade_date, error_message)


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    return shared_fetch_with_retry(
        func,
        *args,
        retries=retries,
        sleep_seconds=sleep_seconds,
        logger=LOGGER,
        **kwargs,
    )


def normalize_etf_code(value):
    return str(value or "").strip()


def normalize_trade_date(value):
    if value is None or pd.isna(value):
        return ""
    if hasattr(value, "date"):
        try:
            return value.date().strftime("%Y-%m-%d")
        except Exception:
            pass
    text = str(value).split(" ")[0].strip()
    if not text:
        return ""
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def normalize_datetime_value(value):
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if hasattr(value, "tzinfo") and getattr(value, "tzinfo", None) is not None:
        value = value.replace(tzinfo=None)
    return value


def get_etf_spot():
    return fetch_with_retry(ak.fund_etf_spot_em)


def get_etf_history(etf_code, start_date, end_date):
    return fetch_with_retry(
        ak.fund_etf_hist_em,
        symbol=etf_code,
        period="daily",
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        adjust=HIST_ADJUST,
    )


def build_etf_basic_rows(spot_df):
    rows = []
    seen_codes = set()
    for _, row in spot_df.iterrows():
        etf_code = normalize_etf_code(row.get(COL_CODE))
        if not etf_code or etf_code in seen_codes:
            continue
        seen_codes.add(etf_code)
        rows.append({
            "etf_code": etf_code,
            "etf_name": str(row.get(COL_NAME, "")).strip() or None,
        })
    return rows


def filter_spot_df_by_codes(spot_df, selected_codes=None):
    normalized_codes = [normalize_etf_code(code) for code in (selected_codes or []) if normalize_etf_code(code)]
    if not normalized_codes:
        return spot_df, []

    selected_set = set(normalized_codes)
    filtered_df = spot_df[spot_df[COL_CODE].map(normalize_etf_code).isin(selected_set)]
    available_codes = {normalize_etf_code(code) for code in filtered_df[COL_CODE].tolist()}
    missing_codes = [code for code in normalized_codes if code not in available_codes]
    return filtered_df, missing_codes


def build_etf_spot_daily_rows(spot_df):
    rows = []
    today_text = datetime.now().strftime("%Y-%m-%d")

    for _, row in spot_df.iterrows():
        etf_code = normalize_etf_code(row.get(COL_CODE))
        if not etf_code:
            continue

        trade_date = normalize_trade_date(row.get(COL_SPOT_DATA_DATE)) or today_text
        rows.append({
            "etf_code": etf_code,
            "etf_name": str(row.get(COL_NAME, "")).strip() or None,
            "trade_date": trade_date,
            "open_price": row.get(COL_OPEN),
            "close_price": row.get(COL_LATEST),
            "high_price": row.get(COL_HIGH),
            "low_price": row.get(COL_LOW),
            "volume": row.get(COL_VOLUME),
            "turnover": row.get(COL_TURNOVER),
            "amplitude": row.get(COL_AMPLITUDE),
            "price_change_rate": row.get(COL_CHANGE_RATE),
            "price_change_amount": row.get(COL_CHANGE_AMOUNT),
            "turnover_rate": row.get(COL_TURNOVER_RATE),
            "pre_close_price": row.get(COL_PRE_CLOSE),
            "iopv_realtime": row.get(COL_IOPV),
            "discount_rate": row.get(COL_DISCOUNT_RATE),
            "volume_ratio": row.get(COL_VOLUME_RATIO),
            "current_hand": row.get(COL_CURRENT_HAND),
            "bid1_price": row.get(COL_BID1),
            "ask1_price": row.get(COL_ASK1),
            "outer_volume": row.get(COL_OUTER),
            "inner_volume": row.get(COL_INNER),
            "latest_share": row.get(COL_LATEST_SHARE),
            "circulating_market_value": row.get(COL_CIRCULATING_MARKET_VALUE),
            "total_market_value": row.get(COL_TOTAL_MARKET_VALUE),
            "main_net_inflow": row.get(COL_MAIN_NET_INFLOW),
            "main_net_inflow_ratio": row.get(COL_MAIN_NET_INFLOW_RATIO),
            "extra_large_net_inflow": row.get(COL_EXTRA_LARGE_NET_INFLOW),
            "extra_large_net_inflow_ratio": row.get(COL_EXTRA_LARGE_NET_INFLOW_RATIO),
            "large_net_inflow": row.get(COL_LARGE_NET_INFLOW),
            "large_net_inflow_ratio": row.get(COL_LARGE_NET_INFLOW_RATIO),
            "medium_net_inflow": row.get(COL_MEDIUM_NET_INFLOW),
            "medium_net_inflow_ratio": row.get(COL_MEDIUM_NET_INFLOW_RATIO),
            "small_net_inflow": row.get(COL_SMALL_NET_INFLOW),
            "small_net_inflow_ratio": row.get(COL_SMALL_NET_INFLOW_RATIO),
            "spot_data_date": trade_date,
            "spot_update_time": normalize_datetime_value(row.get(COL_SPOT_UPDATE_TIME)),
            "data_source": "fund_etf_spot_em",
            "adjust_type": None,
        })

    return rows


def build_etf_hist_rows(etf_code, etf_name, history_df):
    rows = []
    for _, row in history_df.iterrows():
        trade_date = normalize_trade_date(row.get(COL_HIST_DATE))
        if not trade_date:
            continue

        rows.append({
            "etf_code": etf_code,
            "etf_name": etf_name,
            "trade_date": trade_date,
            "open_price": row.get(COL_HIST_OPEN),
            "close_price": row.get(COL_HIST_CLOSE),
            "high_price": row.get(COL_HIST_HIGH),
            "low_price": row.get(COL_HIST_LOW),
            "volume": row.get(COL_HIST_VOLUME),
            "turnover": row.get(COL_HIST_TURNOVER),
            "amplitude": row.get(COL_HIST_AMPLITUDE),
            "price_change_rate": row.get(COL_HIST_CHANGE_RATE),
            "price_change_amount": row.get(COL_HIST_CHANGE_AMOUNT),
            "turnover_rate": row.get(COL_HIST_TURNOVER_RATE),
            "pre_close_price": None,
            "iopv_realtime": None,
            "discount_rate": None,
            "volume_ratio": None,
            "current_hand": None,
            "bid1_price": None,
            "ask1_price": None,
            "outer_volume": None,
            "inner_volume": None,
            "latest_share": None,
            "circulating_market_value": None,
            "total_market_value": None,
            "main_net_inflow": None,
            "main_net_inflow_ratio": None,
            "extra_large_net_inflow": None,
            "extra_large_net_inflow_ratio": None,
            "large_net_inflow": None,
            "large_net_inflow_ratio": None,
            "medium_net_inflow": None,
            "medium_net_inflow_ratio": None,
            "small_net_inflow": None,
            "small_net_inflow_ratio": None,
            "spot_data_date": None,
            "spot_update_time": None,
            "data_source": "fund_etf_hist_em",
            "adjust_type": HIST_ADJUST,
        })
    return rows


async def process_history_symbol(symbol_row, db_tools, semaphore, progress_lock, progress_lines, mode_label, start_date, end_date):
    etf_code = symbol_row["etf_code"]
    etf_name = symbol_row.get("etf_name") or ""

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_etf_history, etf_code, start_date, end_date)

        if history_df is None or history_df.empty:
            async with progress_lock:
                progress_lines.append(f"{mode_label},{etf_code},{start_date},{end_date},0")
                await asyncio.to_thread(save_progress_batch, [progress_lines[-1]])
            print(f"{mode_label} {etf_code}: no history rows")
            return 0

        history_rows = build_etf_hist_rows(etf_code, etf_name, history_df)
        upserted = await db_tools.upsert_etf_daily_data(history_rows)
        async with progress_lock:
            progress_lines.append(f"{mode_label},{etf_code},{start_date},{end_date},{upserted}")
            await asyncio.to_thread(save_progress_batch, [progress_lines[-1]])
        print(f"{mode_label} {etf_code}: upserted {upserted}")
        return upserted
    except Exception as exc:
        error_message = f"{mode_label} {etf_code} failed: {exc}"
        print(error_message)
        log_error(etf_code, str(end_date), error_message)
        return 0


async def load_selected_etfs(selected_codes=None):
    spot_df = await asyncio.to_thread(get_etf_spot)
    if spot_df is None or spot_df.empty:
        print("No ETF spot data fetched.")
        return None, [], []

    filtered_df, missing_codes = filter_spot_df_by_codes(spot_df, selected_codes)
    basic_rows = build_etf_basic_rows(filtered_df)
    return filtered_df, basic_rows, missing_codes


async def sync_history(mode_label, selected_codes=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    progress_lock = asyncio.Lock()
    progress_lines = []

    try:
        _, basic_rows, missing_codes = await load_selected_etfs(selected_codes)
        if missing_codes:
            print(f"{mode_label} ignored unknown ETF codes: {', '.join(missing_codes)}")
        if not basic_rows:
            print(f"No ETF symbols matched for {mode_label}.")
            return 0

        basic_upserted = await db_tools.upsert_etf_basic_info(basic_rows)
        print(f"{mode_label} etf_basic_info upserted: {basic_upserted}")

        end_date = datetime.now().date() - timedelta(days=1)
        tasks = [
            process_history_symbol(
                symbol_row,
                db_tools,
                semaphore,
                progress_lock,
                progress_lines,
                mode_label,
                BACKFILL_START_DATE,
                end_date,
            )
            for symbol_row in basic_rows
        ]
        results = await asyncio.gather(*tasks)
        total_upserted = sum(results)
        print(
            f"{mode_label} finished, "
            f"symbols={len(basic_rows)}, "
            f"upserted_rows={total_upserted}"
        )
        return total_upserted
    finally:
        await db_tools.close()


async def backfill_history(selected_codes=None):
    return await sync_history("etf_backfill", selected_codes)


async def weekly_repair(selected_codes=None):
    return await sync_history("etf_weekly_repair", selected_codes)


async def sync_daily(selected_codes=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        filtered_df, basic_rows, missing_codes = await load_selected_etfs(selected_codes)
        if missing_codes:
            print(f"etf_daily ignored unknown ETF codes: {', '.join(missing_codes)}")
        if filtered_df is None or filtered_df.empty or not basic_rows:
            print("No ETF symbols matched for daily sync.")
            return 0

        daily_rows = build_etf_spot_daily_rows(filtered_df)
        basic_upserted = await db_tools.upsert_etf_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_etf_daily_data(daily_rows)
        trade_dates = sorted({row["trade_date"] for row in daily_rows if row.get("trade_date")})
        print(
            "etf daily finished, "
            f"etf_basic_info upserted: {basic_upserted}, "
            f"etf_daily_data upserted: {daily_upserted}, "
            f"trade_dates: {','.join(trade_dates) if trade_dates else 'NONE'}"
        )
        return daily_upserted
    finally:
        await db_tools.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "backfill"
    selected_codes = sys.argv[2:] if len(sys.argv) > 2 else []

    if command == "backfill":
        await backfill_history(selected_codes)
        return
    if command == "daily":
        await sync_daily(selected_codes)
        return
    if command == "weekly-repair":
        await weekly_repair(selected_codes)
        return

    raise ValueError(
        "usage: python run.py etf [backfill|daily|weekly-repair] [ETF_CODE ...]"
    )


if __name__ == "__main__":
    asyncio.run(main())
