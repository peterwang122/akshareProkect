import asyncio
import re
import sys
from datetime import date, datetime, timedelta

import akshare as ak

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

    raise ValueError(
        "usage: python run.py futures backfill [start_date] [end_date] [HIST_SYMBOL ...]\n"
        "   or: python run.py futures daily [trade_date|start_date end_date] [HIST_SYMBOL ...]\n"
        "   or: python run.py futures trade-date YYYY-MM-DD\n"
        "   or: python run.py futures market-backfill [start_date] [end_date]\n"
        "   or: python run.py futures market-daily [trade_date|start_date end_date]\n"
        "   or: python run.py futures hist-backfill [start_date] [end_date] [HIST_SYMBOL ...]\n"
        "   or: python run.py futures hist-daily [trade_date|start_date end_date] [HIST_SYMBOL ...]"
    )


if __name__ == "__main__":
    asyncio.run(main())
