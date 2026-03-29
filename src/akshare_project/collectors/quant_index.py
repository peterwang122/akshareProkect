import asyncio
import sys
from datetime import datetime

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools

LOGGER = get_logger("quant_index")

INDEX_NAME_ORDER = [
    "上证指数",
    "上证50",
    "沪深300",
    "中证500",
    "中证1000",
]
CORE_INDEX_NAMES = INDEX_NAME_ORDER[1:]
INDEX_CODE_FALLBACKS = {
    "上证指数": "sh000001",
    "上证50": "sh000016",
    "沪深300": "sh000300",
    "中证500": "sh000905",
    "中证1000": "sh000852",
}
INDEX_FUTURES_SYMBOLS = {
    "上证50": {"main_symbol": "IHM", "month_symbol": "IHM0"},
    "沪深300": {"main_symbol": "IFM", "month_symbol": "IFM0"},
    "中证500": {"main_symbol": "ICM", "month_symbol": "ICM0"},
    "中证1000": {"main_symbol": "IMM", "month_symbol": "IMM0"},
}
FUTURES_SOURCE_PRIORITY = {
    "get_futures_daily_derived": 0,
    "futures_hist_em": 1,
}


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def normalize_date_text(value):
    if value is None:
        return None
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%d")
        except Exception:
            pass
    text = str(value).strip().split(" ")[0]
    return text or None


def parse_date_arg(value):
    text = str(value or "").strip()
    if not text:
        return None
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value}")


def to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value):
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def average_or_default(values, default_value):
    valid_values = [float(value) for value in values if value is not None]
    if not valid_values:
        return default_value
    return sum(valid_values) / len(valid_values)


def build_index_close_map(rows):
    result = {}
    for row in rows:
        index_name = str(row.get("index_name", "")).strip()
        trade_date = normalize_date_text(row.get("trade_date"))
        close_price = to_float(row.get("close_price"))
        if not index_name or not trade_date or close_price is None:
            continue
        result[(trade_date, index_name)] = close_price
    return result


def build_emotion_map(rows):
    result = {}
    for row in rows:
        trade_date = normalize_date_text(row.get("emotion_date"))
        index_name = str(row.get("index_name", "")).strip()
        emotion_value = to_float(row.get("emotion_value"))
        if not trade_date or not index_name or emotion_value is None:
            continue
        result[(trade_date, index_name)] = emotion_value
    return result


def build_futures_close_map(rows):
    best_rows = {}
    for row in rows:
        trade_date = normalize_date_text(row.get("trade_date"))
        symbol = str(row.get("symbol", "")).strip().upper()
        close_price = to_float(row.get("close_price"))
        data_source = str(row.get("data_source", "")).strip()
        source_priority = FUTURES_SOURCE_PRIORITY.get(data_source)
        if not trade_date or not symbol or close_price is None or source_priority is None:
            continue

        row_key = (trade_date, symbol)
        current = best_rows.get(row_key)
        if current is None or source_priority < current["source_priority"]:
            best_rows[row_key] = {
                "close_price": close_price,
                "source_priority": source_priority,
            }

    return {
        row_key: payload["close_price"]
        for row_key, payload in best_rows.items()
    }


def build_breadth_map(rows):
    result = {}
    for row in rows:
        trade_date = normalize_date_text(row.get("trade_date"))
        if not trade_date:
            continue
        up_count = to_int(row.get("breadth_up_count"))
        total_count = to_int(row.get("breadth_total_count"))
        up_pct = (up_count / total_count * 100) if total_count else 0
        result[trade_date] = {
            "breadth_up_count": up_count,
            "breadth_total_count": total_count,
            "breadth_up_pct": up_pct,
        }

    return result


async def resolve_index_codes(db_tools):
    code_map = dict(INDEX_CODE_FALLBACKS)
    db_code_map = await db_tools.get_index_codes_by_names(INDEX_NAME_ORDER)
    for index_name, index_code in db_code_map.items():
        if index_code:
            code_map[index_name] = index_code
    return code_map


def build_dashboard_rows(trade_dates, index_code_map, emotion_map, index_close_map, futures_close_map, breadth_map):
    rows = []
    for trade_date in trade_dates:
        raw_core_emotions = {
            index_name: emotion_map.get((trade_date, index_name))
            for index_name in CORE_INDEX_NAMES
        }
        raw_core_basis = {}
        for index_name in CORE_INDEX_NAMES:
            index_close = index_close_map.get((trade_date, index_name))
            symbol_meta = INDEX_FUTURES_SYMBOLS[index_name]
            main_close = futures_close_map.get((trade_date, symbol_meta["main_symbol"]))
            month_close = futures_close_map.get((trade_date, symbol_meta["month_symbol"]))
            raw_core_basis[index_name] = {
                "main_basis": (main_close - index_close) if main_close is not None and index_close is not None else None,
                "month_basis": (month_close - index_close) if month_close is not None and index_close is not None else None,
            }

        sse_emotion = average_or_default(raw_core_emotions.values(), 50)
        sse_main_basis = average_or_default(
            [raw_core_basis[index_name]["main_basis"] for index_name in CORE_INDEX_NAMES],
            0,
        )
        sse_month_basis = average_or_default(
            [raw_core_basis[index_name]["month_basis"] for index_name in CORE_INDEX_NAMES],
            0,
        )
        breadth = breadth_map.get(
            trade_date,
            {
                "breadth_up_count": 0,
                "breadth_total_count": 0,
                "breadth_up_pct": 0,
            },
        )

        for index_name in INDEX_NAME_ORDER:
            if index_name == "上证指数":
                emotion_value = sse_emotion
                main_basis = sse_main_basis
                month_basis = sse_month_basis
            else:
                emotion_value = raw_core_emotions.get(index_name)
                emotion_value = 50 if emotion_value is None else emotion_value
                main_basis = raw_core_basis[index_name]["main_basis"]
                month_basis = raw_core_basis[index_name]["month_basis"]
                main_basis = 0 if main_basis is None else main_basis
                month_basis = 0 if month_basis is None else month_basis

            rows.append({
                "trade_date": trade_date,
                "index_code": index_code_map.get(index_name) or INDEX_CODE_FALLBACKS[index_name],
                "index_name": index_name,
                "emotion_value": emotion_value,
                "main_basis": main_basis,
                "month_basis": month_basis,
                "breadth_up_count": breadth["breadth_up_count"],
                "breadth_total_count": breadth["breadth_total_count"],
                "breadth_up_pct": breadth["breadth_up_pct"],
            })

    return rows


async def compute_and_upsert_range(db_tools, start_date, end_date):
    trade_dates = await db_tools.get_quant_index_dashboard_trade_dates(
        INDEX_NAME_ORDER,
        start_date=start_date,
        end_date=end_date,
    )
    if not trade_dates:
        print(f"quant index dashboard: no trade dates found for {start_date} -> {end_date}")
        return 0

    index_code_map = await resolve_index_codes(db_tools)
    index_close_rows = await db_tools.get_quant_index_dashboard_index_closes(
        INDEX_NAME_ORDER,
        start_date,
        end_date,
    )
    emotion_rows = await db_tools.get_quant_index_dashboard_emotions(
        CORE_INDEX_NAMES,
        start_date,
        end_date,
    )
    futures_rows = await db_tools.get_quant_index_dashboard_futures_closes(
        [symbol for item in INDEX_FUTURES_SYMBOLS.values() for symbol in (item["main_symbol"], item["month_symbol"])],
        start_date,
        end_date,
    )
    breadth_rows = await db_tools.get_quant_index_dashboard_breadth(start_date, end_date)

    rows = build_dashboard_rows(
        trade_dates=trade_dates,
        index_code_map=index_code_map,
        emotion_map=build_emotion_map(emotion_rows),
        index_close_map=build_index_close_map(index_close_rows),
        futures_close_map=build_futures_close_map(futures_rows),
        breadth_map=build_breadth_map(breadth_rows),
    )
    affected = await db_tools.upsert_quant_index_dashboard_daily(rows)
    print(
        "quant index dashboard sync finished: "
        f"start_date={start_date}, end_date={end_date}, trade_dates={len(trade_dates)}, affected={affected}"
    )
    return affected


async def backfill_history(start_date=None, end_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        if start_date is None or end_date is None:
            trade_dates = await db_tools.get_quant_index_dashboard_trade_dates(INDEX_NAME_ORDER)
            if not trade_dates:
                print("quant index dashboard backfill finished: no index trade dates found")
                return 0
            actual_start = start_date or trade_dates[0]
            actual_end = end_date or trade_dates[-1]
        else:
            actual_start = start_date
            actual_end = end_date
        return await compute_and_upsert_range(db_tools, actual_start, actual_end)
    finally:
        await db_tools.close()


async def sync_daily(target_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        actual_date = target_date or await db_tools.get_latest_quant_index_trade_date(INDEX_NAME_ORDER)
        if not actual_date:
            print("quant index dashboard daily finished: no latest trade date found")
            return 0
        return await compute_and_upsert_range(db_tools, actual_date, actual_date)
    finally:
        await db_tools.close()


async def refresh_breadth_data(start_date=None, end_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        if start_date is None or end_date is None:
            trade_dates = await db_tools.get_quant_index_dashboard_trade_dates(INDEX_NAME_ORDER)
            if not trade_dates:
                print("quant index breadth refresh finished: no index trade dates found")
                return 0
            actual_start = start_date or trade_dates[0]
            actual_end = end_date or trade_dates[-1]
        else:
            actual_start = start_date
            actual_end = end_date

        affected = await compute_and_upsert_range(db_tools, actual_start, actual_end)
        print(
            "quant index breadth refresh finished: "
            f"start_date={actual_start}, end_date={actual_end}, affected={affected}"
        )
        return affected
    finally:
        await db_tools.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "backfill"
    args = sys.argv[2:]

    if command == "backfill":
        start_date = parse_date_arg(args[0]) if len(args) > 0 else None
        end_date = parse_date_arg(args[1]) if len(args) > 1 else None
        await backfill_history(start_date=start_date, end_date=end_date)
        return
    if command == "daily":
        target_date = parse_date_arg(args[0]) if args else None
        await sync_daily(target_date=target_date)
        return
    if command == "refresh-breadth":
        start_date = parse_date_arg(args[0]) if len(args) > 0 else None
        end_date = parse_date_arg(args[1]) if len(args) > 1 else None
        await refresh_breadth_data(start_date=start_date, end_date=end_date)
        return

    raise ValueError(
        "quant-index supports: backfill [start_date] [end_date] | "
        "daily [trade_date] | refresh-breadth [start_date] [end_date]"
    )


if __name__ == "__main__":
    asyncio.run(main())
