import asyncio
import sys
from datetime import datetime, timedelta

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
HK_INDEX_NAME_ORDER = [
    "恒生指数",
    "恒生中国企业指数",
    "恒生科技指数",
]
US_INDEX_NAME_ORDER = [
    "标普500指数",
    "纳斯达克100指数",
]
ALL_INDEX_NAME_ORDER = [*INDEX_NAME_ORDER, *HK_INDEX_NAME_ORDER, *US_INDEX_NAME_ORDER]
INDEX_CODE_FALLBACKS = {
    "上证指数": "sh000001",
    "上证50": "sh000016",
    "沪深300": "sh000300",
    "中证500": "sh000905",
    "中证1000": "sh000852",
    "恒生指数": "HSI",
    "恒生中国企业指数": "HSCEI",
    "恒生科技指数": "HSTECH",
    "标普500指数": ".INX",
    "纳斯达克100指数": ".NDX",
}
INDEX_FUTURES_SYMBOLS = {
    "上证50": {"main_symbol": "IHM", "month_symbol": "IHM0"},
    "沪深300": {"main_symbol": "IFM", "month_symbol": "IFM0"},
    "中证500": {"main_symbol": "ICM", "month_symbol": "ICM0"},
    "中证1000": {"main_symbol": "IMM", "month_symbol": "IMM0"},
}
HK_INDEX_FUTURES_SYMBOLS = {
    "恒生指数": "HSI",
    "恒生中国企业指数": "HHI",
    "恒生科技指数": "HTI",
}
US_INDEX_FUTURES_SYMBOLS = {
    "标普500指数": "ES",
    "纳斯达克100指数": "NQ",
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


def parse_trade_day_count_arg(value, default_value=10):
    if value is None:
        return default_value
    try:
        parsed_value = int(str(value).strip())
    except (TypeError, ValueError):
        raise ValueError(f"invalid trade day count: {value}")
    if parsed_value <= 0:
        raise ValueError(f"invalid trade day count: {value}")
    return parsed_value


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


def month_sort_value(value):
    text = str(value or "").strip()
    if not text:
        return "9999-99"
    return text[:7]


def build_hk_futures_basis_map(rows):
    grouped = {}
    for row in rows:
        trade_date = normalize_date_text(row.get("trade_date"))
        root_symbol = str(row.get("root_symbol", "")).strip().upper()
        close_price = to_float(row.get("close_price"))
        if not trade_date or not root_symbol or close_price is None:
            continue
        grouped.setdefault((trade_date, root_symbol), []).append(
            {
                "close_price": close_price,
                "contract_month": month_sort_value(row.get("contract_month")),
                "source_contract_code": str(row.get("source_contract_code", "")).strip().upper(),
                "volume": to_float(row.get("volume")),
                "open_interest": to_float(row.get("open_interest")),
            }
        )

    result = {}
    for row_key, candidates in grouped.items():
        month_contract = sorted(
            candidates,
            key=lambda item: (item["contract_month"], item["source_contract_code"]),
        )[0]
        main_contract = sorted(
            candidates,
            key=lambda item: (
                -(item["open_interest"] if item["open_interest"] is not None else -1),
                -(item["volume"] if item["volume"] is not None else -1),
                item["contract_month"],
                item["source_contract_code"],
            ),
        )[0]
        result[row_key] = {
            "main_close": main_contract["close_price"],
            "month_close": month_contract["close_price"],
        }
    return result


def build_us_futures_close_map(rows):
    result = {}
    for row in rows:
        trade_date = normalize_date_text(row.get("trade_date"))
        root_symbol = str(row.get("root_symbol", "")).strip().upper()
        close_price = to_float(row.get("close_price"))
        if not trade_date or not root_symbol or close_price is None:
            continue
        result[(trade_date, root_symbol)] = close_price
    return result


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
    for market, index_names in [
        ("cn", INDEX_NAME_ORDER),
        ("hk", HK_INDEX_NAME_ORDER),
        ("us", US_INDEX_NAME_ORDER),
    ]:
        db_code_map = await db_tools.get_index_codes_by_names_for_market(index_names, market=market)
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


def build_hk_dashboard_rows(trade_dates, index_code_map, index_close_map, futures_basis_map):
    rows = []
    for trade_date in trade_dates:
        for index_name in HK_INDEX_NAME_ORDER:
            root_symbol = HK_INDEX_FUTURES_SYMBOLS[index_name]
            index_close = index_close_map.get((trade_date, index_name))
            futures_basis = futures_basis_map.get((trade_date, root_symbol), {})
            main_close = futures_basis.get("main_close")
            month_close = futures_basis.get("month_close")
            main_basis = (main_close - index_close) if main_close is not None and index_close is not None else 0
            month_basis = (month_close - index_close) if month_close is not None and index_close is not None else 0
            rows.append({
                "trade_date": trade_date,
                "index_code": index_code_map.get(index_name) or INDEX_CODE_FALLBACKS[index_name],
                "index_name": index_name,
                "emotion_value": 50,
                "main_basis": main_basis,
                "month_basis": month_basis,
                "breadth_up_count": 0,
                "breadth_total_count": 0,
                "breadth_up_pct": 0,
            })
    return rows


def build_us_dashboard_rows(trade_dates, index_code_map, index_close_map, futures_close_map):
    rows = []
    for trade_date in trade_dates:
        for index_name in US_INDEX_NAME_ORDER:
            root_symbol = US_INDEX_FUTURES_SYMBOLS[index_name]
            index_close = index_close_map.get((trade_date, index_name))
            futures_close = futures_close_map.get((trade_date, root_symbol))
            main_basis = (futures_close - index_close) if futures_close is not None and index_close is not None else 0
            rows.append({
                "trade_date": trade_date,
                "index_code": index_code_map.get(index_name) or INDEX_CODE_FALLBACKS[index_name],
                "index_name": index_name,
                "emotion_value": 50,
                "main_basis": main_basis,
                "month_basis": 0,
                "breadth_up_count": 0,
                "breadth_total_count": 0,
                "breadth_up_pct": 0,
            })
    return rows


async def compute_and_upsert_range(db_tools, start_date, end_date):
    cn_trade_dates = await db_tools.get_quant_index_dashboard_trade_dates(
        INDEX_NAME_ORDER,
        start_date=start_date,
        end_date=end_date,
    )
    hk_trade_dates = await db_tools.get_quant_index_dashboard_trade_dates_for_market(
        HK_INDEX_NAME_ORDER,
        market="hk",
        start_date=start_date,
        end_date=end_date,
    )
    us_trade_dates = await db_tools.get_quant_index_dashboard_trade_dates_for_market(
        US_INDEX_NAME_ORDER,
        market="us",
        start_date=start_date,
        end_date=end_date,
    )
    if not cn_trade_dates and not hk_trade_dates and not us_trade_dates:
        print(f"quant index dashboard: no trade dates found for {start_date} -> {end_date}")
        return 0

    index_code_map = await resolve_index_codes(db_tools)
    cn_index_close_rows = await db_tools.get_quant_index_dashboard_index_closes(
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
        trade_dates=cn_trade_dates,
        index_code_map=index_code_map,
        emotion_map=build_emotion_map(emotion_rows),
        index_close_map=build_index_close_map(cn_index_close_rows),
        futures_close_map=build_futures_close_map(futures_rows),
        breadth_map=build_breadth_map(breadth_rows),
    )
    if hk_trade_dates:
        hk_index_close_rows = await db_tools.get_quant_index_dashboard_index_closes_for_market(
            HK_INDEX_NAME_ORDER,
            "hk",
            start_date,
            end_date,
        )
        hk_futures_rows = await db_tools.get_quant_index_dashboard_hk_index_futures_closes(
            HK_INDEX_FUTURES_SYMBOLS.values(),
            start_date,
            end_date,
        )
        rows.extend(
            build_hk_dashboard_rows(
                trade_dates=hk_trade_dates,
                index_code_map=index_code_map,
                index_close_map=build_index_close_map(hk_index_close_rows),
                futures_basis_map=build_hk_futures_basis_map(hk_futures_rows),
            )
        )
    if us_trade_dates:
        us_index_close_rows = await db_tools.get_quant_index_dashboard_index_closes_for_market(
            US_INDEX_NAME_ORDER,
            "us",
            start_date,
            end_date,
        )
        us_futures_rows = await db_tools.get_quant_index_dashboard_us_index_futures_closes(
            US_INDEX_FUTURES_SYMBOLS.values(),
            start_date,
            end_date,
        )
        rows.extend(
            build_us_dashboard_rows(
                trade_dates=us_trade_dates,
                index_code_map=index_code_map,
                index_close_map=build_index_close_map(us_index_close_rows),
                futures_close_map=build_us_futures_close_map(us_futures_rows),
            )
        )
    affected = await db_tools.upsert_quant_index_dashboard_daily(rows)
    print(
        "quant index dashboard sync finished: "
        f"start_date={start_date}, end_date={end_date}, "
        f"cn_trade_dates={len(cn_trade_dates)}, hk_trade_dates={len(hk_trade_dates)}, "
        f"us_trade_dates={len(us_trade_dates)}, affected={affected}"
    )
    return affected


def merge_trade_dates_to_ranges(trade_dates):
    normalized_dates = sorted({
        normalize_date_text(trade_date)
        for trade_date in (trade_dates or [])
        if normalize_date_text(trade_date)
    })
    if not normalized_dates:
        return []

    ranges = []
    current_start = normalized_dates[0]
    current_end = normalized_dates[0]
    current_end_date = datetime.strptime(current_end, "%Y-%m-%d").date()

    for trade_date in normalized_dates[1:]:
        parsed_trade_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
        if parsed_trade_date == current_end_date + timedelta(days=1):
            current_end = trade_date
            current_end_date = parsed_trade_date
            continue

        ranges.append((current_start, current_end))
        current_start = trade_date
        current_end = trade_date
        current_end_date = parsed_trade_date

    ranges.append((current_start, current_end))
    return ranges


async def refresh_trade_dates(db_tools, trade_dates):
    merged_ranges = merge_trade_dates_to_ranges(trade_dates)
    if not merged_ranges:
        print("quant index dashboard refresh skipped: no valid trade dates")
        return 0

    total_affected = 0
    for start_date, end_date in merged_ranges:
        total_affected += await compute_and_upsert_range(db_tools, start_date, end_date)
    return total_affected


async def get_recent_trade_dates_for_market(db_tools, market, trade_day_count=10):
    if market == "cn":
        return await db_tools.get_latest_quant_index_trade_dates(INDEX_NAME_ORDER, limit=trade_day_count)
    if market == "hk":
        dates = await db_tools.get_quant_index_dashboard_trade_dates_for_market(HK_INDEX_NAME_ORDER, market="hk")
        return dates[-trade_day_count:]
    if market == "us":
        dates = await db_tools.get_quant_index_dashboard_trade_dates_for_market(US_INDEX_NAME_ORDER, market="us")
        return dates[-trade_day_count:]
    return []


async def get_previous_trade_date_for_market(db_tools, market, reference_date=None):
    if reference_date is None:
        parsed_reference_date = datetime.now().date()
    elif hasattr(reference_date, "date"):
        parsed_reference_date = reference_date.date()
    elif hasattr(reference_date, "strftime"):
        parsed_reference_date = reference_date
    else:
        parsed_reference_date = datetime.strptime(parse_date_arg(reference_date), "%Y-%m-%d").date()

    end_date = (parsed_reference_date - timedelta(days=1)).strftime("%Y-%m-%d")
    if market == "cn":
        dates = await db_tools.get_quant_index_dashboard_trade_dates(
            INDEX_NAME_ORDER,
            end_date=end_date,
        )
    elif market == "hk":
        dates = await db_tools.get_quant_index_dashboard_trade_dates_for_market(
            HK_INDEX_NAME_ORDER,
            market="hk",
            end_date=end_date,
        )
    elif market == "us":
        dates = await db_tools.get_quant_index_dashboard_trade_dates_for_market(
            US_INDEX_NAME_ORDER,
            market="us",
            end_date=end_date,
        )
    else:
        dates = []
    return dates[-1] if dates else None


async def repair_market_recent(market, trade_day_count=10):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        recent_trade_dates = await get_recent_trade_dates_for_market(db_tools, market, trade_day_count)
        if not recent_trade_dates:
            print(f"quant index dashboard repair market recent finished: market={market}, no trade dates found")
            return 0

        affected = await refresh_trade_dates(db_tools, recent_trade_dates)
        print(
            "quant index dashboard repair market recent finished: "
            f"market={market}, trade_day_count={trade_day_count}, "
            f"trade_dates={len(recent_trade_dates)}, "
            f"start_date={min(recent_trade_dates)}, end_date={max(recent_trade_dates)}, affected={affected}"
        )
        return affected
    finally:
        await db_tools.close()


async def repair_market_previous_trade_day(market, reference_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        previous_trade_date = await get_previous_trade_date_for_market(db_tools, market, reference_date=reference_date)
        if not previous_trade_date:
            print(f"quant index dashboard repair previous trade day finished: market={market}, no trade date found")
            return 0

        affected = await compute_and_upsert_range(db_tools, previous_trade_date, previous_trade_date)
        print(
            "quant index dashboard repair previous trade day finished: "
            f"market={market}, trade_date={previous_trade_date}, affected={affected}"
        )
        return affected
    finally:
        await db_tools.close()


async def resolve_market_previous_trade_date(market, reference_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        return await get_previous_trade_date_for_market(db_tools, market, reference_date=reference_date)
    finally:
        await db_tools.close()


async def backfill_history(start_date=None, end_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        if start_date is None or end_date is None:
            trade_dates = sorted(
                set(
                    await db_tools.get_quant_index_dashboard_trade_dates(INDEX_NAME_ORDER)
                    + await db_tools.get_quant_index_dashboard_trade_dates_for_market(HK_INDEX_NAME_ORDER, market="hk")
                    + await db_tools.get_quant_index_dashboard_trade_dates_for_market(US_INDEX_NAME_ORDER, market="us")
                )
            )
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
        if target_date:
            return await compute_and_upsert_range(db_tools, target_date, target_date)

        recent_trade_dates = sorted(
            set(
                await get_recent_trade_dates_for_market(db_tools, "cn", 10)
                + await get_recent_trade_dates_for_market(db_tools, "hk", 10)
                + await get_recent_trade_dates_for_market(db_tools, "us", 10)
            )
        )
        if not recent_trade_dates:
            print("quant index dashboard daily finished: no latest trade date found")
            return 0
        return await refresh_trade_dates(db_tools, recent_trade_dates)
    finally:
        await db_tools.close()


async def repair_recent(trade_day_count=10):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        recent_trade_dates = sorted(
            set(
                await get_recent_trade_dates_for_market(db_tools, "cn", trade_day_count)
                + await get_recent_trade_dates_for_market(db_tools, "hk", trade_day_count)
                + await get_recent_trade_dates_for_market(db_tools, "us", trade_day_count)
            )
        )
        if not recent_trade_dates:
            print("quant index dashboard repair recent finished: no trade dates found")
            return 0

        affected = await refresh_trade_dates(db_tools, recent_trade_dates)
        print(
            "quant index dashboard repair recent finished: "
            f"trade_day_count={trade_day_count}, "
            f"trade_dates={len(recent_trade_dates)}, "
            f"start_date={min(recent_trade_dates)}, end_date={max(recent_trade_dates)}, "
            f"affected={affected}"
        )
        return affected
    finally:
        await db_tools.close()


async def refresh_breadth_data(start_date=None, end_date=None):
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        if start_date is None or end_date is None:
            trade_dates = sorted(
                set(
                    await db_tools.get_quant_index_dashboard_trade_dates(INDEX_NAME_ORDER)
                    + await db_tools.get_quant_index_dashboard_trade_dates_for_market(HK_INDEX_NAME_ORDER, market="hk")
                    + await db_tools.get_quant_index_dashboard_trade_dates_for_market(US_INDEX_NAME_ORDER, market="us")
                )
            )
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
    if command == "repair-recent":
        trade_day_count = parse_trade_day_count_arg(args[0]) if args else 10
        await repair_recent(trade_day_count=trade_day_count)
        return
    if command == "repair-market-recent":
        market = str(args[0]).strip().lower() if args else "cn"
        trade_day_count = parse_trade_day_count_arg(args[1], 10) if len(args) > 1 else 10
        await repair_market_recent(market, trade_day_count=trade_day_count)
        return
    if command == "repair-market-previous":
        market = str(args[0]).strip().lower() if args else "cn"
        reference_date = parse_date_arg(args[1]) if len(args) > 1 else None
        await repair_market_previous_trade_day(market, reference_date=reference_date)
        return

    raise ValueError(
        "quant-index supports: backfill [start_date] [end_date] | "
        "daily [trade_date] | refresh-breadth [start_date] [end_date] | "
        "repair-recent [trade_day_count] | repair-market-recent [cn|hk|us] [trade_day_count] | "
        "repair-market-previous [cn|hk|us] [reference_date]"
    )


if __name__ == "__main__":
    asyncio.run(main())
