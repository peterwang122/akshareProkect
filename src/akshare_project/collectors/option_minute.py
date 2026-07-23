import asyncio
import json
import math
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, time as datetime_time, timedelta

import akshare as ak

from akshare_project.collectors import exchange_option
from akshare_project.collectors.quant_index import (
    MINUTES_PER_YEAR,
    OPTION_VIX_PRODUCT_NAMES,
    OPTION_VIX_ROLL_DAYS,
    OPTION_VIX_SOURCES_BY_INDEX,
    OPTION_VIX_TARGET_DAYS,
    build_risk_free_rate_curve_map,
    interpolate_risk_free_rate,
    resolve_risk_free_curve,
    third_friday_of_contract_month,
)
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools


LOGGER = get_logger("option_minute")
SINA_CFFEX_OPTION_URL = (
    "https://stock.finance.sina.com.cn/futures/api/openapi.php/"
    "OptionService.getOptionData"
)
SINA_EXCHANGE_MINUTE_URL = (
    "https://stock.finance.sina.com.cn/futures/api/openapi.php/"
    "StockOptionDaylineService.getFiveDayLine"
)
SUPPORTED_EXCHANGE_UNDERLYINGS = (
    "510050",
    "510300",
    "510500",
    "588000",
    "588080",
    "159919",
    "159922",
)
CFFEX_PRODUCTS = ("HO", "IO", "MO")
CFFEX_LIST_FETCHERS = {
    "HO": ak.option_cffex_sz50_list_sina,
    "IO": ak.option_cffex_hs300_list_sina,
    "MO": ak.option_cffex_zz1000_list_sina,
}
CFFEX_PRODUCT_SLUGS = {"HO": "ho", "IO": "io", "MO": "mo"}
INDEX_NAME_BY_SOURCE = {
    (exchange, product): index_name
    for index_name, sources in OPTION_VIX_SOURCES_BY_INDEX.items()
    for exchange, product in sources
}
MARKET_SESSIONS = (
    (datetime_time(9, 30), datetime_time(11, 30)),
    (datetime_time(13, 0), datetime_time(15, 0)),
)
MINIMUM_EXPECTED_MINUTES = 220
RECENT_BACKFILL_CONCURRENCY = 8
MAX_LIVE_SNAPSHOT_LAG_SECONDS = 120
DAILY_SESSION_START_TIME = datetime_time(9, 25)
DAILY_SESSION_END_TIME = datetime_time(15, 0, 59)
SNAPSHOT_READY_SECOND = 3
MAX_SNAPSHOT_ATTEMPTS_PER_MINUTE = 3


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def parse_number(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def normalize_bar_time(value):
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        parsed = None
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
        if parsed is None:
            raise ValueError(f"invalid minute timestamp: {value}")
    return parsed.replace(second=0, microsecond=0)


def is_market_minute(value):
    current = normalize_bar_time(value)
    if current.weekday() >= 5:
        return False
    return any(start <= current.time() <= end for start, end in MARKET_SESSIONS)


def is_standard_exchange_contract(row):
    trade_code = str(row.get("contract_trade_code") or "").strip().upper()
    return bool(re.search(r"[CP]\d{4}M\d+$", trade_code))


def select_required_expiries(rows, reference_date, maximum_terms=3):
    reference = (
        reference_date
        if isinstance(reference_date, date)
        else datetime.strptime(str(reference_date).split(" ")[0], "%Y-%m-%d").date()
    )
    eligible = sorted(
        {
            row.get("expire_date")
            if isinstance(row.get("expire_date"), date)
            else datetime.strptime(
                str(row.get("expire_date") or row.get("last_trade_date")).split(" ")[0],
                "%Y-%m-%d",
            ).date()
            for row in rows
            if row.get("expire_date") or row.get("last_trade_date")
        }
    )
    eligible = [
        expiry
        for expiry in eligible
        if (expiry - reference).days > OPTION_VIX_ROLL_DAYS
    ]
    if not eligible:
        return set()
    if (eligible[0] - reference).days >= OPTION_VIX_TARGET_DAYS:
        return {eligible[0]}
    return set(eligible[:maximum_terms])


def choose_exchange_contracts(rows, reference_date):
    grouped = defaultdict(list)
    for row in rows or []:
        source = (
            str(row.get("exchange") or "").strip().upper(),
            str(row.get("underlying_code") or "").strip(),
        )
        if source not in INDEX_NAME_BY_SOURCE or not is_standard_exchange_contract(row):
            continue
        grouped[source].append(row)
    selected = []
    for source_rows in grouped.values():
        expiries = select_required_expiries(source_rows, reference_date)
        for row in source_rows:
            expiry = row.get("expire_date") or row.get("last_trade_date")
            expiry = (
                expiry
                if isinstance(expiry, date)
                else datetime.strptime(str(expiry).split(" ")[0], "%Y-%m-%d").date()
            )
            if expiry in expiries:
                selected.append({**row, "expire_date": expiry})
    return selected


def quote_midpoint(bid_price, ask_price):
    bid = parse_number(bid_price)
    ask = parse_number(ask_price)
    if bid is None or ask is None or bid < 0 or ask <= 0 or ask < bid:
        return None
    return (bid + ask) / 2


def build_exchange_snapshot_rows(contract_rows, quotes, requested_at):
    requested_bar = normalize_bar_time(requested_at)
    result = []
    for metadata in contract_rows or []:
        quote = quotes.get(str(metadata.get("contract_code") or "").strip()) or {}
        quote_time = quote.get("quote_time") or requested_bar.strftime("%Y-%m-%d %H:%M:%S")
        try:
            source_quote_time = normalize_bar_time(quote_time)
        except ValueError:
            continue
        if source_quote_time.date() != requested_bar.date():
            continue
        bar_time = requested_bar
        last_price = parse_number(quote.get("close_price"))
        if last_price is not None and last_price <= 0:
            last_price = None
        midpoint = quote_midpoint(quote.get("bid1_price"), quote.get("ask1_price"))
        expiry = metadata.get("expire_date") or metadata.get("last_trade_date")
        result.append(
            {
                "exchange": str(metadata.get("exchange") or "").strip().upper(),
                "contract_code": str(metadata.get("contract_code") or "").strip(),
                "contract_trade_code": metadata.get("contract_trade_code"),
                "underlying_code": str(metadata.get("underlying_code") or "").strip(),
                "option_type": str(metadata.get("option_type") or "").strip().upper(),
                "contract_month": str(metadata.get("contract_month") or "").strip(),
                "strike_price": metadata.get("strike_price"),
                "expire_date": expiry,
                "trade_date": bar_time.date().isoformat(),
                "bar_time": bar_time.strftime("%Y-%m-%d %H:%M:%S"),
                "open_price": last_price,
                "high_price": last_price,
                "low_price": last_price,
                "close_price": last_price,
                "bid1_price": quote.get("bid1_price"),
                "bid1_volume": quote.get("bid1_volume"),
                "ask1_price": quote.get("ask1_price"),
                "ask1_volume": quote.get("ask1_volume"),
                "mid_price": midpoint,
                "cumulative_volume": quote.get("volume"),
                "cumulative_turnover": quote.get("turnover"),
                "open_interest": quote.get("open_interest"),
                "price_basis": "mid_quote" if midpoint is not None else "last_trade",
                "data_source": "sina_realtime_snapshot",
                "source_url": exchange_option.SINA_QUOTE_URL,
                "raw_json": {
                    **quote,
                    "source_quote_time": source_quote_time.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                },
            }
        )
    return result


def fetch_cffex_contract_months_sync(reference_date=None):
    reference = reference_date or datetime.now().date()
    result = {}
    for product, fetcher in CFFEX_LIST_FETCHERS.items():
        payload = fetcher()
        symbols = [
            str(symbol).strip().lower()
            for values in (payload or {}).values()
            for symbol in values
        ]
        rows = []
        for symbol in symbols:
            match = re.fullmatch(r"(ho|io|mo)(\d{4})", symbol)
            if not match or match.group(1).upper() != product:
                continue
            contract_month = match.group(2)
            expiry = third_friday_of_contract_month(contract_month)
            rows.append(
                {
                    "symbol": symbol,
                    "contract_month": contract_month,
                    "expire_date": expiry,
                }
            )
        expiries = select_required_expiries(rows, reference, maximum_terms=2)
        result[product] = [row for row in rows if row["expire_date"] in expiries]
    return result


def parse_cffex_option_payload(payload, product, contract_month, expire_date, bar_time):
    data = ((payload or {}).get("result") or {}).get("data") or {}
    result = []
    for option_type, side_key in (("CALL", "up"), ("PUT", "down")):
        for item in data.get(side_key) or []:
            if not isinstance(item, (list, tuple)) or len(item) < 8:
                continue
            strike_index = 7 if option_type == "CALL" else None
            strike = parse_number(item[strike_index]) if strike_index is not None else None
            symbol = str(item[8 if option_type == "CALL" else 7] or "").strip()
            if strike is None:
                match = re.search(r"(\d+(?:\.\d+)?)$", symbol)
                strike = parse_number(match.group(1)) if match else None
            if strike is None:
                continue
            last_price = parse_number(item[2])
            if last_price is not None and last_price <= 0:
                last_price = None
            midpoint = quote_midpoint(item[1], item[3])
            normalized_code = (
                f"{product}{contract_month}-"
                f"{'C' if option_type == 'CALL' else 'P'}-{strike:g}"
            )
            result.append(
                {
                    "exchange": "CFFEX",
                    "contract_code": normalized_code,
                    "contract_trade_code": symbol,
                    "underlying_code": product,
                    "option_type": option_type,
                    "contract_month": contract_month,
                    "strike_price": strike,
                    "expire_date": expire_date,
                    "trade_date": bar_time.date().isoformat(),
                    "bar_time": bar_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "open_price": last_price,
                    "high_price": last_price,
                    "low_price": last_price,
                    "close_price": last_price,
                    "bid1_price": parse_number(item[1]),
                    "bid1_volume": parse_number(item[0]),
                    "ask1_price": parse_number(item[3]),
                    "ask1_volume": parse_number(item[4]),
                    "mid_price": midpoint,
                    "open_interest": parse_number(item[5]),
                    "price_basis": "mid_quote" if midpoint is not None else "last_trade",
                    "data_source": "sina_cffex_realtime_snapshot",
                    "source_url": SINA_CFFEX_OPTION_URL,
                    "raw_json": item,
                }
            )
    return result


def fetch_cffex_snapshot_rows_sync(requested_at):
    bar_time = normalize_bar_time(requested_at)
    session = exchange_option.direct_session()
    months_by_product = fetch_cffex_contract_months_sync(bar_time.date())
    result = []
    for product, month_rows in months_by_product.items():
        for month_row in month_rows:
            response = session.get(
                SINA_CFFEX_OPTION_URL,
                params={
                    "type": "futures",
                    "product": CFFEX_PRODUCT_SLUGS[product],
                    "exchange": "cffex",
                    "pinzhong": month_row["symbol"],
                },
                timeout=exchange_option.REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            result.extend(
                parse_cffex_option_payload(
                    response.json(),
                    product,
                    month_row["contract_month"],
                    month_row["expire_date"],
                    bar_time,
                )
            )
    return result


def parse_sina_five_day_payload(payload, metadata, target_date=None):
    result = []
    current_date = None
    for day_rows in (((payload or {}).get("result") or {}).get("data") or []):
        for item in day_rows or []:
            if isinstance(item, dict):
                raw_time = item.get("i")
                raw_price = item.get("p")
                raw_volume = item.get("v")
                raw_average = item.get("a")
                raw_date = item.get("d")
            elif isinstance(item, (list, tuple)) and len(item) >= 6:
                raw_time = item[0]
                raw_price = item[1]
                raw_volume = item[2]
                raw_average = item[4]
                raw_date = item[5]
            else:
                continue
            if raw_date:
                current_date = str(raw_date).strip()
            if not current_date or (target_date and current_date != str(target_date)):
                continue
            try:
                bar_time = normalize_bar_time(f"{current_date} {raw_time}")
            except ValueError:
                continue
            if not is_market_minute(bar_time):
                continue
            price = parse_number(raw_price)
            if price is not None and price <= 0:
                price = None
            expiry = metadata.get("expire_date") or metadata.get("last_trade_date")
            result.append(
                {
                    "exchange": str(metadata.get("exchange") or "").strip().upper(),
                    "contract_code": str(metadata.get("contract_code") or "").strip(),
                    "contract_trade_code": metadata.get("contract_trade_code"),
                    "underlying_code": str(metadata.get("underlying_code") or "").strip(),
                    "option_type": str(metadata.get("option_type") or "").strip().upper(),
                    "contract_month": str(metadata.get("contract_month") or "").strip(),
                    "strike_price": metadata.get("strike_price"),
                    "expire_date": expiry,
                    "trade_date": current_date,
                    "bar_time": bar_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "open_price": price,
                    "high_price": price,
                    "low_price": price,
                    "close_price": price,
                    "average_price": parse_number(raw_average),
                    "minute_volume": parse_number(raw_volume),
                    "price_basis": "last_trade",
                    "data_source": "sina_five_day_minute",
                    "source_url": SINA_EXCHANGE_MINUTE_URL,
                    "raw_json": item,
                }
            )
    return result


def fetch_sina_five_day_rows_sync(metadata, target_date=None):
    session = exchange_option.direct_session()
    response = session.get(
        SINA_EXCHANGE_MINUTE_URL,
        params={"symbol": f"CON_OP_{metadata['contract_code']}"},
        headers=exchange_option.SINA_HEADERS,
        timeout=exchange_option.REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return parse_sina_five_day_payload(response.json(), metadata, target_date)


def _row_price(row):
    midpoint = parse_number(row.get("mid_price"))
    if midpoint is not None and midpoint > 0:
        return midpoint, "mid_quote"
    close_price = parse_number(row.get("close_price"))
    if close_price is not None and close_price > 0:
        return close_price, "last_trade"
    return None, None


def _expiry_datetime(expiry_date):
    expiry = (
        expiry_date
        if isinstance(expiry_date, date)
        else datetime.strptime(str(expiry_date).split(" ")[0], "%Y-%m-%d").date()
    )
    return datetime.combine(expiry, datetime_time(15, 0))


def calculate_minute_term_variance(rows, bar_time, risk_free_rate):
    minute = normalize_bar_time(bar_time)
    if not rows:
        return None
    expiry = _expiry_datetime(rows[0].get("expire_date"))
    minutes_to_expiry = (expiry - minute).total_seconds() / 60
    if minutes_to_expiry <= 0:
        return None
    time_to_expiry = minutes_to_expiry / MINUTES_PER_YEAR
    chain = defaultdict(dict)
    for row in rows:
        option_type = str(row.get("option_type") or "").strip().upper()
        strike = parse_number(row.get("strike_price"))
        price, basis = _row_price(row)
        if option_type not in {"CALL", "PUT"} or strike is None or price is None:
            continue
        chain[strike][option_type] = {
            "price": price,
            "basis": basis,
            "bid": parse_number(row.get("bid1_price")),
        }
    paired = [
        (strike, sides["CALL"]["price"], sides["PUT"]["price"])
        for strike, sides in chain.items()
        if "CALL" in sides and "PUT" in sides
    ]
    if len(paired) < 2:
        return None
    forward_strike, call_price, put_price = min(
        paired,
        key=lambda item: abs(item[1] - item[2]),
    )
    forward = forward_strike + math.exp(risk_free_rate * time_to_expiry) * (
        call_price - put_price
    )
    k0_candidates = [strike for strike in chain if strike <= forward]
    if not k0_candidates:
        return None
    k0 = max(k0_candidates)
    selected = {}
    selected_basis = []
    k0_sides = chain.get(k0) or {}
    if "CALL" not in k0_sides or "PUT" not in k0_sides:
        return None
    selected[k0] = (k0_sides["CALL"]["price"] + k0_sides["PUT"]["price"]) / 2
    selected_basis.extend(
        [k0_sides["CALL"]["basis"], k0_sides["PUT"]["basis"]]
    )

    for option_type, strikes in (
        ("PUT", sorted((strike for strike in chain if strike < k0), reverse=True)),
        ("CALL", sorted(strike for strike in chain if strike > k0)),
    ):
        consecutive_zero_bids = 0
        for strike in strikes:
            quote = (chain.get(strike) or {}).get(option_type)
            if not quote:
                continue
            bid = quote.get("bid")
            if bid is not None and bid <= 0:
                consecutive_zero_bids += 1
                if consecutive_zero_bids >= 2:
                    break
                continue
            consecutive_zero_bids = 0
            selected[strike] = quote["price"]
            selected_basis.append(quote["basis"])

    strikes = sorted(selected)
    if len(strikes) < 3:
        return None
    weighted_sum = 0.0
    for index, strike in enumerate(strikes):
        if index == 0:
            delta_k = strikes[1] - strike
        elif index == len(strikes) - 1:
            delta_k = strike - strikes[index - 1]
        else:
            delta_k = (strikes[index + 1] - strikes[index - 1]) / 2
        if delta_k <= 0 or strike <= 0:
            continue
        weighted_sum += (
            delta_k
            / (strike * strike)
            * math.exp(risk_free_rate * time_to_expiry)
            * selected[strike]
        )
    variance = (
        2 / time_to_expiry * weighted_sum
        - 1 / time_to_expiry * ((forward / k0) - 1) ** 2
    )
    if not math.isfinite(variance) or variance <= 0:
        return None
    return {
        "variance": variance,
        "minutes_to_expiry": minutes_to_expiry,
        "contract_month": str(rows[0].get("contract_month") or ""),
        "expire_date": expiry.date().isoformat(),
        "strike_count": len(strikes),
        "risk_free_rate": risk_free_rate,
        "price_basis": (
            "mid_quote"
            if selected_basis and all(item == "mid_quote" for item in selected_basis)
            else "last_trade"
        ),
    }


def calculate_constant_30d_minute_vix(term_results):
    valid_terms = sorted(
        (
            item
            for item in term_results or []
            if item and item["minutes_to_expiry"] > OPTION_VIX_ROLL_DAYS * 24 * 60
        ),
        key=lambda item: item["minutes_to_expiry"],
    )
    if not valid_terms:
        return None
    near = valid_terms[0]
    target_minutes = OPTION_VIX_TARGET_DAYS * 24 * 60
    next_term = None
    if near["minutes_to_expiry"] >= target_minutes:
        annual_variance = near["variance"]
    else:
        if len(valid_terms) < 2:
            return None
        next_term = valid_terms[1]
        near_minutes = near["minutes_to_expiry"]
        next_minutes = next_term["minutes_to_expiry"]
        denominator = next_minutes - near_minutes
        if denominator <= 0:
            return None
        annual_variance = (
            (
                near["variance"]
                * (near_minutes / MINUTES_PER_YEAR)
                * (next_minutes - target_minutes)
                / denominator
            )
            + (
                next_term["variance"]
                * (next_minutes / MINUTES_PER_YEAR)
                * (target_minutes - near_minutes)
                / denominator
            )
        ) * (MINUTES_PER_YEAR / target_minutes)
    if not math.isfinite(annual_variance) or annual_variance <= 0:
        return None
    return {
        "vix_value": 100 * math.sqrt(annual_variance),
        "near": near,
        "next": next_term,
    }


def build_minute_vix_rows(snapshot_rows, rate_rows):
    if not snapshot_rows:
        return []
    rate_curve_map = build_risk_free_rate_curve_map(rate_rows)
    grouped = defaultdict(lambda: defaultdict(list))
    for row in snapshot_rows:
        bar_time = normalize_bar_time(row.get("bar_time"))
        source = (
            str(row.get("exchange") or "").strip().upper(),
            str(row.get("underlying_code") or "").strip().upper(),
        )
        if source not in INDEX_NAME_BY_SOURCE:
            continue
        expiry = str(row.get("expire_date") or "").split(" ")[0]
        grouped[(bar_time, *source)][expiry].append(row)

    result = []
    for (bar_time, exchange, product), terms in grouped.items():
        curve_date, curve = resolve_risk_free_curve(
            rate_curve_map,
            bar_time.date().isoformat(),
        )
        if not curve:
            continue
        term_results = []
        for expiry, rows in terms.items():
            days_to_expiry = (
                datetime.strptime(expiry, "%Y-%m-%d").date() - bar_time.date()
            ).days
            risk_free_rate = interpolate_risk_free_rate(curve, days_to_expiry)
            if risk_free_rate is None:
                continue
            term_result = calculate_minute_term_variance(
                rows,
                bar_time,
                risk_free_rate,
            )
            if term_result:
                term_results.append(term_result)
        vix_result = calculate_constant_30d_minute_vix(term_results)
        if not vix_result:
            continue
        near = vix_result["near"]
        next_term = vix_result["next"]
        basis = (
            "mid_quote"
            if near.get("price_basis") == "mid_quote"
            and (not next_term or next_term.get("price_basis") == "mid_quote")
            else "last_trade"
        )
        result.append(
            {
                "index_name": INDEX_NAME_BY_SOURCE[(exchange, product)],
                "source_key": f"{exchange.lower()}:{product}",
                "exchange": exchange,
                "product_code": product,
                "trade_date": bar_time.date().isoformat(),
                "bar_time": bar_time.strftime("%Y-%m-%d %H:%M:%S"),
                "vix_value": vix_result["vix_value"],
                "near_contract_month": near.get("contract_month"),
                "near_expire_date": near.get("expire_date"),
                "near_strike_count": near.get("strike_count"),
                "next_contract_month": next_term.get("contract_month") if next_term else None,
                "next_expire_date": next_term.get("expire_date") if next_term else None,
                "next_strike_count": next_term.get("strike_count") if next_term else None,
                "risk_free_curve_date": curve_date,
                "near_risk_free_rate": near.get("risk_free_rate"),
                "next_risk_free_rate": next_term.get("risk_free_rate") if next_term else None,
                "price_basis": basis,
                "calculation_method": (
                    "ivix_30d_minute_mid_quote"
                    if basis == "mid_quote"
                    else "ivix_30d_minute_last_trade_approximation"
                ),
                "quality_json": {
                    "raw_contract_rows": sum(len(rows) for rows in terms.values()),
                    "term_count": len(term_results),
                    "product_name": OPTION_VIX_PRODUCT_NAMES.get(
                        (exchange, product),
                        product,
                    ),
                },
            }
        )
    return result


async def _rate_rows_for_date(db, trade_date):
    target = (
        trade_date
        if isinstance(trade_date, date)
        else datetime.strptime(str(trade_date), "%Y-%m-%d").date()
    )
    return await db.get_cn_risk_free_rate_rows(
        (target - timedelta(days=14)).isoformat(),
        target.isoformat(),
    )


async def collect_snapshot(requested_at=None, force=False, db_tools=None):
    bar_time = normalize_bar_time(requested_at or datetime.now())
    current_time = datetime.now()
    if abs((current_time - bar_time).total_seconds()) > MAX_LIVE_SNAPSHOT_LAG_SECONDS:
        raise ValueError(
            "live option snapshot cannot be backdated; "
            "use backfill-recent for free SSE/SZSE history"
        )
    if not force and not is_market_minute(bar_time):
        return {
            "status": "OUTSIDE_MARKET_SESSION",
            "bar_time": bar_time.strftime("%Y-%m-%d %H:%M:%S"),
            "minute_rows": 0,
            "vix_rows": 0,
        }
    owns_db = db_tools is None
    db = db_tools or DbTools()
    if owns_db:
        await db.init_pool()
    try:
        contract_rows = await db.list_option_minute_exchange_contract_rows(
            bar_time.date().isoformat(),
            bar_time.date().isoformat(),
            SUPPORTED_EXCHANGE_UNDERLYINGS,
        )
        contract_rows = choose_exchange_contracts(contract_rows, bar_time.date())
        exchange_codes = [row["contract_code"] for row in contract_rows]
        exchange_quotes, cffex_rows = await asyncio.gather(
            asyncio.to_thread(exchange_option.fetch_sina_quotes_sync, exchange_codes),
            asyncio.to_thread(fetch_cffex_snapshot_rows_sync, bar_time),
        )
        exchange_rows = build_exchange_snapshot_rows(
            contract_rows,
            exchange_quotes,
            bar_time,
        )
        minute_rows = [*exchange_rows, *cffex_rows]
        inserted = await db.batch_option_contract_minute_data(minute_rows)
        rate_rows = await _rate_rows_for_date(db, bar_time.date())
        vix_rows = build_minute_vix_rows(minute_rows, rate_rows)
        inserted_vix = await db.batch_option_vix_minute_data(vix_rows)
        source_counts = defaultdict(int)
        for row in minute_rows:
            source_counts[
                f"{str(row['exchange']).lower()}:{row['underlying_code']}"
            ] += 1
        return {
            "status": "SUCCESS",
            "bar_time": bar_time.strftime("%Y-%m-%d %H:%M:%S"),
            "minute_rows": inserted,
            "vix_rows": inserted_vix,
            "source_counts": dict(sorted(source_counts.items())),
            "vix_sources": sorted(row["source_key"] for row in vix_rows),
        }
    finally:
        if owns_db:
            await db.close()


async def backfill_recent(target_date=None, db_tools=None):
    target = (
        datetime.strptime(str(target_date), "%Y-%m-%d").date()
        if target_date
        else datetime.now().date()
    )
    owns_db = db_tools is None
    db = db_tools or DbTools()
    if owns_db:
        await db.init_pool()
    try:
        contract_rows = await db.list_option_minute_exchange_contract_rows(
            (target - timedelta(days=10)).isoformat(),
            target.isoformat(),
            SUPPORTED_EXCHANGE_UNDERLYINGS,
        )
        if target_date:
            contract_rows = choose_exchange_contracts(contract_rows, target)
        else:
            selected_by_code = {}
            for offset_days in range(0, 8):
                reference_date = target - timedelta(days=offset_days)
                for row in choose_exchange_contracts(contract_rows, reference_date):
                    selected_by_code[
                        (
                            str(row.get("exchange") or "").strip().upper(),
                            str(row.get("contract_code") or "").strip(),
                        )
                    ] = row
            contract_rows = list(selected_by_code.values())
        total_rows = 0
        failed = []
        semaphore = asyncio.Semaphore(RECENT_BACKFILL_CONCURRENCY)

        async def fetch(metadata):
            async with semaphore:
                try:
                    rows = await asyncio.to_thread(
                        fetch_sina_five_day_rows_sync,
                        metadata,
                        str(target_date) if target_date else None,
                    )
                    return metadata, rows, None
                except Exception as exc:
                    return metadata, [], str(exc)

        for offset in range(0, len(contract_rows), 40):
            results = await asyncio.gather(
                *(fetch(row) for row in contract_rows[offset : offset + 40])
            )
            batch = []
            for metadata, rows, error in results:
                if error:
                    failed.append(
                        f"{metadata.get('exchange')}:{metadata.get('contract_code')} {error}"
                    )
                batch.extend(rows)
            total_rows += await db.batch_option_contract_minute_data(batch)
        if target_date:
            vix_rows = await rebuild_vix(
                str(target_date),
                str(target_date),
                db_tools=db,
            )
        else:
            trade_dates = sorted(
                {
                    row.get("trade_date")
                    for row in await db.get_option_contract_minute_rows(
                        f"{(target - timedelta(days=10)).isoformat()} 00:00:00",
                        f"{target.isoformat()} 23:59:59",
                    )
                    if row.get("trade_date")
                }
            )
            vix_rows = 0
            for trade_date in trade_dates:
                vix_rows += await rebuild_vix(
                    str(trade_date),
                    str(trade_date),
                    db_tools=db,
                )
        return {
            "status": "SUCCESS" if not failed else "PARTIAL",
            "target_date": str(target_date) if target_date else None,
            "contracts": len(contract_rows),
            "minute_rows": total_rows,
            "vix_rows": vix_rows,
            "failed_contracts": len(failed),
            "failure_samples": failed[:10],
        }
    finally:
        if owns_db:
            await db.close()


async def rebuild_vix(start_date, end_date=None, db_tools=None):
    start = datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end = datetime.strptime(str(end_date or start_date), "%Y-%m-%d").date()
    if start > end:
        raise ValueError("start_date cannot be greater than end_date")
    owns_db = db_tools is None
    db = db_tools or DbTools()
    if owns_db:
        await db.init_pool()
    total = 0
    try:
        current = start
        while current <= end:
            rows = await db.get_option_contract_minute_rows(
                f"{current.isoformat()} 09:30:00",
                f"{current.isoformat()} 15:00:00",
            )
            rate_rows = await _rate_rows_for_date(db, current)
            rows_by_bar = defaultdict(list)
            for row in rows:
                rows_by_bar[normalize_bar_time(row.get("bar_time"))].append(row)
            for bar_rows in rows_by_bar.values():
                total += await db.batch_option_vix_minute_data(
                    build_minute_vix_rows(bar_rows, rate_rows)
                )
            current += timedelta(days=1)
        return total
    finally:
        if owns_db:
            await db.close()


async def finalize_daily(target_date=None):
    target = str(target_date or datetime.now().date())
    db = DbTools()
    await db.init_pool()
    try:
        backfill_result = await backfill_recent(target, db_tools=db)
        vix_rows = await rebuild_vix(target, target, db_tools=db)
        summary = await db.summarize_option_minute_trade_date(target)
        cffex_sources = {
            str(row.get("underlying_code") or "").strip().upper(): int(
                row.get("minute_count") or 0
            )
            for row in summary
            if str(row.get("exchange") or "").strip().upper() == "CFFEX"
        }
        missing_cffex = [
            product
            for product in CFFEX_PRODUCTS
            if cffex_sources.get(product, 0) < MINIMUM_EXPECTED_MINUTES
        ]
        if missing_cffex:
            raise RuntimeError(
                f"{target}中金所期权分钟采集不完整："
                f"{','.join(missing_cffex)}少于{MINIMUM_EXPECTED_MINUTES}分钟；"
                "中金所公开源无法盘后回补，请检查盘中分钟服务"
            )
        return {
            "status": "SUCCESS",
            "target_date": target,
            "recent_backfill": backfill_result,
            "vix_rows": vix_rows,
            "coverage": summary,
        }
    finally:
        await db.close()


async def run_daily_session(
    target_date=None,
    *,
    now_provider=None,
    sleep_handler=None,
    snapshot_handler=None,
    finalize_handler=None,
):
    """Collect every live market minute, then finalize and validate the day."""
    now_provider = now_provider or datetime.now
    sleep_handler = sleep_handler or asyncio.sleep
    snapshot_handler = snapshot_handler or collect_snapshot
    finalize_handler = finalize_handler or finalize_daily

    current = now_provider()
    target = (
        datetime.strptime(str(target_date), "%Y-%m-%d").date()
        if target_date
        else current.date()
    )
    if target > current.date():
        raise ValueError("option minute session target date cannot be in the future")

    session_start = datetime.combine(target, DAILY_SESSION_START_TIME)
    session_end = datetime.combine(target, DAILY_SESSION_END_TIME)
    if target < current.date() or current > session_end:
        result = await finalize_handler(target.isoformat())
        return {
            **result,
            "session_mode": "post_close_finalize",
            "snapshot_bars": 0,
            "snapshot_failures": 0,
        }
    if target.weekday() >= 5:
        raise RuntimeError(f"{target.isoformat()} is not an A-share trading day")

    completed_bars = set()
    attempts_by_bar = defaultdict(int)
    failure_samples = []
    total_minute_rows = 0
    total_vix_rows = 0
    source_counts = defaultdict(int)
    started_at = current

    while True:
        current = now_provider()
        if current.date() > target or current > session_end:
            break
        if current < session_start:
            await sleep_handler(min(20, max(1, (session_start - current).total_seconds())))
            continue

        bar_time = normalize_bar_time(current)
        if not is_market_minute(bar_time):
            await sleep_handler(20)
            continue
        if current.second < SNAPSHOT_READY_SECOND:
            await sleep_handler(SNAPSHOT_READY_SECOND - current.second)
            continue
        if bar_time in completed_bars:
            await sleep_handler(1)
            continue

        attempts_by_bar[bar_time] += 1
        try:
            snapshot = await snapshot_handler(bar_time)
        except Exception as exc:
            attempt = attempts_by_bar[bar_time]
            LOGGER.exception(
                "option minute session snapshot failed at %s attempt %s/%s: %s",
                bar_time,
                attempt,
                MAX_SNAPSHOT_ATTEMPTS_PER_MINUTE,
                exc,
            )
            if len(failure_samples) < 20:
                failure_samples.append(
                    {
                        "bar_time": bar_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "attempt": attempt,
                        "error": str(exc),
                    }
                )
            if attempt >= MAX_SNAPSHOT_ATTEMPTS_PER_MINUTE:
                completed_bars.add(bar_time)
            await sleep_handler(1)
            continue

        completed_bars.add(bar_time)
        total_minute_rows += int(snapshot.get("minute_rows") or 0)
        total_vix_rows += int(snapshot.get("vix_rows") or 0)
        for source_key, count in (snapshot.get("source_counts") or {}).items():
            source_counts[str(source_key)] += int(count or 0)
        print(
            "option minute session snapshot: "
            f"bar={bar_time.strftime('%H:%M')}, "
            f"minute_rows={snapshot.get('minute_rows', 0)}, "
            f"vix_rows={snapshot.get('vix_rows', 0)}"
        )
        await sleep_handler(1)

    final_result = await finalize_handler(target.isoformat())
    return {
        **final_result,
        "session_mode": "live_trading_session",
        "session_started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "session_finished_at": now_provider().strftime("%Y-%m-%d %H:%M:%S"),
        "snapshot_bars": len(completed_bars),
        "snapshot_failures": len(failure_samples),
        "failure_samples": failure_samples,
        "live_minute_rows": total_minute_rows,
        "live_vix_rows": total_vix_rows,
        "live_source_counts": dict(sorted(source_counts.items())),
    }


async def service_loop():
    print("option minute service started")
    last_bar = None
    while True:
        now = datetime.now()
        bar_time = normalize_bar_time(now)
        if is_market_minute(bar_time) and bar_time != last_bar and now.second >= 3:
            try:
                result = await collect_snapshot(bar_time)
                print(f"option minute snapshot: {result}")
                last_bar = bar_time
            except Exception as exc:
                LOGGER.exception("option minute snapshot failed: %s", exc)
        await asyncio.sleep(1 if is_market_minute(bar_time) else 20)


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "snapshot"
    args = sys.argv[2:]
    if command == "snapshot":
        requested_at = args[0] if args else None
        print(await collect_snapshot(requested_at=requested_at))
        return
    if command == "backfill-recent":
        target_date = args[0] if args else None
        print(await backfill_recent(target_date=target_date))
        return
    if command == "rebuild-vix":
        if not args:
            raise ValueError("rebuild-vix requires start_date [end_date]")
        print(await rebuild_vix(args[0], args[1] if len(args) > 1 else None))
        return
    if command == "finalize":
        print(await finalize_daily(args[0] if args else None))
        return
    if command == "service":
        await service_loop()
        return
    raise ValueError(
        "option-minute supports: snapshot [datetime] | backfill-recent [date] | "
        "rebuild-vix <start_date> [end_date] | finalize [date] | service"
    )


if __name__ == "__main__":
    asyncio.run(main())
