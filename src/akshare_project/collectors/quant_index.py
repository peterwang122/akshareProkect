import asyncio
import math
import re
import sys
from datetime import datetime, timedelta

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools

LOGGER = get_logger("quant_index")

INDEX_NAME_ORDER = [
    "上证指数",
    "上证50",
    "科创50",
    "沪深300",
    "中证500",
    "中证1000",
]
CORE_INDEX_NAMES = [
    "上证50",
    "沪深300",
    "中证500",
    "中证1000",
]
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
    "科创50": "sh000688",
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
CFFEX_NET_SHORT_PRODUCTS_BY_INDEX_NAME = {
    "上证指数": ["IH", "IF", "IC", "IM"],
    "上证50": ["IH"],
    "沪深300": ["IF"],
    "中证500": ["IC"],
    "中证1000": ["IM"],
}
CFFEX_NET_SHORT_DELTA_WINDOWS = (5, 7, 14, 20, 30, 60, 120)
CFFEX_NET_SHORT_DELTA_LOOKBACK_DAYS = 300
CFFEX_NET_SHORT_DELTA_SOURCES = (
    ("top20", "top20_institutions"),
    ("citic", "citic_customer"),
)
CFFEX_NET_SHORT_DELTA_FIELDS = tuple(
    (f"cffex_{field_prefix}_net_short_delta_{window}d", source_key, window)
    for field_prefix, source_key in CFFEX_NET_SHORT_DELTA_SOURCES
    for window in CFFEX_NET_SHORT_DELTA_WINDOWS
)
BASIS_DELTA_WINDOWS = CFFEX_NET_SHORT_DELTA_WINDOWS
BASIS_DELTA_FIELDS = tuple(
    (f"basis_{basis_kind}_delta_{window}d", basis_kind, window)
    for basis_kind in ("main", "month")
    for window in BASIS_DELTA_WINDOWS
)
INDEX_OPTION_PRODUCTS = {
    "上证50": "HO",
    "沪深300": "IO",
    "中证1000": "MO",
}
INDEX_NAME_BY_OPTION_PRODUCT = {
    product_prefix: index_name
    for index_name, product_prefix in INDEX_OPTION_PRODUCTS.items()
}
EXCHANGE_OPTION_PRODUCTS_BY_INDEX = {
    "上证50": [("SSE", "510050")],
    "沪深300": [("SSE", "510300"), ("SZSE", "159919")],
    "中证500": [("SSE", "510500"), ("SZSE", "159922")],
    "科创50": [("SSE", "588000"), ("SSE", "588080")],
}
EXCHANGE_OPTION_PRODUCT_META = {
    ("SSE", "510050"): {"product_name": "上证50ETF期权"},
    ("SSE", "510300"): {"product_name": "沪深300ETF期权"},
    ("SZSE", "159919"): {"product_name": "沪深300ETF期权"},
    ("SSE", "510500"): {"product_name": "中证500ETF期权"},
    ("SZSE", "159922"): {"product_name": "中证500ETF期权"},
    ("SSE", "588000"): {"product_name": "科创50ETF期权"},
    ("SSE", "588080"): {"product_name": "科创板50ETF期权"},
}
EXCHANGE_LABELS = {
    "SSE": "上交所",
    "SZSE": "深交所",
    "CFFEX": "中金所",
}
OPTION_VIX_SOURCES_BY_INDEX = {
    "上证50": [("CFFEX", "HO"), ("SSE", "510050")],
    "沪深300": [("CFFEX", "IO"), ("SSE", "510300"), ("SZSE", "159919")],
    "中证500": [("SSE", "510500"), ("SZSE", "159922")],
    "中证1000": [("CFFEX", "MO")],
    "科创50": [("SSE", "588000"), ("SSE", "588080")],
}
OPTION_VIX_PRODUCT_NAMES = {
    ("CFFEX", "HO"): "上证50股指期权",
    ("CFFEX", "IO"): "沪深300股指期权",
    ("CFFEX", "MO"): "中证1000股指期权",
    **{
        source: metadata["product_name"]
        for source, metadata in EXCHANGE_OPTION_PRODUCT_META.items()
    },
}
OPTION_VIX_ROLL_DAYS = 7
OPTION_VIX_TARGET_DAYS = 30
OPTION_VIX_MINUTE_MINIMUM_COUNT = 220
MINUTES_PER_YEAR = 365 * 24 * 60
OPTION_PC_INDEX_CLOSE_OVERRIDES = {
    ("2024-09-30", "MO"): 5700.0,
    ("2025-04-07", "MO"): 5500.0,
}
OPTION_PC_BUCKETS = [
    {
        "key": "current_month",
        "ratio_field": "option_pc_current_month",
        "month_field": "option_pc_current_month_contract_month",
        "special_flag_field": "option_pc_current_month_special_flag",
        "special_note_field": "option_pc_current_month_special_note",
    },
    {
        "key": "next_month",
        "ratio_field": "option_pc_next_month",
        "month_field": "option_pc_next_month_contract_month",
        "special_flag_field": "option_pc_next_month_special_flag",
        "special_note_field": "option_pc_next_month_special_note",
    },
    {
        "key": "quarter_1",
        "ratio_field": "option_pc_quarter_1",
        "month_field": "option_pc_quarter_1_contract_month",
        "special_flag_field": "option_pc_quarter_1_special_flag",
        "special_note_field": "option_pc_quarter_1_special_note",
    },
    {
        "key": "quarter_2",
        "ratio_field": "option_pc_quarter_2",
        "month_field": "option_pc_quarter_2_contract_month",
        "special_flag_field": "option_pc_quarter_2_special_flag",
        "special_note_field": "option_pc_quarter_2_special_note",
    },
]
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


def shift_date_text(value, days):
    parsed_date = parse_normalized_date(value)
    if parsed_date is None:
        return value
    return (parsed_date + timedelta(days=days)).strftime("%Y-%m-%d")


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


def average_or_none(values):
    valid_values = [float(value) for value in values if value is not None]
    if not valid_values:
        return None
    return sum(valid_values) / len(valid_values)


def empty_option_pc_payload():
    payload = {}
    for bucket in OPTION_PC_BUCKETS:
        payload[bucket["ratio_field"]] = None
        payload[bucket["month_field"]] = None
        payload[bucket["special_flag_field"]] = 0
        payload[bucket["special_note_field"]] = None
    return payload


def empty_option_flow_pc_payload():
    return {
        "option_volume_pc_ratio": None,
        "option_turnover_pc_ratio": None,
    }


def empty_exchange_option_pc_payload(exchange, underlying_code):
    payload = {
        "source_key": f"{str(exchange).strip().lower()}:{str(underlying_code).strip()}",
        "exchange": str(exchange).strip().upper(),
        "exchange_label": EXCHANGE_LABELS.get(str(exchange).strip().upper(), str(exchange).strip().upper()),
        "product_code": str(underlying_code).strip(),
        "product_name": (
            EXCHANGE_OPTION_PRODUCT_META
            .get((str(exchange).strip().upper(), str(underlying_code).strip()), {})
            .get("product_name")
        ),
    }
    payload.update(empty_option_pc_payload())
    payload.update(empty_option_flow_pc_payload())
    return payload


def empty_cffex_net_short_delta_payload():
    return {field_name: None for field_name, _source_key, _window in CFFEX_NET_SHORT_DELTA_FIELDS}


def empty_basis_delta_payload():
    return {field_name: None for field_name, _basis_kind, _window in BASIS_DELTA_FIELDS}


def normalize_contract_month(value):
    text = str(value or "").strip()
    return text if len(text) == 4 and text.isdigit() else ""


def contract_month_sort_key(value):
    text = normalize_contract_month(value)
    if not text:
        return (9999, 99)
    return (2000 + int(text[:2]), int(text[2:]))


def parse_normalized_date(value):
    text = normalize_date_text(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def third_friday_of_contract_month(contract_month):
    text = normalize_contract_month(contract_month)
    if not text:
        return None

    year, month = contract_month_sort_key(text)
    first_day = datetime(year, month, 1).date()
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    return first_friday + timedelta(days=14)


def is_contract_month_expired_for_trade_date(contract_month, trade_date):
    trade_day = parse_normalized_date(trade_date)
    expiry_day = third_friday_of_contract_month(contract_month)
    return bool(trade_day and expiry_day and trade_day >= expiry_day)


def is_quarter_contract_month(value):
    text = normalize_contract_month(value)
    return bool(text) and int(text[2:]) in {3, 6, 9, 12}


def select_option_pc_contract_months(contract_months, trade_date=None):
    sorted_months = sorted(
        {
            normalize_contract_month(contract_month)
            for contract_month in (contract_months or [])
            if normalize_contract_month(contract_month)
        },
        key=contract_month_sort_key,
    )
    if trade_date is not None:
        sorted_months = [
            contract_month
            for contract_month in sorted_months
            if not is_contract_month_expired_for_trade_date(contract_month, trade_date)
        ]
    selected = {
        "current_month": sorted_months[0] if len(sorted_months) >= 1 else None,
        "next_month": sorted_months[1] if len(sorted_months) >= 2 else None,
        "quarter_1": None,
        "quarter_2": None,
    }
    regular_months = {selected["current_month"], selected["next_month"]}
    quarter_months = [
        contract_month
        for contract_month in sorted_months
        if contract_month not in regular_months and is_quarter_contract_month(contract_month)
    ]
    selected["quarter_1"] = quarter_months[0] if len(quarter_months) >= 1 else None
    selected["quarter_2"] = quarter_months[1] if len(quarter_months) >= 2 else None
    return selected


def interpolate_option_price(points, target_price):
    target = to_float(target_price)
    if target is None:
        return None

    normalized_points = []
    for strike_price, option_price in (points or {}).items():
        strike = to_float(strike_price)
        price = to_float(option_price)
        if strike is None or price is None or price < 0:
            continue
        normalized_points.append((strike, price))
    if not normalized_points:
        return None

    normalized_points.sort(key=lambda item: item[0])
    for strike, price in normalized_points:
        if abs(strike - target) < 1e-9:
            return price

    lower = None
    upper = None
    for strike, price in normalized_points:
        if strike < target:
            lower = (strike, price)
        elif strike > target:
            upper = (strike, price)
            break
    if lower is None or upper is None:
        return None

    lower_strike, lower_price = lower
    upper_strike, upper_price = upper
    if upper_strike == lower_strike:
        return None
    return lower_price + (upper_price - lower_price) / (upper_strike - lower_strike) * (target - lower_strike)


def resolve_option_pc_index_close(trade_date, product_prefix, index_close):
    normalized_trade_date = normalize_date_text(trade_date)
    normalized_product_prefix = str(product_prefix or "").strip().upper()
    override_value = OPTION_PC_INDEX_CLOSE_OVERRIDES.get((normalized_trade_date, normalized_product_prefix))
    if override_value is not None:
        return override_value, True
    return index_close, False


def build_option_pc_special_note(index_name, product_prefix, contract_month, effective_index_close):
    normalized_index_name = str(index_name or INDEX_NAME_BY_OPTION_PRODUCT.get(product_prefix, "")).strip()
    normalized_product_prefix = str(product_prefix or "").strip().upper()
    normalized_contract_month = normalize_contract_month(contract_month)
    point_value = to_float(effective_index_close)
    point_text = f"{point_value:g}" if point_value is not None else str(effective_index_close or "").strip()
    return f"{normalized_index_name} {normalized_product_prefix}{normalized_contract_month} 使用特殊点位 {point_text} 计算"


def build_option_pc_payload_for_product(trade_date, product_prefix, index_close, product_rows):
    normalized_product_prefix = str(product_prefix or "").strip().upper()
    index_name = INDEX_NAME_BY_OPTION_PRODUCT.get(normalized_product_prefix, "")
    grouped_by_month_type = {}
    for row in product_rows or []:
        contract_month = normalize_contract_month(row.get("contract_month"))
        option_type = str(row.get("option_type") or "").strip().upper()
        strike_price = to_float(row.get("strike_price"))
        close_price = to_float(row.get("close_price"))
        if not contract_month or option_type not in {"CALL", "PUT"} or strike_price is None or close_price is None:
            continue
        grouped_by_month_type.setdefault((contract_month, option_type), {})[strike_price] = close_price

    selected_months = select_option_pc_contract_months(
        (
            contract_month
            for contract_month, option_type in grouped_by_month_type
            if option_type in {"CALL", "PUT"}
        ),
        trade_date=trade_date,
    )
    payload = empty_option_pc_payload()
    effective_index_close, used_special_close = resolve_option_pc_index_close(trade_date, product_prefix, index_close)
    for bucket in OPTION_PC_BUCKETS:
        contract_month = selected_months.get(bucket["key"])
        if not contract_month:
            continue

        put_price = interpolate_option_price(grouped_by_month_type.get((contract_month, "PUT"), {}), effective_index_close)
        call_price = interpolate_option_price(grouped_by_month_type.get((contract_month, "CALL"), {}), effective_index_close)
        payload[bucket["month_field"]] = contract_month
        if put_price is None or call_price is None or call_price <= 0:
            continue
        payload[bucket["ratio_field"]] = put_price / call_price
        if used_special_close:
            payload[bucket["special_flag_field"]] = 1
            payload[bucket["special_note_field"]] = build_option_pc_special_note(
                index_name,
                normalized_product_prefix,
                contract_month,
                effective_index_close,
            )
    return payload


def build_index_option_pc_map(option_rows, index_close_map):
    rows_by_trade_product = {}
    for row in option_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        product_prefix = str(row.get("product_prefix") or "").strip().upper()
        if not trade_date or product_prefix not in INDEX_NAME_BY_OPTION_PRODUCT:
            continue
        rows_by_trade_product.setdefault((trade_date, product_prefix), []).append(row)

    result = {}
    all_trade_dates = sorted({trade_date for trade_date, _product_prefix in rows_by_trade_product})
    for trade_date in all_trade_dates:
        core_payloads = {}
        for index_name, product_prefix in INDEX_OPTION_PRODUCTS.items():
            index_close = index_close_map.get((trade_date, index_name))
            if index_close is None:
                continue
            product_rows = rows_by_trade_product.get((trade_date, product_prefix), [])
            payload = build_option_pc_payload_for_product(trade_date, product_prefix, index_close, product_rows)
            result[(trade_date, index_name)] = payload
            core_payloads[index_name] = payload

        shanghai_payload = empty_option_pc_payload()
        for bucket in OPTION_PC_BUCKETS:
            values = [
                payload.get(bucket["ratio_field"])
                for payload in core_payloads.values()
                if payload.get(bucket["ratio_field"]) is not None
            ]
            contract_months = [
                str(payload.get(bucket["month_field"]) or "").strip()
                for payload in core_payloads.values()
                if payload.get(bucket["ratio_field"]) is not None and str(payload.get(bucket["month_field"]) or "").strip()
            ]
            shanghai_payload[bucket["ratio_field"]] = average_or_none(values)
            unique_contract_months = sorted(set(contract_months), key=contract_month_sort_key)
            shanghai_payload[bucket["month_field"]] = "/".join(unique_contract_months) if unique_contract_months else None
            special_notes = [
                str(payload.get(bucket["special_note_field"]) or "").strip()
                for payload in core_payloads.values()
                if payload.get(bucket["ratio_field"]) is not None
                and payload.get(bucket["special_flag_field"])
                and str(payload.get(bucket["special_note_field"]) or "").strip()
            ]
            unique_special_notes = sorted(set(special_notes))
            shanghai_payload[bucket["special_flag_field"]] = 1 if unique_special_notes else 0
            shanghai_payload[bucket["special_note_field"]] = "；".join(unique_special_notes) if unique_special_notes else None
        result[(trade_date, "上证指数")] = shanghai_payload

    return result


def build_option_flow_pc_payload_for_product(trade_date, product_prefix, product_rows):
    totals = {
        "CALL": {"volume": 0.0, "turnover": 0.0, "volume_seen": False, "turnover_seen": False},
        "PUT": {"volume": 0.0, "turnover": 0.0, "volume_seen": False, "turnover_seen": False},
    }
    for row in product_rows or []:
        contract_month = normalize_contract_month(row.get("contract_month"))
        option_type = str(row.get("option_type") or "").strip().upper()
        if not contract_month or option_type not in totals:
            continue
        if is_contract_month_expired_for_trade_date(contract_month, trade_date):
            continue

        volume = to_float(row.get("volume"))
        turnover = to_float(row.get("turnover"))
        if volume is not None and volume >= 0:
            totals[option_type]["volume"] += volume
            totals[option_type]["volume_seen"] = True
        if turnover is not None and turnover >= 0:
            totals[option_type]["turnover"] += turnover
            totals[option_type]["turnover_seen"] = True

    payload = empty_option_flow_pc_payload()
    if totals["CALL"]["volume_seen"] and totals["CALL"]["volume"] > 0:
        payload["option_volume_pc_ratio"] = totals["PUT"]["volume"] / totals["CALL"]["volume"]
    if totals["CALL"]["turnover_seen"] and totals["CALL"]["turnover"] > 0:
        payload["option_turnover_pc_ratio"] = totals["PUT"]["turnover"] / totals["CALL"]["turnover"]
    return payload


def build_index_option_flow_pc_map(option_rows):
    rows_by_trade_product = {}
    for row in option_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        product_prefix = str(row.get("product_prefix") or "").strip().upper()
        if not trade_date or product_prefix not in INDEX_NAME_BY_OPTION_PRODUCT:
            continue
        rows_by_trade_product.setdefault((trade_date, product_prefix), []).append(row)

    result = {}
    all_trade_dates = sorted({trade_date for trade_date, _product_prefix in rows_by_trade_product})
    for trade_date in all_trade_dates:
        core_payloads = {}
        for index_name, product_prefix in INDEX_OPTION_PRODUCTS.items():
            product_rows = rows_by_trade_product.get((trade_date, product_prefix), [])
            payload = build_option_flow_pc_payload_for_product(trade_date, product_prefix, product_rows)
            result[(trade_date, index_name)] = payload
            core_payloads[index_name] = payload

        shanghai_payload = empty_option_flow_pc_payload()
        for field in shanghai_payload:
            values = [
                payload.get(field)
                for payload in core_payloads.values()
                if payload.get(field) is not None
            ]
            shanghai_payload[field] = average_or_none(values)
        result[(trade_date, "上证指数")] = shanghai_payload

    return result


def is_standard_exchange_option_contract(row):
    trade_code = str(row.get("contract_trade_code") or "").strip().upper()
    return bool(re.search(r"[CP]\d{4}M\d+", trade_code))


def is_exchange_option_contract_expired(row, trade_date):
    trade_day = parse_normalized_date(trade_date)
    last_trade_day = parse_normalized_date(row.get("last_trade_date"))
    if trade_day and last_trade_day:
        return trade_day >= last_trade_day
    return is_contract_month_expired_for_trade_date(row.get("contract_month"), trade_date)


def build_exchange_option_price_payload(trade_date, etf_close, product_rows):
    grouped_by_month_type = {}
    for row in product_rows or []:
        if is_exchange_option_contract_expired(row, trade_date):
            continue
        if not is_standard_exchange_option_contract(row):
            continue
        contract_month = normalize_contract_month(row.get("contract_month"))
        option_type = str(row.get("option_type") or "").strip().upper()
        strike_price = to_float(row.get("strike_price"))
        close_price = to_float(row.get("close_price"))
        if (
            not contract_month
            or option_type not in {"CALL", "PUT"}
            or strike_price is None
            or close_price is None
        ):
            continue
        grouped_by_month_type.setdefault((contract_month, option_type), {})[strike_price] = close_price

    selected_months = select_option_pc_contract_months(
        (contract_month for contract_month, _option_type in grouped_by_month_type),
    )
    payload = empty_option_pc_payload()
    for bucket in OPTION_PC_BUCKETS:
        contract_month = selected_months.get(bucket["key"])
        if not contract_month:
            continue
        payload[bucket["month_field"]] = contract_month
        put_price = interpolate_option_price(
            grouped_by_month_type.get((contract_month, "PUT"), {}),
            etf_close,
        )
        call_price = interpolate_option_price(
            grouped_by_month_type.get((contract_month, "CALL"), {}),
            etf_close,
        )
        if put_price is None or call_price is None or call_price <= 0:
            continue
        payload[bucket["ratio_field"]] = put_price / call_price
    return payload


def build_exchange_option_flow_payload(trade_date, product_rows):
    totals = {
        "CALL": {"volume": 0.0, "turnover": 0.0, "volume_seen": False, "turnover_seen": False},
        "PUT": {"volume": 0.0, "turnover": 0.0, "volume_seen": False, "turnover_seen": False},
    }
    for row in product_rows or []:
        if is_exchange_option_contract_expired(row, trade_date):
            continue
        option_type = str(row.get("option_type") or "").strip().upper()
        if option_type not in totals:
            continue
        volume = to_float(row.get("volume"))
        turnover = to_float(row.get("turnover"))
        if volume is not None and volume >= 0:
            totals[option_type]["volume"] += volume
            totals[option_type]["volume_seen"] = True
        if turnover is not None and turnover >= 0:
            totals[option_type]["turnover"] += turnover
            totals[option_type]["turnover_seen"] = True

    payload = empty_option_flow_pc_payload()
    if totals["CALL"]["volume_seen"] and totals["CALL"]["volume"] > 0:
        payload["option_volume_pc_ratio"] = totals["PUT"]["volume"] / totals["CALL"]["volume"]
    if totals["CALL"]["turnover_seen"] and totals["CALL"]["turnover"] > 0:
        payload["option_turnover_pc_ratio"] = totals["PUT"]["turnover"] / totals["CALL"]["turnover"]
    return payload


def build_etf_close_map(rows):
    result = {}
    for row in rows or []:
        etf_code = str(row.get("etf_code") or "").strip()
        trade_date = normalize_date_text(row.get("trade_date"))
        close_price = to_float(row.get("close_price"))
        if not etf_code or not trade_date or close_price is None:
            continue
        result.setdefault((trade_date, etf_code), close_price)
    return result


def build_exchange_option_pc_map(option_rows, etf_close_map):
    rows_by_trade_source = {}
    for row in option_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        exchange = str(row.get("exchange") or "").strip().upper()
        underlying_code = str(row.get("underlying_code") or "").strip()
        if not trade_date or (exchange, underlying_code) not in EXCHANGE_OPTION_PRODUCT_META:
            continue
        rows_by_trade_source.setdefault((trade_date, exchange, underlying_code), []).append(row)

    index_names_by_source = {
        (exchange, product_code): index_name
        for index_name, sources in EXCHANGE_OPTION_PRODUCTS_BY_INDEX.items()
        for exchange, product_code in sources
    }
    result = {}
    for (trade_date, exchange, underlying_code), product_rows in rows_by_trade_source.items():
        etf_close = etf_close_map.get((trade_date, underlying_code))
        if etf_close is None:
            continue
        payload = empty_exchange_option_pc_payload(exchange, underlying_code)
        payload.update(
            build_exchange_option_price_payload(
                trade_date,
                etf_close,
                product_rows,
            )
        )
        payload.update(build_exchange_option_flow_payload(trade_date, product_rows))
        index_name = index_names_by_source[(exchange, underlying_code)]
        result.setdefault((trade_date, index_name), {})[payload["source_key"]] = payload
    return result


def build_risk_free_rate_curve_map(rate_rows):
    curves = {}
    for row in rate_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        tenor_days = to_int(row.get("tenor_days"))
        rate_decimal = to_float(row.get("rate_decimal"))
        if (
            not trade_date
            or tenor_days <= 0
            or rate_decimal is None
            or rate_decimal < 0
        ):
            continue
        curves.setdefault(trade_date, {})[tenor_days] = rate_decimal
    return curves


def resolve_risk_free_curve(rate_curve_map, trade_date):
    trade_date_text = normalize_date_text(trade_date)
    eligible_dates = [
        curve_date
        for curve_date in rate_curve_map
        if curve_date <= trade_date_text
    ]
    if not eligible_dates:
        return None, {}
    curve_date = max(eligible_dates)
    return curve_date, rate_curve_map[curve_date]


def interpolate_risk_free_rate(rate_curve, tenor_days):
    target_days = to_float(tenor_days)
    points = sorted(
        (to_float(days), to_float(rate))
        for days, rate in (rate_curve or {}).items()
        if to_float(days) is not None and to_float(rate) is not None
    )
    if target_days is None or not points:
        return None
    if target_days <= points[0][0]:
        return points[0][1]
    if target_days >= points[-1][0]:
        return points[-1][1]
    for index in range(1, len(points)):
        lower_days, lower_rate = points[index - 1]
        upper_days, upper_rate = points[index]
        if target_days <= upper_days:
            weight = (target_days - lower_days) / (upper_days - lower_days)
            return lower_rate + (upper_rate - lower_rate) * weight
    return None


def resolve_option_vix_price(row, price_mode="close"):
    if price_mode == "open":
        candidates = (("open_price", row.get("open_price")),)
    else:
        candidates = (
            ("close_price", row.get("close_price")),
            ("settle_price", row.get("settle_price")),
            ("pre_settle_price", row.get("pre_settle_price")),
        )
    for field_name, raw_value in candidates:
        value = to_float(raw_value)
        if value is not None and value > 0:
            return value, field_name
    return None, None


def resolve_option_vix_expiry(row, exchange):
    if exchange in {"SSE", "SZSE"}:
        return (
            parse_normalized_date(row.get("last_trade_date"))
            or parse_normalized_date(row.get("expire_date"))
        )
    return third_friday_of_contract_month(row.get("contract_month"))


def calculate_option_term_variance(
    product_rows,
    trade_date,
    expiry_date,
    risk_free_rate,
    price_mode="close",
):
    trade_day = parse_normalized_date(trade_date)
    if not trade_day or not expiry_date or expiry_date <= trade_day:
        return None
    minutes_to_expiry = (expiry_date - trade_day).days * 24 * 60
    if minutes_to_expiry <= 0:
        return None
    time_to_expiry = minutes_to_expiry / MINUTES_PER_YEAR

    strike_prices = {}
    price_basis_counts = {}
    pre_settle_sources = set()
    for row in product_rows or []:
        option_type = str(row.get("option_type") or "").strip().upper()
        strike_price = to_float(row.get("strike_price"))
        if option_type not in {"CALL", "PUT"} or strike_price is None:
            continue
        price, price_basis = resolve_option_vix_price(row, price_mode=price_mode)
        if price is None:
            continue
        strike_prices.setdefault(strike_price, {})[option_type] = price
        price_basis_counts[price_basis] = price_basis_counts.get(price_basis, 0) + 1
        if price_basis == "pre_settle_price":
            pre_settle_source = str(row.get("pre_settle_source") or "").strip()
            if pre_settle_source:
                pre_settle_sources.add(pre_settle_source)

    paired_strikes = [
        (strike, prices["CALL"], prices["PUT"])
        for strike, prices in strike_prices.items()
        if "CALL" in prices and "PUT" in prices
    ]
    if len(paired_strikes) < 2:
        return None
    forward_strike, call_price, put_price = min(
        paired_strikes,
        key=lambda item: abs(item[1] - item[2]),
    )
    forward = forward_strike + math.exp(risk_free_rate * time_to_expiry) * (
        call_price - put_price
    )
    k0_candidates = [strike for strike in strike_prices if strike <= forward]
    if not k0_candidates:
        return None
    k0 = max(k0_candidates)

    selected_prices = {}
    for strike, prices in strike_prices.items():
        if strike < k0 and "PUT" in prices:
            selected_prices[strike] = prices["PUT"]
        elif strike > k0 and "CALL" in prices:
            selected_prices[strike] = prices["CALL"]
        elif strike == k0 and "CALL" in prices and "PUT" in prices:
            selected_prices[strike] = (prices["CALL"] + prices["PUT"]) / 2

    strikes = sorted(selected_prices)
    if len(strikes) < 3 or k0 not in selected_prices:
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
            * selected_prices[strike]
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
        "days_to_expiry": minutes_to_expiry / (24 * 60),
        "forward": forward,
        "k0": k0,
        "strike_count": len(strikes),
        "risk_free_rate": risk_free_rate,
        "price_basis_counts": price_basis_counts,
        "pre_settle_sources": sorted(pre_settle_sources),
    }


def calculate_constant_30d_vix(term_results):
    valid_terms = sorted(
        (
            item
            for item in (term_results or [])
            if item
            and item["minutes_to_expiry"] > OPTION_VIX_ROLL_DAYS * 24 * 60
        ),
        key=lambda item: item["minutes_to_expiry"],
    )
    if not valid_terms:
        return None
    near = valid_terms[0]
    target_minutes = OPTION_VIX_TARGET_DAYS * 24 * 60
    if near["minutes_to_expiry"] >= target_minutes:
        annual_variance = near["variance"]
        next_term = None
    else:
        next_candidates = [
            item
            for item in valid_terms[1:]
            if item["minutes_to_expiry"] > near["minutes_to_expiry"]
        ]
        if not next_candidates:
            return None
        next_term = next_candidates[0]
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
        "vix_close": 100 * math.sqrt(annual_variance),
        "near": near,
        "next": next_term,
    }


def build_option_vix_payload(
    trade_date,
    exchange,
    product_code,
    product_rows,
    rate_curve_date,
    rate_curve,
):
    normalized_exchange = str(exchange or "").strip().upper()
    normalized_product = str(product_code or "").strip().upper()
    rows_by_expiry = {}
    for row in product_rows or []:
        if normalized_exchange in {"SSE", "SZSE"} and not is_standard_exchange_option_contract(row):
            continue
        expiry_date = resolve_option_vix_expiry(row, normalized_exchange)
        if expiry_date is None:
            continue
        rows_by_expiry.setdefault(expiry_date, []).append(row)

    def calculate_for_mode(price_mode):
        term_results = []
        for expiry_date, expiry_rows in rows_by_expiry.items():
            trade_day = parse_normalized_date(trade_date)
            if not trade_day or expiry_date <= trade_day:
                continue
            days_to_expiry = (expiry_date - trade_day).days
            risk_free_rate = interpolate_risk_free_rate(rate_curve, days_to_expiry)
            if risk_free_rate is None:
                continue
            term_result = calculate_option_term_variance(
                expiry_rows,
                trade_date,
                expiry_date,
                risk_free_rate,
                price_mode=price_mode,
            )
            if term_result is None:
                continue
            term_result["expiry_date"] = expiry_date.isoformat()
            contract_months = sorted(
                {
                    normalize_contract_month(row.get("contract_month"))
                    for row in expiry_rows
                    if normalize_contract_month(row.get("contract_month"))
                },
                key=contract_month_sort_key,
            )
            term_result["contract_month"] = (
                contract_months[0] if contract_months else None
            )
            term_results.append(term_result)
        return calculate_constant_30d_vix(term_results)

    open_result = calculate_for_mode("open")
    close_result = calculate_for_mode("close")
    result = close_result or open_result
    if result is None:
        return None
    near = result["near"]
    next_term = result["next"]
    basis_counts = dict(near.get("price_basis_counts") or {})
    pre_settle_sources = set(near.get("pre_settle_sources") or [])
    if next_term:
        for key, value in (next_term.get("price_basis_counts") or {}).items():
            basis_counts[key] = basis_counts.get(key, 0) + value
        pre_settle_sources.update(next_term.get("pre_settle_sources") or [])
    return {
        "source_key": f"{normalized_exchange.lower()}:{normalized_product}",
        "exchange": normalized_exchange,
        "exchange_label": EXCHANGE_LABELS.get(
            normalized_exchange,
            normalized_exchange,
        ),
        "product_code": normalized_product,
        "product_name": OPTION_VIX_PRODUCT_NAMES.get(
            (normalized_exchange, normalized_product),
            normalized_product,
        ),
        "vix_open": open_result["vix_close"] if open_result else None,
        "vix_close": close_result["vix_close"] if close_result else None,
        "vix_high": max(
            value
            for value in (
                open_result["vix_close"] if open_result else None,
                close_result["vix_close"] if close_result else None,
            )
            if value is not None
        ),
        "vix_low": min(
            value
            for value in (
                open_result["vix_close"] if open_result else None,
                close_result["vix_close"] if close_result else None,
            )
            if value is not None
        ),
        "near_contract_month": near.get("contract_month"),
        "near_expiry_date": near.get("expiry_date"),
        "near_strike_count": near.get("strike_count"),
        "next_contract_month": next_term.get("contract_month") if next_term else None,
        "next_expiry_date": next_term.get("expiry_date") if next_term else None,
        "next_strike_count": next_term.get("strike_count") if next_term else None,
        "risk_free_curve_date": normalize_date_text(rate_curve_date),
        "near_risk_free_rate": near.get("risk_free_rate"),
        "next_risk_free_rate": (
            next_term.get("risk_free_rate") if next_term else None
        ),
        "price_basis_counts": basis_counts,
        "pre_settle_sources": sorted(pre_settle_sources),
        "calculation_method": "ivix_30d_option_open_and_close",
        "uses_minute_ohlc": False,
        "minute_count": None,
        "minute_mid_quote_count": None,
    }


def build_option_vix_map(cffex_rows, exchange_rows, rate_rows):
    rate_curve_map = build_risk_free_rate_curve_map(rate_rows)
    rows_by_trade_source = {}
    for row in cffex_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        product_code = str(row.get("product_prefix") or "").strip().upper()
        if trade_date and product_code in {"HO", "IO", "MO"}:
            rows_by_trade_source.setdefault(
                (trade_date, "CFFEX", product_code),
                [],
            ).append(row)
    for row in exchange_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        exchange = str(row.get("exchange") or "").strip().upper()
        product_code = str(row.get("underlying_code") or "").strip()
        if (
            trade_date
            and (exchange, product_code) in EXCHANGE_OPTION_PRODUCT_META
        ):
            rows_by_trade_source.setdefault(
                (trade_date, exchange, product_code),
                [],
            ).append(row)

    index_by_source = {
        source: index_name
        for index_name, sources in OPTION_VIX_SOURCES_BY_INDEX.items()
        for source in sources
    }
    result = {}
    for (trade_date, exchange, product_code), product_rows in rows_by_trade_source.items():
        index_name = index_by_source.get((exchange, product_code))
        if not index_name:
            continue
        rate_curve_date, rate_curve = resolve_risk_free_curve(
            rate_curve_map,
            trade_date,
        )
        if not rate_curve:
            continue
        payload = build_option_vix_payload(
            trade_date,
            exchange,
            product_code,
            product_rows,
            rate_curve_date,
            rate_curve,
        )
        if payload:
            result.setdefault((trade_date, index_name), {})[
                payload["source_key"]
            ] = payload
    return result


def merge_option_vix_minute_ohlc(option_vix_map, minute_rows):
    result = option_vix_map or {}
    for row in minute_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        exchange = str(row.get("exchange") or "").strip().upper()
        product_code = str(row.get("product_code") or "").strip().upper()
        index_name = str(row.get("index_name") or "").strip()
        source_key = (
            f"{exchange.lower()}:{product_code}"
            if exchange and product_code
            else ""
        )
        if not index_name:
            index_name = next(
                (
                    name
                    for name, sources in OPTION_VIX_SOURCES_BY_INDEX.items()
                    if (exchange, product_code) in sources
                ),
                "",
            )
        if not trade_date or not source_key or not index_name:
            continue
        existing = result.setdefault((trade_date, index_name), {}).get(
            source_key,
            {},
        )
        price_basis = str(row.get("price_basis") or "").strip() or "mid_quote"
        minute_count = int(row.get("minute_count") or 0)
        mid_quote_count = int(row.get("mid_quote_count") or 0)
        if (
            minute_count < OPTION_VIX_MINUTE_MINIMUM_COUNT
            or mid_quote_count / minute_count < 0.8
        ):
            continue
        result[(trade_date, index_name)][source_key] = {
            **existing,
            "source_key": source_key,
            "exchange": exchange,
            "exchange_label": EXCHANGE_LABELS.get(exchange, exchange),
            "product_code": product_code,
            "product_name": OPTION_VIX_PRODUCT_NAMES.get(
                (exchange, product_code),
                product_code,
            ),
            "vix_open": to_float(row.get("vix_open")),
            "vix_high": to_float(row.get("vix_high")),
            "vix_low": to_float(row.get("vix_low")),
            "vix_close": to_float(row.get("vix_close")),
            "near_contract_month": row.get("near_contract_month"),
            "near_expiry_date": normalize_date_text(row.get("near_expire_date")),
            "near_strike_count": row.get("near_strike_count"),
            "next_contract_month": row.get("next_contract_month"),
            "next_expiry_date": normalize_date_text(row.get("next_expire_date")),
            "next_strike_count": row.get("next_strike_count"),
            "risk_free_curve_date": normalize_date_text(
                row.get("risk_free_curve_date")
            ),
            "near_risk_free_rate": to_float(row.get("near_risk_free_rate")),
            "next_risk_free_rate": to_float(row.get("next_risk_free_rate")),
            "price_basis_counts": {
                "mid_quote": mid_quote_count,
                "last_trade": minute_count - mid_quote_count,
            },
            "pre_settle_sources": [],
            "calculation_method": str(
                row.get("calculation_method") or "ivix_30d_minute"
            ).strip(),
            "uses_minute_ohlc": True,
            "minute_count": minute_count,
            "minute_mid_quote_count": mid_quote_count,
        }
    return result


def build_index_cffex_net_short_delta_map(position_rows, start_date=None, end_date=None):
    series = {}
    for row in position_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        source_key = str(row.get("source_key") or "").strip()
        product_code = str(row.get("product_code") or "").strip().upper()
        short_position = to_float(row.get("short_position"))
        long_position = to_float(row.get("long_position"))
        if not trade_date or source_key not in {"top20_institutions", "citic_customer"}:
            continue
        if product_code not in {"IH", "IF", "IC", "IM"} or short_position is None or long_position is None:
            continue
        series.setdefault(source_key, {}).setdefault(product_code, []).append(
            {
                "trade_date": trade_date,
                "net_position": short_position - long_position,
            }
        )

    product_delta_maps = {}
    for field_name, source_key, window in CFFEX_NET_SHORT_DELTA_FIELDS:
        product_delta_maps[field_name] = {}
        for product_code, points in series.get(source_key, {}).items():
            sorted_points = sorted(points, key=lambda item: item["trade_date"])
            product_delta_maps[field_name][product_code] = {}
            for index, point in enumerate(sorted_points):
                previous_point = sorted_points[index - window] if index >= window else None
                product_delta_maps[field_name][product_code][point["trade_date"]] = (
                    point["net_position"] - previous_point["net_position"]
                    if previous_point is not None
                    else None
                )

    all_trade_dates = sorted({
        trade_date
        for product_maps in product_delta_maps.values()
        for trade_date_map in product_maps.values()
        for trade_date in trade_date_map
    })
    start_text = normalize_date_text(start_date)
    end_text = normalize_date_text(end_date)
    if start_text:
        all_trade_dates = [trade_date for trade_date in all_trade_dates if trade_date >= start_text]
    if end_text:
        all_trade_dates = [trade_date for trade_date in all_trade_dates if trade_date <= end_text]

    result = {}
    for trade_date in all_trade_dates:
        for index_name, product_codes in CFFEX_NET_SHORT_PRODUCTS_BY_INDEX_NAME.items():
            payload = empty_cffex_net_short_delta_payload()
            for field_name, _source_key, _window in CFFEX_NET_SHORT_DELTA_FIELDS:
                values = [
                    product_delta_maps.get(field_name, {}).get(product_code, {}).get(trade_date)
                    for product_code in product_codes
                ]
                valid_values = [value for value in values if value is not None]
                payload[field_name] = sum(valid_values) if valid_values else None
            result[(trade_date, index_name)] = payload
    return result


def build_raw_core_basis_by_date(trade_dates, index_close_map, futures_close_map):
    result = {}
    for trade_date in trade_dates:
        result[trade_date] = {}
        for index_name in CORE_INDEX_NAMES:
            index_close = index_close_map.get((trade_date, index_name))
            symbol_meta = INDEX_FUTURES_SYMBOLS[index_name]
            main_close = futures_close_map.get((trade_date, symbol_meta["main_symbol"]))
            month_close = futures_close_map.get((trade_date, symbol_meta["month_symbol"]))
            result[trade_date][index_name] = {
                "main": (main_close - index_close) if main_close is not None and index_close is not None else None,
                "month": (month_close - index_close) if month_close is not None and index_close is not None else None,
            }
    return result


def build_index_basis_delta_map(raw_core_basis_by_date, trade_dates):
    sorted_trade_dates = sorted(normalize_date_text(trade_date) for trade_date in trade_dates if normalize_date_text(trade_date))
    product_delta_maps = {}
    for field_name, basis_kind, window in BASIS_DELTA_FIELDS:
        product_delta_maps[field_name] = {}
        for index_name in CORE_INDEX_NAMES:
            points = [
                {
                    "trade_date": trade_date,
                    "basis": (raw_core_basis_by_date.get(trade_date, {}).get(index_name) or {}).get(basis_kind),
                }
                for trade_date in sorted_trade_dates
            ]
            product_delta_maps[field_name][index_name] = {}
            for index, point in enumerate(points):
                current_value = point["basis"]
                previous_value = points[index - window]["basis"] if index >= window else None
                product_delta_maps[field_name][index_name][point["trade_date"]] = (
                    current_value - previous_value
                    if current_value is not None and previous_value is not None
                    else None
                )

    result = {}
    for trade_date in sorted_trade_dates:
        for index_name in INDEX_NAME_ORDER:
            payload = empty_basis_delta_payload()
            for field_name, _basis_kind, _window in BASIS_DELTA_FIELDS:
                if index_name == "上证指数":
                    values = [
                        product_delta_maps.get(field_name, {}).get(core_index_name, {}).get(trade_date)
                        for core_index_name in CORE_INDEX_NAMES
                    ]
                    valid_values = [value for value in values if value is not None]
                    payload[field_name] = sum(valid_values) if valid_values else None
                else:
                    payload[field_name] = product_delta_maps.get(field_name, {}).get(index_name, {}).get(trade_date)
            result[(trade_date, index_name)] = payload
    return result


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


def build_dashboard_rows(
    trade_dates,
    index_code_map,
    emotion_map,
    index_close_map,
    futures_close_map,
    breadth_map,
    option_pc_map=None,
    option_flow_pc_map=None,
    exchange_option_pc_map=None,
    option_vix_map=None,
    cffex_net_short_delta_map=None,
    basis_delta_trade_dates=None,
    fund_purchase_limit_map=None,
    margin_trading_map=None,
):
    rows = []
    option_pc_map = option_pc_map or {}
    option_flow_pc_map = option_flow_pc_map or {}
    exchange_option_pc_map = exchange_option_pc_map or {}
    option_vix_map = option_vix_map or {}
    cffex_net_short_delta_map = cffex_net_short_delta_map or {}
    fund_purchase_limit_map = fund_purchase_limit_map or {}
    margin_trading_map = margin_trading_map or {}
    basis_delta_dates = basis_delta_trade_dates or trade_dates
    raw_core_basis_by_date = build_raw_core_basis_by_date(basis_delta_dates, index_close_map, futures_close_map)
    basis_delta_map = build_index_basis_delta_map(raw_core_basis_by_date, basis_delta_dates)
    for trade_date in trade_dates:
        raw_core_emotions = {
            index_name: emotion_map.get((trade_date, index_name))
            for index_name in CORE_INDEX_NAMES
        }
        raw_core_basis = {
            index_name: {
                "main_basis": (raw_core_basis_by_date.get(trade_date, {}).get(index_name) or {}).get("main"),
                "month_basis": (raw_core_basis_by_date.get(trade_date, {}).get(index_name) or {}).get("month"),
            }
            for index_name in CORE_INDEX_NAMES
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
            elif index_name in CORE_INDEX_NAMES:
                emotion_value = raw_core_emotions.get(index_name)
                emotion_value = 50 if emotion_value is None else emotion_value
                main_basis = raw_core_basis[index_name]["main_basis"]
                month_basis = raw_core_basis[index_name]["month_basis"]
                main_basis = 0 if main_basis is None else main_basis
                month_basis = 0 if month_basis is None else month_basis
            else:
                emotion_value = 50
                main_basis = 0
                month_basis = 0

            option_pc_payload = option_pc_map.get((trade_date, index_name)) or empty_option_pc_payload()
            option_flow_pc_payload = option_flow_pc_map.get((trade_date, index_name)) or empty_option_flow_pc_payload()
            cffex_delta_payload = (
                cffex_net_short_delta_map.get((trade_date, index_name))
                or empty_cffex_net_short_delta_payload()
            )
            basis_delta_payload = basis_delta_map.get((trade_date, index_name)) or empty_basis_delta_payload()
            fund_purchase_limit_payload = (
                fund_purchase_limit_map.get(trade_date) or {}
                if index_name == "上证指数"
                else {}
            )
            margin_trading_payload = margin_trading_map.get(trade_date) or {}
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
                **option_pc_payload,
                **option_flow_pc_payload,
                "exchange_option_pc_json": exchange_option_pc_map.get((trade_date, index_name)) or {},
                "option_vix_json": option_vix_map.get((trade_date, index_name)) or {},
                **cffex_delta_payload,
                **basis_delta_payload,
                "fund_purchase_limit_count": fund_purchase_limit_payload.get(
                    "fund_purchase_limit_count"
                ),
                "fund_purchase_limit_total_count": fund_purchase_limit_payload.get(
                    "fund_purchase_limit_total_count"
                ),
                "fund_purchase_limit_pct": fund_purchase_limit_payload.get(
                    "fund_purchase_limit_pct"
                ),
                "margin_financing_balance": margin_trading_payload.get(
                    "margin_financing_balance"
                ),
                "margin_securities_lending_balance": margin_trading_payload.get(
                    "margin_securities_lending_balance"
                ),
                "margin_total_balance": margin_trading_payload.get(
                    "margin_total_balance"
                ),
                "margin_financing_net_buy_amount": margin_trading_payload.get(
                    "margin_financing_net_buy_amount"
                ),
                "margin_leverage_ratio_pct": margin_trading_payload.get(
                    "margin_leverage_ratio_pct"
                ),
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
    basis_delta_start_date = shift_date_text(start_date, -CFFEX_NET_SHORT_DELTA_LOOKBACK_DAYS)
    cn_trade_dates = await db_tools.get_quant_index_dashboard_trade_dates(
        INDEX_NAME_ORDER,
        start_date=start_date,
        end_date=end_date,
    )
    cn_basis_delta_trade_dates = await db_tools.get_quant_index_dashboard_trade_dates(
        INDEX_NAME_ORDER,
        start_date=basis_delta_start_date,
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
        basis_delta_start_date,
        end_date,
    )
    cn_index_close_map = build_index_close_map(cn_index_close_rows)
    emotion_rows = await db_tools.get_quant_index_dashboard_emotions(
        CORE_INDEX_NAMES,
        start_date,
        end_date,
    )
    futures_rows = await db_tools.get_quant_index_dashboard_futures_closes(
        [symbol for item in INDEX_FUTURES_SYMBOLS.values() for symbol in (item["main_symbol"], item["month_symbol"])],
        basis_delta_start_date,
        end_date,
    )
    breadth_rows = await db_tools.get_quant_index_dashboard_breadth(start_date, end_date)
    option_rows = await db_tools.get_quant_index_dashboard_option_closes(
        INDEX_OPTION_PRODUCTS.values(),
        start_date,
        end_date,
    )
    exchange_option_underlyings = sorted({
        product_code
        for sources in EXCHANGE_OPTION_PRODUCTS_BY_INDEX.values()
        for _exchange, product_code in sources
    })
    exchange_option_rows = await db_tools.get_quant_index_dashboard_exchange_option_rows(
        exchange_option_underlyings,
        start_date,
        end_date,
    )
    etf_close_rows = await db_tools.get_quant_index_dashboard_etf_closes(
        exchange_option_underlyings,
        start_date,
        end_date,
    )
    exchange_option_pc_map = build_exchange_option_pc_map(
        exchange_option_rows,
        build_etf_close_map(etf_close_rows),
    )
    rate_rows = await db_tools.get_cn_risk_free_rate_rows(
        shift_date_text(start_date, -14),
        end_date,
    )
    option_vix_map = build_option_vix_map(
        option_rows,
        exchange_option_rows,
        rate_rows,
    )
    option_vix_minute_rows = await db_tools.get_option_vix_minute_daily_ohlc(
        start_date,
        end_date,
    )
    option_vix_map = merge_option_vix_minute_ohlc(
        option_vix_map,
        option_vix_minute_rows,
    )
    cffex_position_rows = await db_tools.get_quant_index_dashboard_cffex_net_short_positions(
        shift_date_text(start_date, -CFFEX_NET_SHORT_DELTA_LOOKBACK_DAYS),
        end_date,
    )
    fund_purchase_limit_rows = await db_tools.get_fund_purchase_limit_daily_summary(
        start_date,
        end_date,
    )
    fund_purchase_limit_map = {
        normalize_date_text(item.get("trade_date")): {
            "fund_purchase_limit_count": int(item.get("limited_fund_count") or 0),
            "fund_purchase_limit_total_count": int(item.get("total_fund_count") or 0),
            "fund_purchase_limit_pct": to_float(item.get("limited_fund_pct")),
        }
        for item in fund_purchase_limit_rows
        if normalize_date_text(item.get("trade_date"))
    }
    margin_trading_rows = await db_tools.get_margin_trading_daily_summary(
        start_date,
        end_date,
    )
    margin_trading_map = {
        normalize_date_text(item.get("trade_date")): {
            "margin_financing_balance": to_float(
                item.get("margin_financing_balance")
            ),
            "margin_securities_lending_balance": to_float(
                item.get("margin_securities_lending_balance")
            ),
            "margin_total_balance": to_float(item.get("margin_total_balance")),
            "margin_financing_net_buy_amount": to_float(
                item.get("margin_financing_net_buy_amount")
            ),
            "margin_leverage_ratio_pct": to_float(
                item.get("margin_leverage_ratio_pct")
            ),
        }
        for item in margin_trading_rows
        if normalize_date_text(item.get("trade_date"))
    }

    rows = build_dashboard_rows(
        trade_dates=cn_trade_dates,
        index_code_map=index_code_map,
        emotion_map=build_emotion_map(emotion_rows),
        index_close_map=cn_index_close_map,
        futures_close_map=build_futures_close_map(futures_rows),
        breadth_map=build_breadth_map(breadth_rows),
        option_pc_map=build_index_option_pc_map(option_rows, cn_index_close_map),
        option_flow_pc_map=build_index_option_flow_pc_map(option_rows),
        exchange_option_pc_map=exchange_option_pc_map,
        option_vix_map=option_vix_map,
        cffex_net_short_delta_map=build_index_cffex_net_short_delta_map(
            cffex_position_rows,
            start_date=start_date,
            end_date=end_date,
        ),
        basis_delta_trade_dates=cn_basis_delta_trade_dates,
        fund_purchase_limit_map=fund_purchase_limit_map,
        margin_trading_map=margin_trading_map,
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

        cursor_date = datetime.strptime(actual_start, "%Y-%m-%d").date()
        final_date = datetime.strptime(actual_end, "%Y-%m-%d").date()
        affected = 0
        while cursor_date <= final_date:
            chunk_end = min(
                final_date,
                datetime(cursor_date.year, 12, 31).date(),
            )
            affected += await compute_and_upsert_range(
                db_tools,
                cursor_date.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
            )
            cursor_date = chunk_end + timedelta(days=1)
        return affected
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
