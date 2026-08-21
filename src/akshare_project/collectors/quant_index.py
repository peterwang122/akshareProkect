import asyncio
import bisect
import json
import math
import re
import statistics
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import akshare as ak

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools

LOGGER = get_logger("quant_index")
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_CN_TRADE_CALENDAR_CACHE = ()

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
MARGIN_FINANCING_NET_BUY_WINDOWS = CFFEX_NET_SHORT_DELTA_WINDOWS
MARGIN_FINANCING_NET_BUY_FIELDS = tuple(
    (f"margin_financing_net_buy_sum_{window}d", window)
    for window in MARGIN_FINANCING_NET_BUY_WINDOWS
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
SELF_SENTIMENT_LOOKBACK_DAYS = 500
SELF_SENTIMENT_PERCENTILE_WINDOW = 252
SELF_SENTIMENT_VERSION = "v4"
OPTION_SKEW_TARGET_DELTA = 0.25
OPTION_SKEW_MIN_DELTA = 0.10
OPTION_SKEW_MAX_DELTA = 0.40
OPTION_SKEW_MAX_ABS_VOL_POINTS = 50.0
OPTION_TERM_STRUCTURE_MIN_STRIKES = 5
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
RISK_TARGET_INDEX_NAME = "中证1000"
RISK_PERCENTILE_MAX_SAMPLES = 1260
RISK_PERCENTILE_MIN_SAMPLES = 252
RISK_LOOKBACK_CALENDAR_DAYS = 2200
RISK_VERSION = "v3"
RISK_GLOBAL_ASSET_CODES = (
    "KOSPI",
    "SOX",
    "IXN_NAV",
    "ACWI_NAV",
    "WTI",
    "BRENT",
    "COPPER_HG",
)


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


def clamp_score(value):
    numeric = to_float(value)
    if numeric is None or not math.isfinite(numeric):
        return None
    return max(0.0, min(100.0, numeric))


def rolling_percentile(values, index, window=SELF_SENTIMENT_PERCENTILE_WINDOW, inverse=False):
    current = to_float(values[index]) if 0 <= index < len(values) else None
    if current is None or not math.isfinite(current):
        return None
    start = max(0, index - window + 1)
    sample = [
        numeric
        for numeric in (to_float(value) for value in values[start:index + 1])
        if numeric is not None and math.isfinite(numeric)
    ]
    if len(sample) < 20:
        return None
    lower_count = sum(1 for value in sample if value < current)
    equal_count = sum(1 for value in sample if value == current)
    percentile = (lower_count + equal_count * 0.5) / len(sample) * 100.0
    return 100.0 - percentile if inverse else percentile


def strict_prior_percentile(
    values,
    index,
    max_samples=RISK_PERCENTILE_MAX_SAMPLES,
    min_samples=RISK_PERCENTILE_MIN_SAMPLES,
):
    """Return the current rank using only valid observations before index."""
    current = to_float(values[index]) if 0 <= index < len(values) else None
    if current is None or not math.isfinite(current):
        return None
    prior = [
        numeric
        for numeric in (
            to_float(value) for value in values[max(0, index - max_samples):index]
        )
        if numeric is not None and math.isfinite(numeric)
    ]
    if len(prior) < min_samples:
        return None
    prior.sort()
    return bisect.bisect_right(prior, current) / len(prior) * 100.0


def build_change_values(values, periods, percent=False):
    result = [None] * len(values)
    for index in range(periods, len(values)):
        current = to_float(values[index])
        previous = to_float(values[index - periods])
        if current is None or previous is None:
            continue
        if percent:
            if previous == 0:
                continue
            result[index] = (current / previous - 1.0) * 100.0
        else:
            result[index] = current - previous
    return result


def build_metric_points(dates, values, sources=None, available_ats=None):
    points = []
    for index, trade_date in enumerate(dates):
        value = to_float(values[index]) if index < len(values) else None
        points.append({
            "source_date": normalize_date_text(trade_date),
            "value": value,
            "percentile": strict_prior_percentile(values, index),
            "data_source": sources[index] if sources and index < len(sources) else None,
            "available_at": (
                available_ats[index]
                if available_ats and index < len(available_ats)
                else None
            ),
        })
    return points


def load_cn_trade_calendar_dates():
    """读取包含未来交易日的实际A股交易日历；调用失败时由上层降级为数据不完整。"""
    global _CN_TRADE_CALENDAR_CACHE
    today = datetime.now(SHANGHAI_TZ).date()
    required_end = (today + timedelta(days=30)).isoformat()
    if _CN_TRADE_CALENDAR_CACHE and _CN_TRADE_CALENDAR_CACHE[-1] >= required_end:
        return list(_CN_TRADE_CALENDAR_CACHE)
    frame = ak.tool_trade_date_hist_sina()
    values = sorted({
        normalize_date_text(value)
        for value in frame.get("trade_date", [])
        if normalize_date_text(value)
    })
    if not values:
        raise RuntimeError("A股交易日历为空")
    _CN_TRADE_CALENDAR_CACHE = tuple(values)
    return values


def align_metric_points_to_cn_dates(points, cn_trade_dates, max_stale_days=10):
    sorted_points = sorted(
        [point for point in points if point.get("source_date")],
        key=lambda point: point["source_date"],
    )
    result = {}
    point_index = 0
    latest = None
    for trade_date in sorted(cn_trade_dates):
        while (
            point_index < len(sorted_points)
            and sorted_points[point_index]["source_date"] < trade_date
        ):
            latest = sorted_points[point_index]
            point_index += 1
        if latest is None:
            continue
        age = (
            datetime.strptime(trade_date, "%Y-%m-%d").date()
            - datetime.strptime(latest["source_date"], "%Y-%m-%d").date()
        ).days
        if age <= max_stale_days:
            result[trade_date] = latest
    return result


def _parse_aware_datetime(value):
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed


def align_metric_points_to_cn_dates_by_available_at(
    points,
    cn_trade_dates,
    cn_calendar_dates=None,
    cutoff_time="09:20",
    max_stale_days=10,
):
    """按“下一A股交易日 cutoff_time 前已公开”为每个A股风险日选择最新外盘点。"""
    sorted_cn = sorted(
        normalize_date_text(value)
        for value in cn_trade_dates
        if normalize_date_text(value)
    )
    calendar_dates = sorted({
        normalize_date_text(value)
        for value in (cn_calendar_dates or sorted_cn)
        if normalize_date_text(value)
    })
    sorted_points = sorted(
        [point for point in points if point.get("source_date")],
        key=lambda point: point["source_date"],
    )
    result = {}
    for trade_date in sorted_cn:
        calendar_index = bisect.bisect_right(calendar_dates, trade_date)
        next_cn = (
            calendar_dates[calendar_index]
            if calendar_index < len(calendar_dates)
            else None
        )
        if next_cn is None:
            continue
        try:
            hour_text, _, minute_text = str(cutoff_time).partition(":")
            cutoff = datetime.combine(
                datetime.strptime(next_cn, "%Y-%m-%d").date(),
                datetime.min.time().replace(
                    hour=int(hour_text),
                    minute=int(minute_text or "0"),
                ),
                tzinfo=SHANGHAI_TZ,
            )
        except (TypeError, ValueError):
            continue
        eligible = []
        for point in sorted_points:
            source_date = point["source_date"]
            if source_date >= next_cn:
                continue
            available_at = _parse_aware_datetime(point.get("available_at"))
            if available_at is None:
                if source_date >= trade_date:
                    continue
            elif available_at > cutoff:
                continue
            eligible.append(point)
        if not eligible:
            continue
        latest = eligible[-1]
        age = (
            datetime.strptime(next_cn, "%Y-%m-%d").date()
            - datetime.strptime(latest["source_date"], "%Y-%m-%d").date()
        ).days
        if age <= max_stale_days:
            result[trade_date] = latest
    return result


def build_source_series(rows, value_key, predicate=None, default_source=None):
    deduped = {}
    for row in rows or []:
        if predicate and not predicate(row):
            continue
        trade_date = normalize_date_text(
            row.get("source_date") or row.get("trade_date")
        )
        value = to_float(row.get(value_key))
        if not trade_date or value is None or not math.isfinite(value):
            continue
        deduped[trade_date] = {
            "source_date": trade_date,
            "value": value,
            "data_source": str(row.get("data_source") or default_source or "").strip(),
            "available_at": str(row.get("available_at") or "").strip() or None,
        }
    return [deduped[key] for key in sorted(deduped)]


def transform_source_series(points, periods, percent=False):
    dates = [point["source_date"] for point in points]
    values = [point["value"] for point in points]
    sources = [point.get("data_source") for point in points]
    available_ats = [point.get("available_at") for point in points]
    return build_metric_points(
        dates,
        build_change_values(values, periods, percent=percent),
        sources,
        available_ats,
    )


def _latest_available_at(*values):
    candidates = [
        (parsed, str(value))
        for value in values
        if (parsed := _parse_aware_datetime(value)) is not None
    ]
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def combine_ratio_series(numerator_points, denominator_points, periods=10):
    numerator = {point["source_date"]: point for point in numerator_points}
    denominator = {point["source_date"]: point for point in denominator_points}
    dates = sorted(set(numerator) & set(denominator))
    values = []
    for trade_date in dates:
        denominator_value = denominator[trade_date]["value"]
        values.append(
            numerator[trade_date]["value"] / denominator_value
            if denominator_value and denominator_value > 0
            else None
        )
    changes = build_change_values(values, periods, percent=True)
    sources = ["blackrock_ishares_historical_nav" for _ in dates]
    available_ats = [
        _latest_available_at(
            numerator[trade_date].get("available_at"),
            denominator[trade_date].get("available_at"),
        )
        for trade_date in dates
    ]
    return build_metric_points(dates, changes, sources, available_ats)


def risk_condition(
    value,
    percentile,
    *,
    direction,
    absolute_threshold,
    percentile_threshold=None,
    data_date=None,
    data_source=None,
    label=None,
    unit=None,
    absolute_inclusive=True,
    available_at=None,
    level_value=None,
):
    numeric = to_float(value)
    rank = to_float(percentile)
    requires_rank = percentile_threshold is not None
    if numeric is None or (requires_rank and rank is None):
        return {
            "label": label,
            "value": numeric,
            "unit": unit,
            "direction": direction,
            "percentile": rank,
            "absolute_threshold": absolute_threshold,
            "percentile_threshold": percentile_threshold,
            "matched": None,
            "data_date": data_date,
            "data_source": data_source,
            "available_at": available_at,
            "level_value": level_value,
            "missing_reason": (
                "有效历史样本不足252个" if numeric is not None else "当日原始值缺失"
            ),
        }
    if direction == "high":
        absolute_matched = (
            numeric >= absolute_threshold
            if absolute_inclusive
            else numeric > absolute_threshold
        )
        matched = absolute_matched and (
            not requires_rank or rank >= percentile_threshold
        )
    else:
        absolute_matched = (
            numeric <= absolute_threshold
            if absolute_inclusive
            else numeric < absolute_threshold
        )
        matched = absolute_matched and (
            not requires_rank or rank <= percentile_threshold
        )
    return {
        "label": label,
        "value": numeric,
        "unit": unit,
        "direction": direction,
        "percentile": rank,
        "absolute_threshold": absolute_threshold,
        "percentile_threshold": percentile_threshold,
        "matched": matched,
        "data_date": data_date,
        "data_source": data_source,
        "available_at": available_at,
        "level_value": level_value,
        "missing_reason": None,
    }


def calculate_rsi_series(values, period=14):
    result = [None] * len(values)
    if len(values) <= period:
        return result
    gains = []
    losses = []
    for index in range(1, len(values)):
        current = to_float(values[index])
        previous = to_float(values[index - 1])
        change = current - previous if current is not None and previous is not None else 0.0
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    average_gain = sum(gains[:period]) / period
    average_loss = sum(losses[:period]) / period
    result[period] = 100.0 if average_loss == 0 else 100.0 - 100.0 / (1.0 + average_gain / average_loss)
    for index in range(period + 1, len(values)):
        average_gain = (average_gain * (period - 1) + gains[index - 1]) / period
        average_loss = (average_loss * (period - 1) + losses[index - 1]) / period
        result[index] = 100.0 if average_loss == 0 else 100.0 - 100.0 / (1.0 + average_gain / average_loss)
    return result


def _first_option_vix_value(option_vix_payload, index_name, field_name, positive_only=False):
    payload = option_vix_payload or {}
    for exchange, product_code in OPTION_VIX_SOURCES_BY_INDEX.get(index_name, []):
        value = to_float(
            (payload.get(f"{exchange.lower()}:{product_code}") or {}).get(field_name)
        )
        if value is not None and (not positive_only or value > 0):
            return value
    return None


def _self_sentiment_option_payloads(index_name, option_pc_payload, option_flow_payload, exchange_payload):
    if index_name == "中证500":
        for exchange, product_code in EXCHANGE_OPTION_PRODUCTS_BY_INDEX.get(index_name, []):
            payload = (exchange_payload or {}).get(f"{exchange.lower()}:{product_code}")
            if payload:
                return payload, payload
        return {}, {}
    return option_pc_payload or {}, option_flow_payload or {}


def build_self_sentiment_map(
    trade_dates,
    index_close_map,
    futures_close_map,
    option_pc_map,
    option_flow_pc_map,
    exchange_option_pc_map,
    option_vix_map,
    margin_financing_net_buy_sum_map=None,
    history_rows=None,
    output_start_date=None,
    output_end_date=None,
):
    margin_financing_net_buy_sum_map = margin_financing_net_buy_sum_map or {}
    raw_history = {}
    for row in history_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        index_name = str(row.get("index_name") or "").strip()
        raw_json = row.get("self_sentiment_components_json")
        try:
            payload = json.loads(raw_json) if isinstance(raw_json, str) else (raw_json or {})
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        raw_values = payload.get("raw_values") if isinstance(payload, dict) else None
        if trade_date and index_name and isinstance(raw_values, dict):
            raw_history[(trade_date, index_name)] = raw_values

    all_dates = sorted({normalize_date_text(value) for value in trade_dates if normalize_date_text(value)})
    all_dates = sorted(set(all_dates) | {date for date, name in index_close_map if name in CORE_INDEX_NAMES})
    normalized_output_start_date = normalize_date_text(output_start_date)
    core_results = {}
    component_keys = (
        "rsi14", "momentum20", "price_strength60", "realized_vol20",
        "vix", "vix_term_structure", "downside_skew_25d",
        "put_call_price", "put_call_volume", "put_call_turnover", "month_basis",
        "margin_financing_net_buy_30d",
    )
    for index_name in CORE_INDEX_NAMES:
        dates = [date for date in all_dates if index_close_map.get((date, index_name)) is not None]
        closes = [index_close_map.get((date, index_name)) for date in dates]
        rsi_values = calculate_rsi_series(closes)
        raw_by_component = {key: [] for key in component_keys}
        for index, trade_date in enumerate(dates):
            close_price = to_float(closes[index])
            previous20 = to_float(closes[index - 20]) if index >= 20 else None
            trailing60 = [to_float(value) for value in closes[max(0, index - 59):index + 1]]
            trailing60 = [value for value in trailing60 if value is not None]
            returns20 = []
            for return_index in range(max(1, index - 19), index + 1):
                current_close = to_float(closes[return_index])
                previous_close = to_float(closes[return_index - 1])
                if current_close is not None and previous_close not in (None, 0):
                    returns20.append(math.log(current_close / previous_close))
            realized_vol = None
            if len(returns20) >= 15:
                mean_return = sum(returns20) / len(returns20)
                variance = sum((value - mean_return) ** 2 for value in returns20) / len(returns20)
                realized_vol = math.sqrt(variance) * math.sqrt(252)

            price_payload, flow_payload = _self_sentiment_option_payloads(
                index_name,
                option_pc_map.get((trade_date, index_name)),
                option_flow_pc_map.get((trade_date, index_name)),
                exchange_option_pc_map.get((trade_date, index_name)),
            )
            main_symbol = INDEX_FUTURES_SYMBOLS[index_name]["month_symbol"]
            month_close = to_float(futures_close_map.get((trade_date, main_symbol)))
            historical_raw = raw_history.get((trade_date, index_name), {})
            raw_values = {
                "rsi14": to_float(rsi_values[index]),
                "momentum20": ((close_price / previous20) - 1.0) if close_price is not None and previous20 not in (None, 0) else None,
                "price_strength60": (
                    (close_price - min(trailing60)) / (max(trailing60) - min(trailing60)) * 100.0
                    if close_price is not None and len(trailing60) >= 40 and max(trailing60) > min(trailing60)
                    else None
                ),
                "realized_vol20": realized_vol,
                "vix": _first_option_vix_value(
                    option_vix_map.get((trade_date, index_name)),
                    index_name,
                    "vix_close",
                    positive_only=True,
                ),
                "vix_term_structure": _first_option_vix_value(
                    option_vix_map.get((trade_date, index_name)),
                    index_name,
                    "vix_term_structure",
                ),
                "downside_skew_25d": _first_option_vix_value(
                    option_vix_map.get((trade_date, index_name)),
                    index_name,
                    "downside_skew_25d",
                ),
                "put_call_price": to_float(price_payload.get("option_pc_current_month")),
                "put_call_volume": to_float(flow_payload.get("option_volume_pc_ratio")),
                "put_call_turnover": to_float(flow_payload.get("option_turnover_pc_ratio")),
                "month_basis": ((month_close - close_price) / close_price) if month_close is not None and close_price not in (None, 0) else None,
                "margin_financing_net_buy_30d": to_float(
                    (margin_financing_net_buy_sum_map.get(trade_date) or {}).get(
                        "margin_financing_net_buy_sum_30d"
                    )
                ),
            }
            for key in component_keys:
                if (
                    raw_values[key] is None
                    and (
                        not normalized_output_start_date
                        or trade_date < normalized_output_start_date
                    )
                ):
                    raw_values[key] = to_float(historical_raw.get(key))
                raw_by_component[key].append(raw_values[key])

        for index, trade_date in enumerate(dates):
            scores = {
                "rsi14": clamp_score(raw_by_component["rsi14"][index]),
                "momentum20": rolling_percentile(raw_by_component["momentum20"], index),
                "price_strength60": clamp_score(raw_by_component["price_strength60"][index]),
                "realized_vol20": rolling_percentile(raw_by_component["realized_vol20"], index, inverse=True),
                "vix": rolling_percentile(raw_by_component["vix"], index, inverse=True),
                "vix_term_structure": rolling_percentile(
                    raw_by_component["vix_term_structure"], index
                ),
                "downside_skew_25d": rolling_percentile(
                    raw_by_component["downside_skew_25d"], index, inverse=True
                ),
                "put_call_price": rolling_percentile(raw_by_component["put_call_price"], index, inverse=True),
                "put_call_volume": rolling_percentile(raw_by_component["put_call_volume"], index, inverse=True),
                "put_call_turnover": rolling_percentile(raw_by_component["put_call_turnover"], index, inverse=True),
                "month_basis": rolling_percentile(raw_by_component["month_basis"], index),
                "margin_financing_net_buy_30d": rolling_percentile(
                    raw_by_component["margin_financing_net_buy_30d"], index
                ),
            }
            core_values = [
                scores[key]
                for key in (
                    "rsi14",
                    "momentum20",
                    "price_strength60",
                    "realized_vol20",
                    "margin_financing_net_buy_30d",
                )
                if scores[key] is not None
            ]
            derivative_values = [
                scores[key]
                for key in (
                    "vix",
                    "vix_term_structure",
                    "downside_skew_25d",
                    "put_call_turnover",
                    "month_basis",
                )
                if scores[key] is not None
            ]
            core_score = average_or_none(core_values) if len(core_values) >= 3 else None
            derivative_score = average_or_none(derivative_values) if len(derivative_values) >= 2 else None
            all_scores = [*core_values, *derivative_values]
            score = (
                average_or_none([core_score, derivative_score])
                if core_score is not None and derivative_score is not None
                else core_score
            )
            core_results[(trade_date, index_name)] = {
                "self_sentiment_score": score,
                "self_sentiment_core_score": core_score,
                "self_sentiment_derivative_score": derivative_score,
                "self_sentiment_components_json": {
                    "version": SELF_SENTIMENT_VERSION,
                    "component_count": len(all_scores),
                    "scores": scores,
                    "raw_values": {key: raw_by_component[key][index] for key in component_keys},
                },
            }

    result = dict(core_results)
    for trade_date in all_dates:
        core_payloads = [core_results.get((trade_date, index_name)) for index_name in CORE_INDEX_NAMES]
        core_payloads = [payload for payload in core_payloads if payload and payload.get("self_sentiment_score") is not None]
        if core_payloads:
            aggregate_scores = {}
            for key in component_keys:
                aggregate_scores[key] = average_or_none([
                    (payload.get("self_sentiment_components_json") or {}).get("scores", {}).get(key)
                    for payload in core_payloads
                ])
            result[(trade_date, "上证指数")] = {
                "self_sentiment_score": average_or_none([payload.get("self_sentiment_score") for payload in core_payloads]),
                "self_sentiment_core_score": average_or_none([payload.get("self_sentiment_core_score") for payload in core_payloads]),
                "self_sentiment_derivative_score": average_or_none([payload.get("self_sentiment_derivative_score") for payload in core_payloads]),
                "self_sentiment_components_json": {
                    "version": SELF_SENTIMENT_VERSION,
                    "component_count": len([value for value in aggregate_scores.values() if value is not None]),
                    "scores": aggregate_scores,
                    "raw_values": {},
                    "aggregate": "core_index_equal_weight",
                },
            }
    if output_start_date or output_end_date:
        return {
            key: value for key, value in result.items()
            if (not output_start_date or key[0] >= output_start_date)
            and (not output_end_date or key[0] <= output_end_date)
        }
    return result


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


def empty_margin_financing_net_buy_payload():
    return {field_name: None for field_name, _window in MARGIN_FINANCING_NET_BUY_FIELDS}


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


def standard_normal_cdf(value):
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def black_76_option_price(forward, strike, time_to_expiry, risk_free_rate, volatility, option_type):
    if (
        forward <= 0
        or strike <= 0
        or time_to_expiry <= 0
        or volatility <= 0
    ):
        return None
    sqrt_time = math.sqrt(time_to_expiry)
    d1 = (
        math.log(forward / strike) + 0.5 * volatility * volatility * time_to_expiry
    ) / (volatility * sqrt_time)
    d2 = d1 - volatility * sqrt_time
    discount = math.exp(-risk_free_rate * time_to_expiry)
    if option_type == "CALL":
        return discount * (
            forward * standard_normal_cdf(d1) - strike * standard_normal_cdf(d2)
        )
    return discount * (
        strike * standard_normal_cdf(-d2) - forward * standard_normal_cdf(-d1)
    )


def solve_black_76_implied_volatility(
    option_price,
    forward,
    strike,
    time_to_expiry,
    risk_free_rate,
    option_type,
):
    price = to_float(option_price)
    if price is None or price <= 0 or forward <= 0 or strike <= 0 or time_to_expiry <= 0:
        return None
    discount = math.exp(-risk_free_rate * time_to_expiry)
    intrinsic = discount * max(
        forward - strike if option_type == "CALL" else strike - forward,
        0.0,
    )
    if price <= intrinsic + 1e-10:
        return None
    low = 1e-6
    high = 5.0
    high_price = black_76_option_price(
        forward,
        strike,
        time_to_expiry,
        risk_free_rate,
        high,
        option_type,
    )
    if high_price is None or high_price < price:
        return None
    for _ in range(80):
        middle = (low + high) / 2.0
        model_price = black_76_option_price(
            forward,
            strike,
            time_to_expiry,
            risk_free_rate,
            middle,
            option_type,
        )
        if model_price is None:
            return None
        if model_price < price:
            low = middle
        else:
            high = middle
    return (low + high) / 2.0


def black_76_absolute_delta(forward, strike, time_to_expiry, volatility, option_type):
    if forward <= 0 or strike <= 0 or time_to_expiry <= 0 or volatility <= 0:
        return None
    d1 = (
        math.log(forward / strike) + 0.5 * volatility * volatility * time_to_expiry
    ) / (volatility * math.sqrt(time_to_expiry))
    if option_type == "CALL":
        return standard_normal_cdf(d1)
    return standard_normal_cdf(-d1)


def calculate_25d_downside_skew(
    strike_prices,
    forward,
    time_to_expiry,
    risk_free_rate,
):
    candidates = {"CALL": [], "PUT": []}
    for strike, prices in strike_prices.items():
        for option_type in ("CALL", "PUT"):
            if option_type not in prices:
                continue
            if option_type == "CALL" and strike <= forward:
                continue
            if option_type == "PUT" and strike >= forward:
                continue
            implied_volatility = solve_black_76_implied_volatility(
                prices[option_type],
                forward,
                strike,
                time_to_expiry,
                risk_free_rate,
                option_type,
            )
            if implied_volatility is None:
                continue
            absolute_delta = black_76_absolute_delta(
                forward,
                strike,
                time_to_expiry,
                implied_volatility,
                option_type,
            )
            if (
                absolute_delta is None
                or absolute_delta < OPTION_SKEW_MIN_DELTA
                or absolute_delta > OPTION_SKEW_MAX_DELTA
            ):
                continue
            candidates[option_type].append(
                {
                    "strike": strike,
                    "absolute_delta": absolute_delta,
                    "implied_volatility": implied_volatility,
                }
            )
    selected = {}
    for option_type in ("CALL", "PUT"):
        if not candidates[option_type]:
            return None
        selected[option_type] = min(
            candidates[option_type],
            key=lambda item: abs(item["absolute_delta"] - OPTION_SKEW_TARGET_DELTA),
        )
    downside_skew = (
        selected["PUT"]["implied_volatility"]
        - selected["CALL"]["implied_volatility"]
    ) * 100.0
    if abs(downside_skew) > OPTION_SKEW_MAX_ABS_VOL_POINTS:
        return None
    return {
        "downside_skew_25d": downside_skew,
        "put_25d_implied_volatility": selected["PUT"]["implied_volatility"] * 100.0,
        "put_25d_delta": selected["PUT"]["absolute_delta"],
        "put_25d_strike": selected["PUT"]["strike"],
        "call_25d_implied_volatility": selected["CALL"]["implied_volatility"] * 100.0,
        "call_25d_delta": selected["CALL"]["absolute_delta"],
        "call_25d_strike": selected["CALL"]["strike"],
    }


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
    result = {
        "variance": variance,
        "term_vix": 100.0 * math.sqrt(variance),
        "minutes_to_expiry": minutes_to_expiry,
        "days_to_expiry": minutes_to_expiry / (24 * 60),
        "forward": forward,
        "k0": k0,
        "strike_count": len(strikes),
        "risk_free_rate": risk_free_rate,
        "price_basis_counts": price_basis_counts,
        "pre_settle_sources": sorted(pre_settle_sources),
    }
    skew_payload = calculate_25d_downside_skew(
        strike_prices,
        forward,
        time_to_expiry,
        risk_free_rate,
    )
    if skew_payload:
        result.update(skew_payload)
    return result


def interpolate_constant_30d_metric(near, next_term, field_name):
    near_value = to_float((near or {}).get(field_name))
    if near_value is None:
        return None
    target_minutes = OPTION_VIX_TARGET_DAYS * 24 * 60
    near_minutes = to_float((near or {}).get("minutes_to_expiry"))
    if near_minutes is None or near_minutes >= target_minutes:
        return near_value
    next_value = to_float((next_term or {}).get(field_name))
    next_minutes = to_float((next_term or {}).get("minutes_to_expiry"))
    if next_value is None or next_minutes is None or next_minutes <= near_minutes:
        return None
    near_weight = (next_minutes - target_minutes) / (next_minutes - near_minutes)
    next_weight = (target_minutes - near_minutes) / (next_minutes - near_minutes)
    return near_value * near_weight + next_value * next_weight


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
    curve_next = valid_terms[1] if len(valid_terms) > 1 else None
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
        "curve_near": near,
        "curve_next": curve_next,
        "downside_skew_25d": interpolate_constant_30d_metric(
            near,
            next_term,
            "downside_skew_25d",
        ),
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
    curve_near = result.get("curve_near") or near
    curve_next = result.get("curve_next")
    near_term_vix = to_float(curve_near.get("term_vix"))
    next_term_vix = to_float((curve_next or {}).get("term_vix"))
    term_structure_is_usable = (
        near_term_vix is not None
        and next_term_vix is not None
        and int(curve_near.get("strike_count") or 0) >= OPTION_TERM_STRUCTURE_MIN_STRIKES
        and int((curve_next or {}).get("strike_count") or 0) >= OPTION_TERM_STRUCTURE_MIN_STRIKES
    )
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
        "near_term_vix": near_term_vix,
        "next_term_vix": next_term_vix,
        "vix_term_structure": (
            next_term_vix - near_term_vix
            if term_structure_is_usable
            else None
        ),
        "downside_skew_25d": to_float(result.get("downside_skew_25d")),
        "near_put_25d_implied_volatility": to_float(
            curve_near.get("put_25d_implied_volatility")
        ),
        "near_call_25d_implied_volatility": to_float(
            curve_near.get("call_25d_implied_volatility")
        ),
        "near_put_25d_strike": to_float(curve_near.get("put_25d_strike")),
        "near_call_25d_strike": to_float(curve_near.get("call_25d_strike")),
        "near_contract_month": near.get("contract_month"),
        "near_expiry_date": near.get("expiry_date"),
        "near_strike_count": near.get("strike_count"),
        "next_contract_month": next_term.get("contract_month") if next_term else None,
        "next_expiry_date": next_term.get("expiry_date") if next_term else None,
        "next_strike_count": next_term.get("strike_count") if next_term else None,
        "curve_next_contract_month": (
            curve_next.get("contract_month") if curve_next else None
        ),
        "curve_next_expiry_date": (
            curve_next.get("expiry_date") if curve_next else None
        ),
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


def build_margin_financing_net_buy_sum_map(margin_trading_map, trade_dates):
    sorted_trade_dates = sorted({
        normalize_date_text(trade_date)
        for trade_date in trade_dates
        if normalize_date_text(trade_date)
    })
    values = [
        to_float((margin_trading_map.get(trade_date) or {}).get("margin_financing_net_buy_amount"))
        for trade_date in sorted_trade_dates
    ]
    result = {}
    for index, trade_date in enumerate(sorted_trade_dates):
        payload = empty_margin_financing_net_buy_payload()
        for field_name, window in MARGIN_FINANCING_NET_BUY_FIELDS:
            start_index = index - window + 1
            window_values = values[start_index:index + 1] if start_index >= 0 else []
            payload[field_name] = (
                sum(window_values)
                if len(window_values) == window and all(value is not None for value in window_values)
                else None
            )
        result[trade_date] = payload
    return result


def build_dominant_im_basis_metrics(trade_dates, index_close_map, futures_rows):
    dominant_by_date = {}
    for row in futures_rows or []:
        trade_date = normalize_date_text(row.get("trade_date"))
        symbol = str(row.get("symbol") or "").strip().upper()
        open_interest = to_float(row.get("open_interest"))
        close_price = to_float(row.get("close_price"))
        if not trade_date or not re.fullmatch(r"IM\d{4}", symbol):
            continue
        if open_interest is None or close_price is None:
            continue
        existing = dominant_by_date.get(trade_date)
        if existing is None or open_interest > existing["open_interest"]:
            dominant_by_date[trade_date] = {
                "symbol": symbol,
                "close_price": close_price,
                "open_interest": open_interest,
                "data_source": str(row.get("data_source") or "").strip(),
            }

    dates = sorted({normalize_date_text(value) for value in trade_dates if normalize_date_text(value)})
    basis_values = []
    sources = []
    contracts = []
    for trade_date in dates:
        contract = dominant_by_date.get(trade_date)
        index_close = index_close_map.get((trade_date, RISK_TARGET_INDEX_NAME))
        basis_values.append(
            (contract["close_price"] / index_close - 1.0) * 10000.0
            if contract and index_close and index_close > 0
            else None
        )
        sources.append(contract.get("data_source") if contract else None)
        contracts.append(contract.get("symbol") if contract else None)

    delta_14 = build_change_values(basis_values, 14)
    delta_30 = build_change_values(basis_values, 30)
    return {
        "level": {point["source_date"]: {**point, "contract": contracts[index]}
                  for index, point in enumerate(build_metric_points(dates, basis_values, sources))},
        "delta_14d": {point["source_date"]: {**point, "contract": contracts[index]}
                      for index, point in enumerate(build_metric_points(dates, delta_14, sources))},
        "delta_30d": {point["source_date"]: {**point, "contract": contracts[index]}
                      for index, point in enumerate(build_metric_points(dates, delta_30, sources))},
    }


def build_tech_concentration_points(rows):
    turnover_by_code = {"sh000985": {}, "sh000993": {}}
    source_by_date = {}
    for row in rows or []:
        index_code = str(row.get("index_code") or "").strip().lower()
        trade_date = normalize_date_text(row.get("trade_date"))
        turnover = to_float(row.get("turnover"))
        if index_code not in turnover_by_code or not trade_date or not turnover or turnover <= 0:
            continue
        turnover_by_code[index_code][trade_date] = turnover
        source_by_date[trade_date] = str(row.get("data_source") or "").strip()
    dates = sorted(set(turnover_by_code["sh000985"]) & set(turnover_by_code["sh000993"]))
    shares = [
        turnover_by_code["sh000993"][trade_date]
        / turnover_by_code["sh000985"][trade_date]
        * 100.0
        for trade_date in dates
    ]
    changes_5d = build_change_values(shares, 5)
    points = []
    for index, trade_date in enumerate(dates):
        recent_start = max(0, index - 19)
        crowded_high = max(shares[recent_start:index + 1])
        prior = [
            value
            for value in shares[max(0, index - RISK_PERCENTILE_MAX_SAMPLES):index]
            if value is not None and math.isfinite(value)
        ]
        crowded_percentile = None
        if len(prior) >= RISK_PERCENTILE_MIN_SAMPLES:
            prior.sort()
            crowded_percentile = bisect.bisect_right(prior, crowded_high) / len(prior) * 100.0
        points.append({
            "source_date": trade_date,
            "value": shares[index],
            "percentile": strict_prior_percentile(shares, index),
            "crowded_high": crowded_high,
            "crowded_percentile": crowded_percentile,
            "change_5d": changes_5d[index],
            "change_5d_percentile": strict_prior_percentile(changes_5d, index),
            "data_source": source_by_date.get(trade_date),
        })
    return points


def _metric_lookup(points):
    return {point["source_date"]: point for point in points if point.get("source_date")}


def _treasury_series_with_available_at(rows, value_key):
    points = build_source_series(rows, value_key, default_source="fred_public_csv")
    available_by_date = {
        normalize_date_text(row.get("trade_date")): str(row.get("available_at") or "") or None
        for row in rows or []
        if normalize_date_text(row.get("trade_date"))
    }
    return [
        {**point, "available_at": available_by_date.get(point["source_date"])}
        for point in points
    ]


def _attach_available_at(points, rows):
    available_by_date = {
        normalize_date_text(row.get("trade_date")): str(row.get("available_at") or "") or None
        for row in rows or []
        if normalize_date_text(row.get("trade_date"))
    }
    return [
        {**point, "available_at": available_by_date.get(point["source_date"])}
        for point in points
    ]


def build_usd_rate_shock_state(
    nominal_point,
    real_point,
    sox_point,
    relative_point,
    nominal_level_value=None,
    real_level_value=None,
):
    """美元利率冲击模式：名义/实际10年美债5D变化+百分位，加SOX或IXN/ACWI市场确认。"""
    nominal_condition = risk_condition(
        nominal_point.get("value"), nominal_point.get("percentile"), direction="high",
        absolute_threshold=0.20, percentile_threshold=80.0,
        data_date=nominal_point.get("source_date"), data_source=nominal_point.get("data_source"),
        label="名义10年美债收益率5D变化", unit="百分点",
        available_at=nominal_point.get("available_at"),
        level_value=nominal_level_value,
    )
    real_condition = risk_condition(
        real_point.get("value"), real_point.get("percentile"), direction="high",
        absolute_threshold=0.15, percentile_threshold=80.0,
        data_date=real_point.get("source_date"), data_source=real_point.get("data_source"),
        label="实际10年美债收益率5D变化", unit="百分点",
        available_at=real_point.get("available_at"),
        level_value=real_level_value,
    )
    sox_market_condition = risk_condition(
        sox_point.get("value"), sox_point.get("percentile"), direction="low",
        absolute_threshold=-5.0,
        data_date=sox_point.get("source_date"), data_source=sox_point.get("data_source"),
        label="SOX 10D", unit="%",
        available_at=sox_point.get("available_at"),
    )
    relative_market_condition = risk_condition(
        relative_point.get("value"), relative_point.get("percentile"), direction="low",
        absolute_threshold=-2.0,
        data_date=relative_point.get("source_date"),
        data_source=relative_point.get("data_source"),
        label="IXN/ACWI相对收益10D", unit="%",
        available_at=relative_point.get("available_at"),
    )
    sox_matched = sox_market_condition["matched"]
    relative_matched = relative_market_condition["matched"]
    if sox_matched is True or relative_matched is True:
        market_matched = True
        market_missing = None
    elif sox_matched is False and relative_matched is False:
        market_matched = False
        market_missing = None
    else:
        market_matched = None
        market_missing = "SOX 或 IXN/ACWI 相对收益存在缺失，市场确认不完整"
    market_condition = {
        "label": "市场确认（SOX 或 IXN/ACWI 相对收益）",
        "value": None,
        "unit": None,
        "direction": "low",
        "absolute_threshold": "-5% 或 -2%",
        "percentile_threshold": None,
        "matched": market_matched,
        "data_date": max(filter(None, (
            sox_market_condition.get("data_date"),
            relative_market_condition.get("data_date"),
        )), default=None),
        "data_source": "+".join(filter(None, (
            sox_market_condition.get("data_source"),
            relative_market_condition.get("data_source"),
        ))) or None,
        "available_at": _latest_available_at(
            sox_market_condition.get("available_at"),
            relative_market_condition.get("available_at"),
        ),
        "missing_reason": market_missing,
        "components": [sox_market_condition, relative_market_condition],
    }
    rate_core = [nominal_condition, real_condition, market_condition]
    rate_complete = all(item["matched"] is not None for item in rate_core)
    rate_count = sum(1 for item in rate_core if item["matched"])
    rate_active = rate_count == 3 if rate_complete else None
    rate_score = rate_count / 3.0 * 100.0 if rate_complete else None
    return {
        "complete": rate_complete,
        "active": rate_active,
        "score": rate_score,
        "matched_condition_count": rate_count,
        "components": rate_core,
    }


def _combined_metric_points(first_points, second_points, combine):
    first = _metric_lookup(first_points)
    second = _metric_lookup(second_points)
    dates = sorted(set(first) & set(second))
    values = []
    for trade_date in dates:
        first_value = to_float(first[trade_date].get("value"))
        second_value = to_float(second[trade_date].get("value"))
        values.append(
            combine(first_value, second_value)
            if first_value is not None and second_value is not None
            else None
        )
    sources = [
        "+".join(filter(None, (first[trade_date].get("data_source"), second[trade_date].get("data_source"))))
        for trade_date in dates
    ]
    available_ats = [
        _latest_available_at(
            first[trade_date].get("available_at"),
            second[trade_date].get("available_at"),
        )
        for trade_date in dates
    ]
    return build_metric_points(dates, values, sources, available_ats)


def _status_from_conditions(conditions):
    statuses = [condition.get("matched") for condition in conditions]
    if any(status is None for status in statuses):
        return None, None
    matched_count = sum(1 for status in statuses if status)
    return matched_count == len(statuses), matched_count / len(statuses) * 100.0


def build_risk_strategy_map(
    trade_dates,
    index_close_map,
    option_pc_map,
    cffex_net_short_delta_map,
    margin_financing_net_buy_sum_map,
    im_futures_rows,
    global_asset_rows,
    us_index_rows,
    hk_index_rows,
    us_vix_rows,
    us_credit_rows,
    us_treasury_rows=None,
    turnover_concentration_rows=None,
    cn_calendar_dates=None,
    output_start_date=None,
    output_end_date=None,
):
    dates = sorted({normalize_date_text(value) for value in trade_dates if normalize_date_text(value)})
    us_treasury_rows = us_treasury_rows or []
    turnover_concentration_rows = turnover_concentration_rows or []
    output_start = normalize_date_text(output_start_date)
    output_end = normalize_date_text(output_end_date)

    margin_120_values = [
        to_float((margin_financing_net_buy_sum_map.get(day) or {}).get("margin_financing_net_buy_sum_120d"))
        for day in dates
    ]
    margin_5_values = [
        to_float((margin_financing_net_buy_sum_map.get(day) or {}).get("margin_financing_net_buy_sum_5d"))
        for day in dates
    ]
    citic_14_values = [
        to_float((cffex_net_short_delta_map.get((day, RISK_TARGET_INDEX_NAME)) or {}).get(
            "cffex_citic_net_short_delta_14d"
        ))
        for day in dates
    ]
    pc_median_values = []
    pc_valid_counts = []
    for day in dates:
        payload = option_pc_map.get((day, RISK_TARGET_INDEX_NAME)) or {}
        values = [
            to_float(payload.get(field))
            for field in (
                "option_pc_current_month",
                "option_pc_next_month",
                "option_pc_quarter_1",
                "option_pc_quarter_2",
            )
        ]
        valid = [value for value in values if value is not None and value > 0]
        pc_valid_counts.append(len(valid))
        pc_median_values.append(statistics.median(valid) if len(valid) >= 2 else None)

    margin_120_points = _metric_lookup(build_metric_points(dates, margin_120_values))
    margin_5_points = _metric_lookup(build_metric_points(dates, margin_5_values))
    citic_14_points = _metric_lookup(build_metric_points(dates, citic_14_values))
    pc_median_points = _metric_lookup(build_metric_points(dates, pc_median_values))
    concentration_by_date = {
        normalize_date_text(row.get("trade_date")): row
        for row in turnover_concentration_rows or []
        if normalize_date_text(row.get("trade_date"))
    }
    concentration_top5_points = _metric_lookup(build_metric_points(
        dates,
        [to_float((concentration_by_date.get(day) or {}).get("top5_pct")) for day in dates],
        [str((concentration_by_date.get(day) or {}).get("top5_data_source") or "") for day in dates],
    ))
    im_metrics = build_dominant_im_basis_metrics(dates, index_close_map, im_futures_rows)

    assets = {}
    for asset_code in RISK_GLOBAL_ASSET_CODES:
        assets[asset_code] = build_source_series(
            global_asset_rows,
            "close_value",
            predicate=lambda row, code=asset_code: str(row.get("asset_code") or "").strip().upper() == code,
        )
    us_series = {
        name: build_source_series(
            us_index_rows,
            "close_price",
            predicate=lambda row, target=name: str(row.get("index_name") or "").strip() == target,
            default_source="index_us_daily_data",
        )
        for name in ("标普500指数", "纳斯达克100指数")
    }
    hk_series = {
        name: build_source_series(
            hk_index_rows,
            "close_price",
            predicate=lambda row, target=name: str(row.get("index_name") or "").strip() == target,
            default_source="index_hk_daily_data",
        )
        for name in ("恒生指数", "恒生科技指数")
    }

    source_metrics = {
        "spx_return_10d": transform_source_series(us_series["标普500指数"], 10, percent=True),
        "ndx_return_10d": transform_source_series(us_series["纳斯达克100指数"], 10, percent=True),
        "hsi_return_10d": transform_source_series(hk_series["恒生指数"], 10, percent=True),
        "hstech_return_10d": transform_source_series(hk_series["恒生科技指数"], 10, percent=True),
        "kospi_return_10d": transform_source_series(assets["KOSPI"], 10, percent=True),
        "sox_return_10d": transform_source_series(assets["SOX"], 10, percent=True),
        "copper_return_10d": transform_source_series(assets["COPPER_HG"], 10, percent=True),
    }
    wti_return = transform_source_series(assets["WTI"], 10, percent=True)
    brent_return = transform_source_series(assets["BRENT"], 10, percent=True)
    source_metrics["oil_return_10d"] = _combined_metric_points(
        wti_return, brent_return, lambda first, second: (first + second) / 2.0
    )
    source_metrics["tech_relative_return_10d"] = combine_ratio_series(
        assets["IXN_NAV"], assets["ACWI_NAV"], periods=10
    )

    vix_level = build_source_series(us_vix_rows, "close_value", default_source="cboe_vix_history")
    credit_level = _attach_available_at(
        build_source_series(
            us_credit_rows,
            "high_yield_oas",
            default_source="fred_public_csv",
        ),
        us_credit_rows,
    )
    treasury_nominal = _treasury_series_with_available_at(us_treasury_rows, "yield_10y")
    treasury_real = _treasury_series_with_available_at(us_treasury_rows, "yield_real_10y")
    nominal_change_5d = _attach_available_at(
        transform_source_series(treasury_nominal, 5),
        us_treasury_rows,
    )
    real_change_5d = _attach_available_at(
        transform_source_series(treasury_real, 5),
        us_treasury_rows,
    )
    source_metrics["vix_level"] = build_metric_points(
        [point["source_date"] for point in vix_level],
        [point["value"] for point in vix_level],
        [point.get("data_source") for point in vix_level],
    )
    source_metrics["vix_change_5d"] = transform_source_series(vix_level, 5)
    source_metrics["hy_oas_level"] = build_metric_points(
        [point["source_date"] for point in credit_level],
        [point["value"] for point in credit_level],
        [point.get("data_source") for point in credit_level],
    )
    source_metrics["hy_oas_change_5d"] = transform_source_series(credit_level, 5)
    source_metrics["nominal_10y_change_5d"] = nominal_change_5d
    source_metrics["real_10y_change_5d"] = real_change_5d
    aligned = {
        key: align_metric_points_to_cn_dates(points, dates)
        for key, points in source_metrics.items()
    }
    aligned_by_available = {
        "nominal_10y_change_5d": align_metric_points_to_cn_dates_by_available_at(
            source_metrics["nominal_10y_change_5d"],
            dates,
            cn_calendar_dates,
        ),
        "real_10y_change_5d": align_metric_points_to_cn_dates_by_available_at(
            source_metrics["real_10y_change_5d"],
            dates,
            cn_calendar_dates,
        ),
        "nominal_10y_level": align_metric_points_to_cn_dates_by_available_at(
            treasury_nominal,
            dates,
            cn_calendar_dates,
        ),
        "real_10y_level": align_metric_points_to_cn_dates_by_available_at(
            treasury_real,
            dates,
            cn_calendar_dates,
        ),
        "sox_return_10d": align_metric_points_to_cn_dates_by_available_at(
            source_metrics["sox_return_10d"],
            dates,
            cn_calendar_dates,
        ),
        "tech_relative_return_10d": align_metric_points_to_cn_dates_by_available_at(
            source_metrics["tech_relative_return_10d"],
            dates,
            cn_calendar_dates,
        ),
        "hy_oas_level": align_metric_points_to_cn_dates_by_available_at(
            _attach_available_at(source_metrics["hy_oas_level"], us_credit_rows),
            dates,
            cn_calendar_dates,
        ),
        "hy_oas_change_5d": align_metric_points_to_cn_dates_by_available_at(
            _attach_available_at(source_metrics["hy_oas_change_5d"], us_credit_rows),
            dates,
            cn_calendar_dates,
        ),
    }

    results = {}
    global_stock_labels = (
        ("spx_return_10d", "标普500 10D"),
        ("ndx_return_10d", "纳斯达克100 10D"),
        ("hsi_return_10d", "恒生指数 10D"),
        ("hstech_return_10d", "恒生科技 10D"),
        ("kospi_return_10d", "KOSPI 10D"),
        ("sox_return_10d", "SOX 10D"),
    )
    for date_index, trade_date in enumerate(dates):
        if output_start and trade_date < output_start:
            continue
        if output_end and trade_date > output_end:
            continue

        margin_120 = margin_120_points.get(trade_date) or {}
        pc_median = pc_median_points.get(trade_date) or {}
        im_30 = im_metrics["delta_30d"].get(trade_date) or {}
        yellow_conditions = [
            risk_condition(
                margin_120.get("value"), margin_120.get("percentile"),
                direction="high", absolute_threshold=0.0, percentile_threshold=80.0,
                data_date=trade_date, data_source="margin_trading_daily_data",
                label="融资净买入累计120D", unit="元", absolute_inclusive=False,
            ),
            risk_condition(
                pc_median.get("value"), pc_median.get("percentile"),
                direction="high", absolute_threshold=1.60, percentile_threshold=80.0,
                data_date=trade_date, data_source="cffex_option_daily_data",
                label="MO四期限价格P/C中位数", unit="倍",
            ),
            risk_condition(
                im_30.get("value"), im_30.get("percentile"),
                direction="low", absolute_threshold=-100.0, percentile_threshold=20.0,
                data_date=trade_date, data_source=im_30.get("data_source"),
                label="真实IM主力期现差率30D变化", unit="bp",
            ),
        ]
        if pc_valid_counts[date_index] < 2:
            yellow_conditions[1]["matched"] = None
            yellow_conditions[1]["missing_reason"] = "MO四期限中少于两个期限有效"
        yellow_active, yellow_score = _status_from_conditions(yellow_conditions)
        concentration_source = concentration_by_date.get(trade_date) or {}
        top5_point = concentration_top5_points.get(trade_date) or {}
        concentration_observations = [
            risk_condition(
                top5_point.get("value"), top5_point.get("percentile"),
                direction="high", absolute_threshold=45.0, percentile_threshold=80.0,
                data_date=trade_date,
                data_source=concentration_source.get("top5_data_source"),
                label="A股成交额前5%集中度MA5", unit="%",
            ),
        ]

        im_14 = im_metrics["delta_14d"].get(trade_date) or {}
        citic_14 = citic_14_points.get(trade_date) or {}
        margin_5 = margin_5_points.get(trade_date) or {}
        red_conditions = [
            risk_condition(
                im_14.get("value"), im_14.get("percentile"),
                direction="low", absolute_threshold=-75.0, percentile_threshold=20.0,
                data_date=trade_date, data_source=im_14.get("data_source"),
                label="真实IM主力期现差率14D变化", unit="bp",
            ),
            risk_condition(
                citic_14.get("value"), citic_14.get("percentile"),
                direction="high", absolute_threshold=3000.0, percentile_threshold=80.0,
                data_date=trade_date, data_source="cffex_member_rankings",
                label="中信净空单14D增量", unit="手",
            ),
            risk_condition(
                margin_5.get("value"), margin_5.get("percentile"),
                direction="low", absolute_threshold=-6_000_000_000.0, percentile_threshold=20.0,
                data_date=trade_date, data_source="margin_trading_daily_data",
                label="融资净买入累计5D", unit="元",
            ),
        ]
        red_active, red_score = _status_from_conditions(red_conditions)

        stock_conditions = []
        for key, label in global_stock_labels:
            point = aligned[key].get(trade_date) or {}
            stock_conditions.append(risk_condition(
                point.get("value"), point.get("percentile"), direction="low",
                absolute_threshold=-5.0, percentile_threshold=20.0,
                data_date=point.get("source_date"), data_source=point.get("data_source"),
                label=label, unit="%",
            ))
        stock_complete = all(item["matched"] is not None for item in stock_conditions)
        stock_count = sum(1 for item in stock_conditions if item["matched"])
        stock_block = stock_count >= 4 if stock_complete else None

        oil_point = aligned["oil_return_10d"].get(trade_date) or {}
        copper_point = aligned["copper_return_10d"].get(trade_date) or {}
        oil_conditions = [
            risk_condition(
                oil_point.get("value"), oil_point.get("percentile"), direction="low",
                absolute_threshold=-7.0, percentile_threshold=20.0,
                data_date=oil_point.get("source_date"), data_source=oil_point.get("data_source"),
                label="WTI/Brent平均10D", unit="%",
            ),
            risk_condition(
                copper_point.get("value"), copper_point.get("percentile"), direction="low",
                absolute_threshold=-5.0, percentile_threshold=20.0,
                data_date=copper_point.get("source_date"), data_source=copper_point.get("data_source"),
                label="COMEX铜10D", unit="%",
            ),
        ]
        oil_block, _oil_score = _status_from_conditions(oil_conditions)

        vix_point = aligned["vix_level"].get(trade_date) or {}
        vix_change = aligned["vix_change_5d"].get(trade_date) or {}
        vix_conditions = [
            risk_condition(
                vix_point.get("value"), None, direction="high", absolute_threshold=25.0,
                data_date=vix_point.get("source_date"), data_source=vix_point.get("data_source"),
                label="VIX收盘", unit="点",
            ),
            risk_condition(
                vix_change.get("value"), vix_change.get("percentile"), direction="high",
                absolute_threshold=5.0, percentile_threshold=80.0,
                data_date=vix_change.get("source_date"), data_source=vix_change.get("data_source"),
                label="VIX 5D增加", unit="点",
            ),
        ]
        vix_block, _vix_score = _status_from_conditions(vix_conditions)

        hy_point = aligned_by_available["hy_oas_level"].get(trade_date) or {}
        hy_change = aligned_by_available["hy_oas_change_5d"].get(trade_date) or {}
        hy_conditions = [
            risk_condition(
                hy_point.get("value"), None, direction="high", absolute_threshold=3.5,
                data_date=hy_point.get("source_date"), data_source=hy_point.get("data_source"),
                label="美国高收益债OAS", unit="%",
                available_at=hy_point.get("available_at"),
            ),
            risk_condition(
                hy_change.get("value"), hy_change.get("percentile"), direction="high",
                absolute_threshold=0.4, percentile_threshold=80.0,
                data_date=hy_change.get("source_date"), data_source=hy_change.get("data_source"),
                label="HY OAS 5D扩大", unit="百分点",
                available_at=hy_change.get("available_at"),
            ),
        ]
        hy_block, _hy_score = _status_from_conditions(hy_conditions)
        broad_blocks = [stock_block, oil_block, vix_block, hy_block]
        broad_complete = all(value is not None for value in broad_blocks)
        broad_count = sum(1 for value in broad_blocks if value)
        broad_active = broad_count >= 3 if broad_complete else None

        kospi_point = aligned["kospi_return_10d"].get(trade_date) or {}
        sox_point = aligned["sox_return_10d"].get(trade_date) or {}
        relative_point = aligned["tech_relative_return_10d"].get(trade_date) or {}
        tech_market_conditions = [
            risk_condition(
                kospi_point.get("value"), kospi_point.get("percentile"), direction="low",
                absolute_threshold=-5.0, percentile_threshold=10.0,
                data_date=kospi_point.get("source_date"), data_source=kospi_point.get("data_source"),
                label="KOSPI 10D", unit="%",
            ),
            risk_condition(
                sox_point.get("value"), sox_point.get("percentile"), direction="low",
                absolute_threshold=-8.0, percentile_threshold=10.0,
                data_date=sox_point.get("source_date"), data_source=sox_point.get("data_source"),
                label="SOX 10D", unit="%",
            ),
            risk_condition(
                relative_point.get("value"), relative_point.get("percentile"), direction="low",
                absolute_threshold=-3.0, percentile_threshold=10.0,
                data_date=relative_point.get("source_date"), data_source=relative_point.get("data_source"),
                label="IXN/ACWI相对收益10D", unit="%",
            ),
        ]
        tech_market_complete = all(item["matched"] is not None for item in tech_market_conditions)
        tech_market_count = sum(1 for item in tech_market_conditions if item["matched"])
        tech_complete = tech_market_complete
        tech_active = tech_market_count >= 2 if tech_complete else None

        nominal_point = aligned_by_available["nominal_10y_change_5d"].get(trade_date) or {}
        real_point = aligned_by_available["real_10y_change_5d"].get(trade_date) or {}
        nominal_level_point = aligned_by_available["nominal_10y_level"].get(trade_date) or {}
        real_level_point = aligned_by_available["real_10y_level"].get(trade_date) or {}
        sox_rate_point = aligned_by_available["sox_return_10d"].get(trade_date) or {}
        relative_rate_point = (
            aligned_by_available["tech_relative_return_10d"].get(trade_date) or {}
        )
        usd_rate_shock = build_usd_rate_shock_state(
            nominal_point,
            real_point,
            sox_rate_point,
            relative_rate_point,
            nominal_level_value=nominal_level_point.get("value"),
            real_level_value=real_level_point.get("value"),
        )
        rate_active = usd_rate_shock["active"]
        rate_score = usd_rate_shock["score"]
        rate_count = usd_rate_shock["matched_condition_count"]
        rate_complete = usd_rate_shock["complete"]

        if broad_active is True or tech_active is True or rate_active is True:
            global_active = True
        elif broad_active is False and tech_active is False and rate_active is False:
            global_active = False
        else:
            global_active = None
        active_modes = []
        if broad_active:
            active_modes.append("broad_risk_off")
        if tech_active:
            active_modes.append("tech_deleveraging")
        if rate_active:
            active_modes.append("usd_rate_shock")
        global_mode = "+".join(active_modes) if active_modes else None
        broad_score = broad_count / 4.0 * 100.0 if broad_complete else None
        tech_score = tech_market_count / 3.0 * 100.0 if tech_complete else None
        available_scores = [
            value for value in (broad_score, tech_score, rate_score) if value is not None
        ]
        global_score = max(available_scores) if available_scores else None

        payload = {
            "version": RISK_VERSION,
            "trade_date": trade_date,
            "yellow": {
                "complete": yellow_active is not None,
                "active": yellow_active,
                "score": yellow_score,
                "action": "降低高弹性仓位、停止追涨。",
                "components": yellow_conditions,
                "observations": {
                    "turnover_concentration": {
                        "label": "A股成交拥挤观察",
                        "components": concentration_observations,
                        "affects_strategy_state": False,
                    }
                },
                "dominant_im_contract": im_30.get("contract"),
            },
            "red": {
                "complete": red_active is not None,
                "active": red_active,
                "score": red_score,
                "action": "按大级别调整管理风险，不按普通回踩处理。",
                "components": red_conditions,
                "dominant_im_contract": im_14.get("contract"),
            },
            "global": {
                "complete": global_active is not None,
                "active": global_active,
                "score": global_score,
                "mode": global_mode,
                "broad_risk_off": {
                    "complete": broad_complete,
                    "active": broad_active,
                    "matched_module_count": broad_count,
                    "modules": {
                        "global_equities": {
                            "active": stock_block,
                            "matched_count": stock_count,
                            "components": stock_conditions,
                        },
                        "oil_and_copper": {"active": oil_block, "components": oil_conditions},
                        "vix": {"active": vix_block, "components": vix_conditions},
                        "hy_oas": {"active": hy_block, "components": hy_conditions},
                    },
                },
                "tech_deleveraging": {
                    "complete": tech_complete,
                    "active": tech_active,
                    "matched_market_count": tech_market_count,
                    "market_components": tech_market_conditions,
                },
                "usd_rate_shock": usd_rate_shock,
            },
        }
        results[trade_date] = {
            "risk_yellow_vulnerability": None if yellow_active is None else int(yellow_active),
            "risk_yellow_vulnerability_score": yellow_score,
            "risk_red_escalation": None if red_active is None else int(red_active),
            "risk_red_escalation_score": red_score,
            "risk_global_shock": None if global_active is None else int(global_active),
            "risk_global_shock_score": global_score,
            "risk_global_shock_mode": global_mode,
            "risk_strategy_components_json": payload,
        }
    return results


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
    margin_financing_net_buy_sum_map=None,
    self_sentiment_map=None,
    risk_strategy_map=None,
    turnover_concentration_map=None,
):
    rows = []
    option_pc_map = option_pc_map or {}
    option_flow_pc_map = option_flow_pc_map or {}
    exchange_option_pc_map = exchange_option_pc_map or {}
    option_vix_map = option_vix_map or {}
    cffex_net_short_delta_map = cffex_net_short_delta_map or {}
    fund_purchase_limit_map = fund_purchase_limit_map or {}
    margin_trading_map = margin_trading_map or {}
    margin_financing_net_buy_sum_map = margin_financing_net_buy_sum_map or {}
    self_sentiment_map = self_sentiment_map or {}
    risk_strategy_map = risk_strategy_map or {}
    turnover_concentration_map = turnover_concentration_map or {}
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
            margin_financing_net_buy_payload = (
                margin_financing_net_buy_sum_map.get(trade_date)
                or empty_margin_financing_net_buy_payload()
            )
            self_sentiment_payload = self_sentiment_map.get((trade_date, index_name)) or {}
            risk_payload = (
                risk_strategy_map.get(trade_date) or {}
                if index_name == RISK_TARGET_INDEX_NAME
                else {}
            )
            turnover_concentration_payload = (
                turnover_concentration_map.get(trade_date) or {}
                if index_name == "上证指数"
                else {}
            )
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
                "self_sentiment_score": self_sentiment_payload.get("self_sentiment_score"),
                "self_sentiment_core_score": self_sentiment_payload.get("self_sentiment_core_score"),
                "self_sentiment_derivative_score": self_sentiment_payload.get("self_sentiment_derivative_score"),
                "self_sentiment_components_json": self_sentiment_payload.get("self_sentiment_components_json") or {},
                "risk_yellow_vulnerability": risk_payload.get("risk_yellow_vulnerability"),
                "risk_yellow_vulnerability_score": risk_payload.get("risk_yellow_vulnerability_score"),
                "risk_red_escalation": risk_payload.get("risk_red_escalation"),
                "risk_red_escalation_score": risk_payload.get("risk_red_escalation_score"),
                "risk_global_shock": risk_payload.get("risk_global_shock"),
                "risk_global_shock_score": risk_payload.get("risk_global_shock_score"),
                "risk_global_shock_mode": risk_payload.get("risk_global_shock_mode"),
                "risk_strategy_components_json": risk_payload.get("risk_strategy_components_json") or {},
                "turnover_concentration_top5_pct": turnover_concentration_payload.get("top5_pct"),
                "turnover_concentration_top1_pct": turnover_concentration_payload.get("top1_pct"),
                "turnover_concentration_top1_raw_pct": turnover_concentration_payload.get("top1_raw_pct"),
                "turnover_concentration_meta_json": {
                    "stock_count": turnover_concentration_payload.get("stock_count"),
                    "top1_stock_count": turnover_concentration_payload.get("top1_stock_count"),
                    "total_turnover_amount": turnover_concentration_payload.get("total_turnover_amount"),
                    "top1_turnover_amount": turnover_concentration_payload.get("top1_turnover_amount"),
                    "top5_data_source": turnover_concentration_payload.get("top5_data_source"),
                    "top1_data_source": turnover_concentration_payload.get("top1_data_source"),
                    "top5_source_url": turnover_concentration_payload.get("top5_source_url"),
                    "source_date": normalize_date_text(turnover_concentration_payload.get("source_date")),
                    "available_at": str(turnover_concentration_payload.get("available_at") or "") or None,
                } if turnover_concentration_payload else {},
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
                "margin_total_market_cap_leverage_ratio_pct": margin_trading_payload.get(
                    "margin_total_market_cap_leverage_ratio_pct"
                ),
                **margin_financing_net_buy_payload,
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
    basis_delta_start_date = shift_date_text(
        start_date,
        -max(
            CFFEX_NET_SHORT_DELTA_LOOKBACK_DAYS,
            SELF_SENTIMENT_LOOKBACK_DAYS,
            RISK_LOOKBACK_CALENDAR_DAYS,
        ),
    )
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
        basis_delta_start_date,
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
        basis_delta_start_date,
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
            "margin_total_market_cap_leverage_ratio_pct": to_float(
                item.get("margin_total_market_cap_leverage_ratio_pct")
            ),
        }
        for item in margin_trading_rows
        if normalize_date_text(item.get("trade_date"))
    }

    option_pc_map = build_index_option_pc_map(option_rows, cn_index_close_map)
    risk_pc_history_rows = await db_tools.get_quant_index_dashboard_option_pc_history(
        RISK_TARGET_INDEX_NAME,
        basis_delta_start_date,
        end_date,
    )
    for history_row in risk_pc_history_rows:
        history_date = normalize_date_text(history_row.get("trade_date"))
        if history_date:
            option_pc_map.setdefault(
                (history_date, RISK_TARGET_INDEX_NAME),
                {
                    "option_pc_current_month": to_float(history_row.get("option_pc_current_month")),
                    "option_pc_next_month": to_float(history_row.get("option_pc_next_month")),
                    "option_pc_quarter_1": to_float(history_row.get("option_pc_quarter_1")),
                    "option_pc_quarter_2": to_float(history_row.get("option_pc_quarter_2")),
                },
            )
    option_flow_pc_map = build_index_option_flow_pc_map(option_rows)
    futures_close_map = build_futures_close_map(futures_rows)
    self_sentiment_history_rows = await db_tools.get_quant_index_dashboard_self_sentiment_history(
        basis_delta_start_date,
        end_date,
    )
    margin_financing_net_buy_sum_map = build_margin_financing_net_buy_sum_map(
        margin_trading_map,
        cn_basis_delta_trade_dates,
    )
    cffex_net_short_delta_map = build_index_cffex_net_short_delta_map(
        cffex_position_rows,
        start_date=basis_delta_start_date,
        end_date=end_date,
    )
    im_futures_rows = await db_tools.get_quant_index_risk_im_contract_rows(
        basis_delta_start_date,
        end_date,
    )
    global_asset_rows = await db_tools.get_global_risk_asset_daily_rows(
        basis_delta_start_date,
        end_date,
        RISK_GLOBAL_ASSET_CODES,
    )
    risk_us_index_rows = await db_tools.get_quant_index_dashboard_index_closes_for_market(
        ["标普500指数", "纳斯达克100指数"],
        "us",
        basis_delta_start_date,
        end_date,
    )
    risk_hk_index_rows = await db_tools.get_quant_index_dashboard_index_closes_for_market(
        ["恒生指数", "恒生科技指数"],
        "hk",
        basis_delta_start_date,
        end_date,
    )
    risk_vix_rows = await db_tools.get_quant_index_risk_us_vix_rows(
        basis_delta_start_date,
        end_date,
    )
    risk_credit_rows = await db_tools.get_quant_index_risk_us_credit_rows(
        basis_delta_start_date,
        end_date,
    )
    risk_treasury_rows = await db_tools.get_quant_index_risk_us_treasury_rows(
        basis_delta_start_date,
        end_date,
    )
    turnover_concentration_rows = await db_tools.get_a_share_turnover_concentration_daily_rows(
        basis_delta_start_date,
        end_date,
    )
    try:
        risk_cn_calendar_dates = await asyncio.to_thread(load_cn_trade_calendar_dates)
    except Exception as exc:
        risk_cn_calendar_dates = cn_basis_delta_trade_dates
        LOGGER.warning(
            "A股交易日历读取失败，最新风险日外盘条件将保持数据不完整: %s",
            exc,
        )
    risk_strategy_map = build_risk_strategy_map(
        trade_dates=cn_basis_delta_trade_dates,
        index_close_map=cn_index_close_map,
        option_pc_map=option_pc_map,
        cffex_net_short_delta_map=cffex_net_short_delta_map,
        margin_financing_net_buy_sum_map=margin_financing_net_buy_sum_map,
        im_futures_rows=im_futures_rows,
        global_asset_rows=global_asset_rows,
        us_index_rows=risk_us_index_rows,
        hk_index_rows=risk_hk_index_rows,
        us_vix_rows=risk_vix_rows,
        us_credit_rows=risk_credit_rows,
        us_treasury_rows=risk_treasury_rows,
        turnover_concentration_rows=turnover_concentration_rows,
        cn_calendar_dates=risk_cn_calendar_dates,
        output_start_date=start_date,
        output_end_date=end_date,
    )
    self_sentiment_map = build_self_sentiment_map(
        trade_dates=cn_basis_delta_trade_dates,
        index_close_map=cn_index_close_map,
        futures_close_map=futures_close_map,
        option_pc_map=option_pc_map,
        option_flow_pc_map=option_flow_pc_map,
        exchange_option_pc_map=exchange_option_pc_map,
        option_vix_map=option_vix_map,
        margin_financing_net_buy_sum_map=margin_financing_net_buy_sum_map,
        history_rows=self_sentiment_history_rows,
        output_start_date=start_date,
        output_end_date=end_date,
    )

    rows = build_dashboard_rows(
        trade_dates=cn_trade_dates,
        index_code_map=index_code_map,
        emotion_map=build_emotion_map(emotion_rows),
        index_close_map=cn_index_close_map,
        futures_close_map=futures_close_map,
        breadth_map=build_breadth_map(breadth_rows),
        option_pc_map=option_pc_map,
        option_flow_pc_map=option_flow_pc_map,
        exchange_option_pc_map=exchange_option_pc_map,
        option_vix_map=option_vix_map,
        cffex_net_short_delta_map=cffex_net_short_delta_map,
        basis_delta_trade_dates=cn_basis_delta_trade_dates,
        fund_purchase_limit_map=fund_purchase_limit_map,
        margin_trading_map=margin_trading_map,
        margin_financing_net_buy_sum_map=margin_financing_net_buy_sum_map,
        self_sentiment_map=self_sentiment_map,
        risk_strategy_map=risk_strategy_map,
        turnover_concentration_map={
            normalize_date_text(row.get("trade_date")): row
            for row in turnover_concentration_rows
            if normalize_date_text(row.get("trade_date"))
        },
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
