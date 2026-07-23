import asyncio
import io
import json
import math
import re
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.db.db_tool import DbTools


LOGGER = get_logger("exchange_option")
REQUEST_TIMEOUT_SECONDS = 30
SINA_BATCH_SIZE = 80
HISTORY_CONCURRENCY = 12
OFFICIAL_DATE_CONCURRENCY = 12
OFFICIAL_HISTORY_REQUEST_INTERVAL_SECONDS = 2.0

SSE_RISK_URL = "http://query.sse.com.cn/commonQuery.do"
SSE_STATS_URL = "http://query.sse.com.cn/commonQuery.do"
SSE_CURRENT_CONTRACT_URL = "http://query.sse.com.cn/commonQuery.do"
SSE_OFFICIAL_HISTORY_URL = "https://yunhq.sse.com.cn:32042/v1/sho/dayk"
SZSE_REPORT_URL = "http://www.szse.cn/api/report/ShowReport"
SZSE_STATS_URL = "http://investor.szse.cn/api/report/ShowReport/data"
SZSE_OFFICIAL_HISTORY_URL = "http://www.szse.cn/api/market/ssjjhq/getHistoryData"
SINA_QUOTE_URL = "https://hq.sinajs.cn/"
SINA_HISTORY_URL = (
    "https://stock.finance.sina.com.cn/futures/api/jsonp_v2.php//"
    "StockOptionDaylineService.getSymbolInfo"
)

EXCHANGE_OPTION_PRODUCTS = {
    "SSE": {
        "510050": "上证50ETF华夏",
        "510300": "沪深300ETF华泰柏瑞",
        "510500": "中证500ETF南方",
        "588000": "科创50ETF华夏",
        "588080": "科创50ETF易方达",
    },
    "SZSE": {
        "159901": "深证100ETF易方达",
        "159915": "创业板ETF易方达",
        "159919": "沪深300ETF嘉实",
        "159922": "中证500ETF嘉实",
    },
}
EXCHANGE_LISTED_DATES = {
    "SSE": date(2015, 2, 9),
    "SZSE": date(2019, 12, 23),
}

SSE_HEADERS = {
    "Referer": "http://www.sse.com.cn/",
    "User-Agent": "Mozilla/5.0",
}
SZSE_HEADERS = {
    "Referer": "http://www.szse.cn/option/",
    "User-Agent": "Mozilla/5.0",
}
SINA_HEADERS = {
    "Referer": "https://stock.finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0",
}

OFFICIAL_PROGRESS = ProgressStore("exchange_option_official")
KLINE_PROGRESS = ProgressStore("exchange_option_kline")
OFFICIAL_KLINE_PROGRESS = ProgressStore("exchange_option_official_kline_v1")
CONTRACT_INFO_PROGRESS = ProgressStore("exchange_option_contract_info")
STATS_PROGRESS = ProgressStore("exchange_option_stats")
CONTRACT_LISTING_CONCURRENCY = 16


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def direct_session():
    session = requests.Session()
    session.trust_env = False
    return session


def parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value}")


def parse_number(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "--", "---", "None", "nan"}:
        return None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def normalize_option_type(value):
    text = str(value or "").strip().upper()
    if text in {"C", "CALL", "认购"}:
        return "CALL"
    if text in {"P", "PUT", "认沽"}:
        return "PUT"
    return None


def parse_contract_trade_code(value):
    text = str(value or "").strip().upper()
    match = re.match(
        r"^(?P<underlying>\d{6})(?P<option_type>[CP])"
        r"(?P<contract_month>\d{4})[A-Z](?P<strike>\d{5,6})(?P<adjust>[A-Z]?)#?$",
        text,
    )
    if not match:
        return {
            "underlying_code": None,
            "option_type": None,
            "contract_month": None,
            "strike_price": None,
        }
    strike_text = match.group("strike")
    return {
        "underlying_code": match.group("underlying"),
        "option_type": "CALL" if match.group("option_type") == "C" else "PUT",
        "contract_month": match.group("contract_month"),
        "strike_price": int(strike_text) / 1000,
    }


def iter_weekdays(start_date, end_date):
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def build_contract_row(
    *,
    exchange,
    contract_code,
    contract_trade_code,
    contract_name,
    trade_date,
    risk_values=None,
    market_values=None,
    raw_payload=None,
):
    parsed = parse_contract_trade_code(contract_trade_code)
    risk_values = risk_values or {}
    market_values = market_values or {}
    underlying_code = parsed["underlying_code"]
    return {
        "exchange": exchange,
        "contract_code": str(contract_code or "").strip(),
        "contract_trade_code": str(contract_trade_code or "").strip() or None,
        "contract_name": str(contract_name or "").strip() or None,
        "underlying_code": underlying_code,
        "underlying_name": EXCHANGE_OPTION_PRODUCTS.get(exchange, {}).get(underlying_code),
        "option_type": parsed["option_type"],
        "contract_month": parsed["contract_month"],
        "strike_price": parsed["strike_price"],
        "trade_date": parse_date(trade_date).isoformat(),
        "open_price": parse_number(market_values.get("open_price")),
        "high_price": parse_number(market_values.get("high_price")),
        "low_price": parse_number(market_values.get("low_price")),
        "close_price": parse_number(market_values.get("close_price")),
        "pre_close_price": parse_number(market_values.get("pre_close_price")),
        "pre_settle_price": parse_number(market_values.get("pre_settle_price")),
        "pre_settle_source": (
            str(market_values.get("pre_settle_source") or "").strip() or None
        ),
        "settle_price": parse_number(market_values.get("settle_price")),
        "volume": parse_number(market_values.get("volume")),
        "turnover": parse_number(market_values.get("turnover")),
        "open_interest": parse_number(market_values.get("open_interest")),
        "delta_value": parse_number(risk_values.get("delta_value")),
        "theta_value": parse_number(risk_values.get("theta_value")),
        "gamma_value": parse_number(risk_values.get("gamma_value")),
        "vega_value": parse_number(risk_values.get("vega_value")),
        "rho_value": parse_number(risk_values.get("rho_value")),
        "implied_volatility": parse_number(risk_values.get("implied_volatility")),
        "data_source": "exchange_official+sina",
        "source_url": SSE_RISK_URL if exchange == "SSE" else SZSE_REPORT_URL,
        "raw_json": raw_payload,
    }


def build_contract_info_row(
    *,
    exchange,
    contract_code,
    contract_trade_code,
    contract_name,
    contract_unit=None,
    listed_date=None,
    last_trade_date=None,
    exercise_date=None,
    expire_date=None,
    delivery_date=None,
    listing_reason=None,
    raw_payload=None,
):
    parsed = parse_contract_trade_code(contract_trade_code)
    underlying_code = parsed["underlying_code"]
    return {
        "exchange": exchange,
        "contract_code": str(contract_code or "").strip(),
        "contract_trade_code": str(contract_trade_code or "").strip() or None,
        "contract_name": str(contract_name or "").strip() or None,
        "underlying_code": underlying_code,
        "underlying_name": EXCHANGE_OPTION_PRODUCTS.get(exchange, {}).get(underlying_code),
        "option_type": parsed["option_type"],
        "contract_month": parsed["contract_month"],
        "strike_price": parsed["strike_price"],
        "contract_unit": parse_number(contract_unit),
        "listed_date": parse_date(listed_date).isoformat() if listed_date else None,
        "last_trade_date": (
            parse_date(last_trade_date).isoformat() if last_trade_date else None
        ),
        "exercise_date": parse_date(exercise_date).isoformat() if exercise_date else None,
        "expire_date": parse_date(expire_date).isoformat() if expire_date else None,
        "delivery_date": parse_date(delivery_date).isoformat() if delivery_date else None,
        "listing_reason": str(listing_reason or "").strip() or None,
        "data_source": f"{exchange.lower()}_official_contract_listing",
        "source_url": SSE_CURRENT_CONTRACT_URL if exchange == "SSE" else SZSE_REPORT_URL,
        "raw_json": raw_payload,
    }


def contract_info_from_daily_row(row):
    raw = row.get("raw_json") if isinstance(row.get("raw_json"), dict) else {}
    exchange = row.get("exchange")
    if exchange == "SSE":
        return build_contract_info_row(
            exchange="SSE",
            contract_code=row.get("contract_code"),
            contract_trade_code=row.get("contract_trade_code"),
            contract_name=row.get("contract_name"),
            contract_unit=raw.get("CONTRACT_UNIT"),
            listed_date=raw.get("START_DATE"),
            last_trade_date=raw.get("END_DATE"),
            exercise_date=raw.get("EXERCISE_DATE"),
            expire_date=raw.get("EXPIRE_DATE"),
            delivery_date=raw.get("DELIVERY_DATE"),
            listing_reason="当日合约",
            raw_payload=raw,
        )
    return build_contract_info_row(
        exchange="SZSE",
        contract_code=row.get("contract_code"),
        contract_trade_code=row.get("contract_trade_code"),
        contract_name=row.get("contract_name"),
        contract_unit=raw.get("合约单位"),
        listed_date=None,
        last_trade_date=raw.get("最后交易日"),
        exercise_date=raw.get("行权日"),
        expire_date=raw.get("到期日"),
        delivery_date=raw.get("交收日"),
        listing_reason=raw.get("挂牌原因") or "当日合约",
        raw_payload=raw,
    )


def contract_is_active_on_target_date(contract_info, target_date):
    target = parse_date(target_date)
    listed_date = contract_info.get("listed_date")
    last_trade_date = contract_info.get("last_trade_date")
    if listed_date and target < parse_date(listed_date):
        return False
    if last_trade_date and target > parse_date(last_trade_date):
        return False
    return True


def fetch_sse_listing_rows_sync(target_date):
    target = parse_date(target_date)
    if target < EXCHANGE_LISTED_DATES["SSE"]:
        return []
    session = direct_session()
    response = session.get(
        SSE_CURRENT_CONTRACT_URL,
        params={
            "isPagination": "false",
            "sqlId": "SSE_ZQPZ_YSP_OPTZSXT_ADJUST_INFO_HYXG_SEARCH_L",
            "adjustDate": target.strftime("%Y%m%d"),
            "securityCode": "",
        },
        headers=SSE_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    rows = []
    for item in response.json().get("result") or []:
        parsed = parse_contract_trade_code(item.get("CONTRACT_ID"))
        if parsed["underlying_code"] not in EXCHANGE_OPTION_PRODUCTS["SSE"]:
            continue
        rows.append(
            build_contract_info_row(
                exchange="SSE",
                contract_code=item.get("SECURITY_ID"),
                contract_trade_code=item.get("CONTRACT_ID"),
                contract_name=item.get("CONTRACT_SYMBOL"),
                contract_unit=item.get("CONTRACT_UNIT"),
                listed_date=item.get("START_DATE") or item.get("ADJUST_DATE"),
                last_trade_date=item.get("END_DATE"),
                exercise_date=item.get("EXERCISE_DATE"),
                expire_date=item.get("EXPIRE_DATE"),
                delivery_date=item.get("DELIVERY_DATE"),
                listing_reason=item.get("UPDATE_TYPE"),
                raw_payload=item,
            )
        )
    return rows


def fetch_szse_all_contract_info_sync():
    response = None
    last_error = None
    for attempt in range(3):
        try:
            session = direct_session()
            response = session.get(
                SZSE_REPORT_URL,
                params={
                    "SHOWTYPE": "xlsx",
                    "CATALOGID": "option_hybg",
                    "TABKEY": "tab1",
                },
                headers=SZSE_HEADERS,
                timeout=180,
            )
            response.raise_for_status()
            break
        except Exception as exc:
            last_error = exc
            response = None
            if attempt < 2:
                time.sleep(attempt + 1)
    if response is None:
        raise RuntimeError(
            f"SZSE full contract listing unavailable after 3 attempts: {last_error}"
        )
    frame = pd.read_excel(io.BytesIO(response.content))
    rows = []
    for item in frame.to_dict("records"):
        parsed = parse_contract_trade_code(item.get("合约代码"))
        if parsed["underlying_code"] not in EXCHANGE_OPTION_PRODUCTS["SZSE"]:
            continue
        rows.append(
            build_contract_info_row(
                exchange="SZSE",
                contract_code=item.get("合约编码"),
                contract_trade_code=item.get("合约代码"),
                contract_name=item.get("合约简称"),
                contract_unit=item.get("合约单位"),
                listed_date=item.get("挂牌日期"),
                last_trade_date=item.get("最后交易日"),
                exercise_date=item.get("行权日"),
                expire_date=item.get("到期日"),
                delivery_date=item.get("交收日"),
                listing_reason=item.get("挂牌原因"),
                raw_payload=item,
            )
        )
    return rows


def fetch_sse_risk_rows_sync(target_date):
    target = parse_date(target_date)
    if target < EXCHANGE_LISTED_DATES["SSE"]:
        return []
    session = direct_session()
    response = session.get(
        SSE_RISK_URL,
        params={
            "isPagination": "false",
            "trade_date": target.strftime("%Y%m%d"),
            "sqlId": "SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L",
            "contractSymbol": "",
        },
        headers=SSE_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    result = response.json().get("result") or []
    rows = []
    for item in result:
        contract_trade_code = item.get("CONTRACT_ID")
        parsed = parse_contract_trade_code(contract_trade_code)
        if parsed["underlying_code"] not in EXCHANGE_OPTION_PRODUCTS["SSE"]:
            continue
        rows.append(
            build_contract_row(
                exchange="SSE",
                contract_code=item.get("SECURITY_ID"),
                contract_trade_code=contract_trade_code,
                contract_name=item.get("CONTRACT_SYMBOL"),
                trade_date=item.get("TRADE_DATE") or target,
                risk_values={
                    "delta_value": item.get("DELTA_VALUE"),
                    "theta_value": item.get("THETA_VALUE"),
                    "gamma_value": item.get("GAMMA_VALUE"),
                    "vega_value": item.get("VEGA_VALUE"),
                    "rho_value": item.get("RHO_VALUE"),
                    "implied_volatility": item.get("IMPLC_VOLATLTY"),
                },
                raw_payload=item,
            )
        )
    return rows


def fetch_sse_current_contract_rows_sync(target_date):
    target = parse_date(target_date)
    session = direct_session()
    response = session.get(
        SSE_CURRENT_CONTRACT_URL,
        params={
            "isPagination": "false",
            "expireDate": "",
            "securityId": "",
            "sqlId": "SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L",
        },
        headers=SSE_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    rows = []
    for item in response.json().get("result") or []:
        contract_trade_code = item.get("CONTRACT_ID")
        parsed = parse_contract_trade_code(contract_trade_code)
        if parsed["underlying_code"] not in EXCHANGE_OPTION_PRODUCTS["SSE"]:
            continue
        rows.append(
            build_contract_row(
                exchange="SSE",
                contract_code=item.get("SECURITY_ID"),
                contract_trade_code=contract_trade_code,
                contract_name=item.get("CONTRACT_SYMBOL"),
                trade_date=target,
                market_values={
                    "pre_settle_price": item.get("SETTL_PRICE"),
                    "pre_settle_source": "sse_official_contract",
                },
                raw_payload=item,
            )
        )
    return rows


def fetch_szse_risk_rows_sync(target_date):
    target = parse_date(target_date)
    if target < EXCHANGE_LISTED_DATES["SZSE"]:
        return []
    session = direct_session()
    response = session.get(
        SZSE_REPORT_URL,
        params={
            "SHOWTYPE": "xlsx",
            "CATALOGID": "option_hyfxzb",
            "TABKEY": "tab1",
            "txtSearchDate": target.isoformat(),
        },
        headers=SZSE_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    frame = pd.read_excel(io.BytesIO(response.content))
    rows = []
    for item in frame.to_dict("records"):
        contract_trade_code = item.get("合约代码")
        parsed = parse_contract_trade_code(contract_trade_code)
        if parsed["underlying_code"] not in EXCHANGE_OPTION_PRODUCTS["SZSE"]:
            continue
        rows.append(
            build_contract_row(
                exchange="SZSE",
                contract_code=item.get("合约编码"),
                contract_trade_code=contract_trade_code,
                contract_name=item.get("合约简称"),
                trade_date=target,
                risk_values={
                    "delta_value": item.get("Delta"),
                    "theta_value": item.get("Theta"),
                    "gamma_value": item.get("Gamma"),
                    "vega_value": item.get("Vega"),
                    "rho_value": item.get("Rho"),
                },
                raw_payload=item,
            )
        )
    return rows


def fetch_szse_current_contract_rows_sync(target_date):
    target = parse_date(target_date)
    session = direct_session()
    response = session.get(
        SZSE_REPORT_URL,
        params={
            "SHOWTYPE": "xlsx",
            "CATALOGID": "option_drhy",
            "TABKEY": "tab1",
        },
        headers=SZSE_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    frame = pd.read_excel(io.BytesIO(response.content))
    rows = []
    for item in frame.to_dict("records"):
        contract_trade_code = item.get("合约代码")
        parsed = parse_contract_trade_code(contract_trade_code)
        if parsed["underlying_code"] not in EXCHANGE_OPTION_PRODUCTS["SZSE"]:
            continue
        rows.append(
            build_contract_row(
                exchange="SZSE",
                contract_code=item.get("合约编码"),
                contract_trade_code=contract_trade_code,
                contract_name=item.get("合约简称"),
                trade_date=target,
                market_values={
                    "pre_settle_price": item.get("前结算价"),
                    "pre_settle_source": "szse_official_contract",
                    "open_interest": item.get("合约总持仓"),
                },
                raw_payload=item,
            )
        )
    return rows


def _sse_stats_row(item, target_date):
    underlying_code = str(item.get("SECURITY_CODE") or "").strip()
    if underlying_code not in EXCHANGE_OPTION_PRODUCTS["SSE"]:
        return None
    total_money = parse_number(item.get("TOTAL_MONEY"))
    return {
        "exchange": "SSE",
        "underlying_code": underlying_code,
        "underlying_name": str(item.get("SECURITY_ABBR") or "").strip()
        or EXCHANGE_OPTION_PRODUCTS["SSE"][underlying_code],
        "trade_date": parse_date(item.get("TRADE_DATE") or target_date).isoformat(),
        "contract_count": parse_number(item.get("CONTRACT_VOLUME")),
        "turnover_amount": total_money * 10000 if total_money is not None else None,
        "total_volume": parse_number(item.get("TOTAL_VOLUME")),
        "call_volume": parse_number(item.get("CALL_VOLUME")),
        "put_volume": parse_number(item.get("PUT_VOLUME")),
        "put_call_volume_ratio": (
            parse_number(item.get("CP_RATE")) / 100
            if parse_number(item.get("CP_RATE")) is not None
            else None
        ),
        "open_interest": parse_number(item.get("LEAVES_QTY")),
        "call_open_interest": parse_number(item.get("LEAVES_CALL_QTY")),
        "put_open_interest": parse_number(item.get("LEAVES_PUT_QTY")),
        "data_source": "sse_official",
        "source_url": SSE_STATS_URL,
        "raw_json": item,
    }


def fetch_sse_stats_rows_sync(target_date):
    target = parse_date(target_date)
    if target < EXCHANGE_LISTED_DATES["SSE"]:
        return []
    session = direct_session()
    response = session.get(
        SSE_STATS_URL,
        params={
            "isPagination": "false",
            "sqlId": "COMMON_SSE_ZQPZ_YSP_QQ_SJTJ_MRTJ_CX",
            "tradeDate": target.strftime("%Y%m%d"),
        },
        headers=SSE_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return [
        row
        for row in (_sse_stats_row(item, target) for item in response.json().get("result") or [])
        if row
    ]


def _szse_stats_row(item, target_date):
    underlying_code = str(item.get("bddm") or "").strip()
    if underlying_code not in EXCHANGE_OPTION_PRODUCTS["SZSE"]:
        return None
    call_volume = parse_number(item.get("rccjl"))
    put_volume = parse_number(item.get("rpcjl"))
    return {
        "exchange": "SZSE",
        "underlying_code": underlying_code,
        "underlying_name": str(item.get("bdmc") or "").strip()
        or EXCHANGE_OPTION_PRODUCTS["SZSE"][underlying_code],
        "trade_date": parse_date(target_date).isoformat(),
        "contract_count": None,
        "turnover_amount": None,
        "total_volume": parse_number(item.get("cjl")),
        "call_volume": call_volume,
        "put_volume": put_volume,
        "put_call_volume_ratio": (
            put_volume / call_volume
            if put_volume is not None and call_volume not in (None, 0)
            else None
        ),
        "open_interest": parse_number(item.get("wpchyzs")),
        "call_open_interest": parse_number(item.get("wpcrchys")),
        "put_open_interest": parse_number(item.get("wpcrphys")),
        "data_source": "szse_official",
        "source_url": SZSE_STATS_URL,
        "raw_json": item,
    }


def fetch_szse_stats_rows_sync(target_date):
    target = parse_date(target_date)
    if target < EXCHANGE_LISTED_DATES["SZSE"]:
        return []
    last_error = None
    payload = None
    for attempt in range(4):
        try:
            session = direct_session()
            response = session.get(
                SZSE_STATS_URL,
                params={
                    "SHOWTYPE": "JSON",
                    "CATALOGID": "ysprdzb",
                    "TABKEY": "tab1",
                    "txtQueryDate": target.isoformat(),
                    "random": "0.0652692406565949",
                },
                headers=SZSE_HEADERS,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            break
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 ** attempt)
    if payload is None:
        raise RuntimeError(
            f"SZSE stats unavailable after 4 attempts: {last_error}"
        )
    items = payload[0].get("data") if payload else []
    return [
        row
        for row in (_szse_stats_row(item, target) for item in items or [])
        if row
    ]


def parse_sina_quote_payload(text):
    rows = {}
    for match in re.finditer(
        r'var hq_str_CON_OP_(?P<code>\d+)="(?P<body>[^"]*)";',
        text,
    ):
        values = match.group("body").split(",")
        if len(values) < 43:
            continue
        quote_time = str(values[32] or "").strip()
        rows[match.group("code")] = {
            "bid1_volume": values[0],
            "bid1_price": values[1],
            "ask1_price": values[3],
            "ask1_volume": values[4],
            "open_interest": values[5],
            "strike_price": values[7],
            "pre_settle_price": values[8],
            "pre_settle_source": "sina_realtime",
            "open_price": values[9],
            "close_price": values[2],
            "high_price": values[39],
            "low_price": values[40],
            "volume": values[41],
            "turnover": values[42],
            "quote_time": quote_time,
            "trade_date": quote_time[:10] if len(quote_time) >= 10 else None,
        }
    return rows


def fetch_sina_quotes_sync(contract_codes):
    codes = [str(code).strip() for code in contract_codes if str(code).strip()]
    if not codes:
        return {}
    session = direct_session()
    result = {}
    for offset in range(0, len(codes), SINA_BATCH_SIZE):
        batch = codes[offset : offset + SINA_BATCH_SIZE]
        symbols = ",".join(f"CON_OP_{code}" for code in batch)
        response = session.get(
            f"{SINA_QUOTE_URL}?list={symbols}",
            headers=SINA_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        result.update(parse_sina_quote_payload(response.text))
    return result


def merge_market_values(rows, quotes, target_date):
    target = parse_date(target_date).isoformat()
    merged = []
    for row in rows:
        item = dict(row)
        quote = quotes.get(item["contract_code"]) or {}
        if quote.get("trade_date") == target:
            for field in (
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "pre_settle_price",
                "volume",
                "turnover",
                "open_interest",
            ):
                item[field] = parse_number(quote.get(field))
            if quote.get("pre_settle_source"):
                item["pre_settle_source"] = quote["pre_settle_source"]
        merged.append(item)
    return merged


def parse_sina_history_payload(text, exchange, contract_code):
    start = text.find("(")
    end = text.rfind(")")
    if start < 0 or end <= start:
        return []
    try:
        payload = json.loads(text[start + 1 : end])
    except json.JSONDecodeError:
        return []
    rows = []
    for item in payload or []:
        if not isinstance(item, dict):
            continue
        trade_date = item.get("d")
        if not trade_date:
            continue
        rows.append(
            {
                "exchange": exchange,
                "contract_code": str(contract_code),
                "trade_date": parse_date(trade_date).isoformat(),
                "open_price": parse_number(item.get("o")),
                "high_price": parse_number(item.get("h")),
                "low_price": parse_number(item.get("l")),
                "close_price": parse_number(item.get("c")),
                "volume": parse_number(item.get("v")),
                "data_source": "exchange_official+sina_history",
                "source_url": SINA_HISTORY_URL,
            }
        )
    return rows


def filter_history_rows_to_trade_dates(rows, trade_dates):
    valid_dates = {parse_date(item) for item in trade_dates}
    filtered = []
    for row in rows:
        try:
            trade_date = parse_date(row.get("trade_date"))
        except (TypeError, ValueError):
            continue
        if trade_date in valid_dates:
            filtered.append(row)
    return filtered


def fetch_sina_history_sync(exchange, contract_code):
    last_error = None
    for attempt in range(3):
        try:
            session = direct_session()
            response = session.get(
                SINA_HISTORY_URL,
                params={"symbol": f"CON_OP_{contract_code}"},
                headers=SINA_HEADERS,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            response_text = response.text or ""
            rows = parse_sina_history_payload(response_text, exchange, contract_code)
            if rows:
                return rows
            if re.search(r"\(\s*null\s*\)\s*;?\s*$", response_text):
                LOGGER.info(
                    "option kline source has no history [%s:%s]",
                    exchange,
                    contract_code,
                )
                return []
            last_error = RuntimeError("empty history response")
        except Exception as exc:
            last_error = exc
        if attempt < 2:
            time.sleep(attempt + 1)
    raise RuntimeError(
        f"{exchange}:{contract_code} history unavailable after 3 attempts: {last_error}"
    )


def parse_sse_official_history_payload(payload, contract_code):
    rows = []
    for item in (payload or {}).get("kline") or []:
        if not isinstance(item, (list, tuple)) or len(item) < 7:
            continue
        try:
            trade_date = parse_date(item[0]).isoformat()
        except (TypeError, ValueError):
            continue
        rows.append(
            {
                "exchange": "SSE",
                "contract_code": str(contract_code),
                "trade_date": trade_date,
                "open_price": parse_number(item[1]),
                "high_price": parse_number(item[2]),
                "low_price": parse_number(item[3]),
                "close_price": parse_number(item[4]),
                "volume": parse_number(item[5]),
                "turnover": parse_number(item[6]),
                "data_source": "sse_official_dayk",
                "source_url": f"{SSE_OFFICIAL_HISTORY_URL}/{contract_code}",
                "raw_json": item,
            }
        )
    return rows


def parse_szse_official_history_payload(payload, contract_code):
    rows = []
    data = (payload or {}).get("data") or {}
    for item in data.get("picupdata") or []:
        if not isinstance(item, (list, tuple)) or len(item) < 9:
            continue
        try:
            trade_date = parse_date(item[0]).isoformat()
        except (TypeError, ValueError):
            continue
        close_price = parse_number(item[2])
        price_change = parse_number(item[5])
        pre_settle_price = (
            close_price - price_change
            if close_price is not None and price_change is not None
            else None
        )
        rows.append(
            {
                "exchange": "SZSE",
                "contract_code": str(contract_code),
                "trade_date": trade_date,
                "open_price": parse_number(item[1]),
                "close_price": close_price,
                "low_price": parse_number(item[3]),
                "high_price": parse_number(item[4]),
                "pre_settle_price": pre_settle_price,
                "pre_settle_source": (
                    "szse_official_dayk_change"
                    if pre_settle_price is not None
                    else None
                ),
                "volume": parse_number(item[7]),
                "turnover": parse_number(item[8]),
                "data_source": "szse_official_dayk",
                "source_url": SZSE_OFFICIAL_HISTORY_URL,
                "raw_json": item,
            }
        )
    return rows


def fetch_official_history_sync(exchange, contract_code):
    normalized_exchange = str(exchange or "").strip().upper()
    last_error = None
    for attempt in range(3):
        try:
            session = direct_session()
            if normalized_exchange == "SSE":
                response = session.get(
                    f"{SSE_OFFICIAL_HISTORY_URL}/{contract_code}",
                    params={
                        "begin": -10000,
                        "end": -1,
                        "period": "day",
                        "select": "date,open,high,low,close,volume,amount",
                    },
                    headers=SSE_HEADERS,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                if response.status_code == 404:
                    LOGGER.info(
                        "SSE official option history has no K-line [%s]",
                        contract_code,
                    )
                    return []
                response.raise_for_status()
                return parse_sse_official_history_payload(
                    response.json(),
                    contract_code,
                )
            if normalized_exchange == "SZSE":
                response = session.get(
                    SZSE_OFFICIAL_HISTORY_URL,
                    params={
                        "cycleType": 32,
                        "marketId": 70,
                        "moduleType": "realoption",
                        "code": str(contract_code),
                    },
                    headers=SZSE_HEADERS,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                payload = response.json()
                if str(payload.get("code")) not in {"0", "-1"}:
                    raise RuntimeError(payload.get("message") or "unexpected SZSE response")
                return parse_szse_official_history_payload(payload, contract_code)
            raise ValueError(f"unsupported exchange: {exchange}")
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(attempt + 1)
    raise RuntimeError(
        f"{normalized_exchange}:{contract_code} official history unavailable "
        f"after 3 attempts: {last_error}"
    )


async def fetch_official_target_rows(contract_rows, target_date):
    target_text = parse_date(target_date).isoformat()
    collected = {
        exchange: [
            row
            for row in contract_rows or []
            if str(row.get("exchange") or "").strip().upper() == exchange
            and row.get("official_complete")
        ]
        for exchange in EXCHANGE_OPTION_PRODUCTS
    }
    rows_by_exchange = {
        exchange: [
            row
            for row in contract_rows or []
            if str(row.get("exchange") or "").strip().upper() == exchange
            and not row.get("official_complete")
        ]
        for exchange in EXCHANGE_OPTION_PRODUCTS
    }
    failures = {"SSE": [], "SZSE": []}

    async def process_exchange(exchange):
        for index, contract_row in enumerate(rows_by_exchange[exchange], start=1):
            started_at = time.monotonic()
            contract_code = str(contract_row.get("contract_code") or "").strip()
            try:
                history_rows = await asyncio.to_thread(
                    fetch_official_history_sync,
                    exchange,
                    contract_code,
                )
                target_rows = [
                    row
                    for row in history_rows
                    if row.get("trade_date") == target_text
                ]
                for row in target_rows:
                    for field in (
                        "contract_trade_code",
                        "contract_name",
                        "underlying_code",
                        "underlying_name",
                        "option_type",
                        "contract_month",
                        "strike_price",
                        "pre_settle_price",
                        "pre_settle_source",
                    ):
                        row[field] = contract_row.get(field)
                collected[exchange].extend(target_rows)
            except Exception as exc:
                failures[exchange].append(f"{contract_code}:{exc}")
            elapsed = time.monotonic() - started_at
            if elapsed < OFFICIAL_HISTORY_REQUEST_INTERVAL_SECONDS:
                await asyncio.sleep(
                    OFFICIAL_HISTORY_REQUEST_INTERVAL_SECONDS - elapsed
                )
            if index % 50 == 0:
                print(
                    f"exchange option {exchange} daily official kline: "
                    f"contracts={index}/{len(rows_by_exchange[exchange])}, "
                    f"target_rows={len(collected[exchange])}"
                )

    await asyncio.gather(
        process_exchange("SSE"),
        process_exchange("SZSE"),
    )
    return collected, failures


async def fetch_official_rows(target_date):
    sse_contracts, szse_contracts, sse_stats, szse_stats = await asyncio.gather(
        asyncio.to_thread(fetch_sse_risk_rows_sync, target_date),
        asyncio.to_thread(fetch_szse_risk_rows_sync, target_date),
        asyncio.to_thread(fetch_sse_stats_rows_sync, target_date),
        asyncio.to_thread(fetch_szse_stats_rows_sync, target_date),
    )
    return sse_contracts + szse_contracts, sse_stats + szse_stats


async def fetch_current_contract_rows(target_date):
    sse_contracts, szse_contracts = await asyncio.gather(
        asyncio.to_thread(fetch_sse_current_contract_rows_sync, target_date),
        asyncio.to_thread(fetch_szse_current_contract_rows_sync, target_date),
    )
    return sse_contracts + szse_contracts


def validate_product_coverage(rows, target_date):
    coverage = {
        exchange: {
            str(row.get("underlying_code") or "")
            for row in rows
            if row.get("exchange") == exchange
        }
        for exchange in EXCHANGE_OPTION_PRODUCTS
    }
    failures = []
    for exchange, products in EXCHANGE_OPTION_PRODUCTS.items():
        missing = sorted(set(products) - coverage[exchange])
        if missing:
            failures.append(f"{exchange}缺少{','.join(missing)}")
    if failures:
        raise RuntimeError(
            f"{parse_date(target_date).isoformat()}期权产品覆盖不完整：" + "；".join(failures)
        )
    return coverage


async def sync_stats_daily(target_date=None):
    target = parse_date(target_date or date.today())
    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_exchange_option_tables()
        source_results = await asyncio.gather(
            asyncio.to_thread(fetch_sse_stats_rows_sync, target),
            asyncio.to_thread(fetch_szse_stats_rows_sync, target),
            return_exceptions=True,
        )
        failures = []
        stats_rows = []
        for source_name, result in zip(("SSE统计", "SZSE统计"), source_results):
            if isinstance(result, Exception):
                failures.append(f"{source_name}：{result}")
            else:
                stats_rows.extend(result)

        inserted_stats = await db.batch_exchange_option_daily_stats(stats_rows)
        try:
            coverage = validate_product_coverage(stats_rows, target)
        except RuntimeError as exc:
            failures.append(str(exc))
            coverage = {exchange: set() for exchange in EXCHANGE_OPTION_PRODUCTS}

        if failures:
            raise RuntimeError(
                f"{target.isoformat()}沪深期权官方统计未完整发布；"
                f"已保留统计{inserted_stats}行；" + "；".join(failures)
            )
        return {
            "status": "SUCCESS",
            "target_date": target.isoformat(),
            "stats_rows": inserted_stats,
            "sse_products": len(coverage["SSE"]),
            "szse_products": len(coverage["SZSE"]),
        }
    finally:
        await db.close()


async def sync_daily(target_date=None):
    target = parse_date(target_date or date.today())
    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_exchange_option_tables()
        source_results = await asyncio.gather(
            asyncio.to_thread(fetch_sse_current_contract_rows_sync, target),
            asyncio.to_thread(fetch_szse_current_contract_rows_sync, target),
            asyncio.to_thread(fetch_sse_stats_rows_sync, target),
            asyncio.to_thread(fetch_szse_stats_rows_sync, target),
            return_exceptions=True,
        )
        source_names = (
            "SSE合约",
            "SZSE合约",
            "SSE统计",
            "SZSE统计",
        )
        failures = []
        warnings = []
        normalized_results = []
        for source_name, result in zip(source_names, source_results):
            if isinstance(result, Exception):
                message = f"{source_name}：{result}"
                if source_name.endswith("统计"):
                    warnings.append(message)
                else:
                    failures.append(message)
                normalized_results.append([])
            else:
                normalized_results.append(result)

        sse_contracts, szse_contracts, sse_stats, szse_stats = normalized_results
        contract_rows = sse_contracts + szse_contracts
        stats_rows = sse_stats + szse_stats
        contract_info_rows = [contract_info_from_daily_row(row) for row in contract_rows]
        inserted_contract_info = await db.batch_exchange_option_contract_info(
            contract_info_rows
        )
        if hasattr(db, "list_exchange_option_active_contract_rows"):
            contract_rows = await db.list_exchange_option_active_contract_rows(target)
        else:
            contract_rows = [
                row
                for row, contract_info in zip(contract_rows, contract_info_rows)
                if contract_is_active_on_target_date(contract_info, target)
            ]
        sse_contracts = [
            row for row in contract_rows if row.get("exchange") == "SSE"
        ]
        szse_contracts = [
            row for row in contract_rows if row.get("exchange") == "SZSE"
        ]
        quotes = await asyncio.to_thread(
            fetch_sina_quotes_sync,
            [
                row["contract_code"]
                for row in contract_rows
                if not row.get("official_complete")
            ],
        )
        contract_rows = merge_market_values(contract_rows, quotes, target)
        inserted_contracts = await db.batch_exchange_option_contract_daily_data(contract_rows)
        official_rows_by_exchange, official_failures = await fetch_official_target_rows(
            contract_rows,
            target,
        )
        official_rows = [
            row
            for exchange_rows in official_rows_by_exchange.values()
            for row in exchange_rows
        ]
        inserted_official_contracts = (
            await db.batch_exchange_option_contract_daily_data(official_rows)
        )
        inserted_stats = await db.batch_exchange_option_daily_stats(stats_rows)
        try:
            validate_product_coverage(contract_rows, target)
        except RuntimeError as exc:
            failures.append(str(exc))
        stats_complete = True
        try:
            validate_product_coverage(stats_rows, target)
        except RuntimeError as exc:
            stats_complete = False
            warnings.append(str(exc))
        official_coverage = {
            exchange: {
                str(row.get("contract_code") or "").strip()
                for row in official_rows_by_exchange[exchange]
                if row.get("close_price") is not None
                and row.get("volume") is not None
                and row.get("turnover") is not None
            }
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }
        quoted_count = sum(
            1
            for row in official_rows
            if row.get("close_price") is not None and row.get("trade_date") == target.isoformat()
        )
        if quoted_count == 0:
            failures.append(f"{target.isoformat()}未取得任何沪深期权收盘行情")
        for exchange, expected_rows in (
            ("SSE", sse_contracts),
            ("SZSE", szse_contracts),
        ):
            expected_codes = {
                str(row.get("contract_code") or "").strip()
                for row in expected_rows
                if str(row.get("contract_code") or "").strip()
            }
            missing_codes = sorted(expected_codes - official_coverage[exchange])
            if missing_codes:
                failures.append(
                    f"{exchange}官方日K量额缺失{len(missing_codes)}份"
                    f"（示例：{','.join(missing_codes[:5])}）"
                )
            if official_failures[exchange]:
                failures.append(
                    f"{exchange}官方日K请求失败{len(official_failures[exchange])}份"
                    f"（示例：{'；'.join(official_failures[exchange][:3])}）"
                )
        if failures:
            raise RuntimeError(
                f"{target.isoformat()}沪深期权日更未完整成功；"
                f"已保留合约主表{inserted_contract_info}行、"
                f"合约日线{inserted_contracts}行、官方覆盖{inserted_official_contracts}行、"
                f"统计{inserted_stats}行；"
                + "；".join(failures)
            )
        return {
            "status": "SUCCESS",
            "target_date": target.isoformat(),
            "contract_info_rows": inserted_contract_info,
            "contract_rows": inserted_contracts,
            "official_contract_rows": inserted_official_contracts,
            "stats_rows": inserted_stats,
            "quoted_contracts": quoted_count,
            "official_coverage": {
                exchange: len(codes)
                for exchange, codes in official_coverage.items()
            },
            "stats_status": "complete" if stats_complete else "source_pending",
            "warnings": warnings,
            "sse_products": len(EXCHANGE_OPTION_PRODUCTS["SSE"]),
            "szse_products": len(EXCHANGE_OPTION_PRODUCTS["SZSE"]),
        }
    finally:
        await db.close()


async def backfill_contract_info(start_date=None, end_date=None):
    start = parse_date(start_date or EXCHANGE_LISTED_DATES["SSE"])
    end = parse_date(end_date or date.today())
    db = DbTools()
    await db.init_pool()
    inserted_sse = 0
    inserted_szse = 0
    processed_dates = 0
    completed = CONTRACT_INFO_PROGRESS.load()
    try:
        await db.ensure_exchange_option_tables()
        existing_szse = await db.count_exchange_option_contract_info("SZSE")
        if existing_szse >= 7776:
            inserted_szse = existing_szse
        else:
            szse_rows = await asyncio.to_thread(fetch_szse_all_contract_info_sync)
            inserted_szse = await db.batch_exchange_option_contract_info(szse_rows)

        trade_dates = await db.list_cn_trade_dates(start, end)
        if not trade_dates:
            trade_dates = list(iter_weekdays(start, end))
        pending_dates = [
            target
            for target in trade_dates
            if f"SSE:{target.isoformat()}" not in completed
        ]

        async def fetch_one(target):
            try:
                rows = await asyncio.to_thread(fetch_sse_listing_rows_sync, target)
                return target, rows, None
            except Exception as exc:
                return target, [], exc

        for offset in range(0, len(pending_dates), CONTRACT_LISTING_CONCURRENCY):
            batch_dates = pending_dates[
                offset : offset + CONTRACT_LISTING_CONCURRENCY
            ]
            results = await asyncio.gather(*(fetch_one(target) for target in batch_dates))
            for target, rows, error in results:
                progress_key = f"SSE:{target.isoformat()}"
                if error is not None:
                    LOGGER.error(
                        "SSE option listing backfill failed [%s]: %s",
                        target,
                        error,
                    )
                    continue
                inserted_sse += await db.batch_exchange_option_contract_info(rows)
                CONTRACT_INFO_PROGRESS.append(progress_key)
                processed_dates += 1
            if processed_dates and processed_dates % 100 < CONTRACT_LISTING_CONCURRENCY:
                print(
                    f"exchange option contract info: dates={processed_dates}/"
                    f"{len(pending_dates)}, SSE={inserted_sse}, SZSE={inserted_szse}"
                )
        return {
            "status": "SUCCESS",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "processed_sse_dates": processed_dates,
            "sse_contracts": inserted_sse,
            "szse_contracts": inserted_szse,
        }
    finally:
        await db.close()


async def backfill_stats(start_date=None, end_date=None):
    start = parse_date(start_date or EXCHANGE_LISTED_DATES["SSE"])
    end = parse_date(end_date or date.today())
    db = DbTools()
    await db.init_pool()
    inserted = {"SSE": 0, "SZSE": 0}
    processed = {"SSE": 0, "SZSE": 0}
    failed = {"SSE": 0, "SZSE": 0}
    try:
        await db.ensure_exchange_option_tables()
        trade_dates = await db.list_cn_trade_dates(start, end)
        if not trade_dates:
            trade_dates = list(iter_weekdays(start, end))
        existing_dates = {
            exchange: await db.list_exchange_option_stats_dates(exchange, start, end)
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }
        pending = {
            exchange: [
                target
                for target in trade_dates
                if target >= EXCHANGE_LISTED_DATES[exchange]
                and target not in existing_dates[exchange]
            ]
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }

        async def fetch_sse_one(target):
            try:
                rows = await asyncio.to_thread(fetch_sse_stats_rows_sync, target)
                if not rows:
                    raise RuntimeError("empty SSE stats response")
                return target, rows, None
            except Exception as exc:
                return target, [], exc

        concurrency = 8
        for offset in range(0, len(pending["SSE"]), concurrency):
            batch_dates = pending["SSE"][offset : offset + concurrency]
            results = await asyncio.gather(
                *(fetch_sse_one(target) for target in batch_dates)
            )
            for target, rows, error in results:
                if error is not None:
                    LOGGER.error(
                        "SSE option stats backfill failed [%s]: %s",
                        target,
                        error,
                    )
                    failed["SSE"] += 1
                    continue
                inserted["SSE"] += await db.batch_exchange_option_daily_stats(rows)
                processed["SSE"] += 1
            if processed["SSE"] and processed["SSE"] % 100 < concurrency:
                print(
                    f"exchange option SSE stats: dates={processed['SSE']}/"
                    f"{len(pending['SSE'])}, rows={inserted['SSE']}"
                )

        consecutive_szse_failures = 0
        for index, target in enumerate(pending["SZSE"], start=1):
            started_at = time.monotonic()
            try:
                rows = await asyncio.to_thread(fetch_szse_stats_rows_sync, target)
                if not rows:
                    raise RuntimeError("empty SZSE stats response")
                inserted["SZSE"] += await db.batch_exchange_option_daily_stats(rows)
                processed["SZSE"] += 1
                consecutive_szse_failures = 0
            except Exception as exc:
                LOGGER.error(
                    "SZSE option stats backfill failed [%s]: %s",
                    target,
                    exc,
                )
                failed["SZSE"] += 1
                consecutive_szse_failures += 1
                if consecutive_szse_failures >= 3:
                    LOGGER.error(
                        "SZSE stats source appears rate limited; paused after %s "
                        "consecutive failures",
                        consecutive_szse_failures,
                    )
                    break
            elapsed = time.monotonic() - started_at
            if elapsed < 2:
                await asyncio.sleep(2 - elapsed)
            if index % 50 == 0:
                print(
                    f"exchange option SZSE stats: dates={processed['SZSE']}/"
                    f"{len(pending['SZSE'])}, rows={inserted['SZSE']}"
                )

        remaining = {
            exchange: max(
                0,
                len(pending[exchange]) - processed[exchange],
            )
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }
        is_complete = all(value == 0 for value in remaining.values())
        return {
            "status": "SUCCESS" if is_complete else "PARTIAL",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "processed_dates": processed,
            "failed_dates": failed,
            "remaining_dates": remaining,
            "rows": inserted,
        }
    finally:
        await db.close()


async def backfill_official(start_date=None, end_date=None):
    start = parse_date(start_date or min(EXCHANGE_LISTED_DATES.values()))
    end = parse_date(end_date or date.today())
    db = DbTools()
    await db.init_pool()
    processed = {"SSE": 0, "SZSE": 0}
    contract_count = {"SSE": 0, "SZSE": 0}
    failed = {"SSE": 0, "SZSE": 0}
    try:
        await db.ensure_exchange_option_tables()
        trade_dates = await db.list_cn_trade_dates(start, end)
        if not trade_dates:
            trade_dates = list(iter_weekdays(start, end))
        existing_dates = {
            exchange: await db.list_exchange_option_risk_dates(exchange, start, end)
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }
        pending = {
            exchange: [
                target
                for target in trade_dates
                if target >= EXCHANGE_LISTED_DATES[exchange]
                and target not in existing_dates[exchange]
            ]
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }

        async def fetch_sse_one(target):
            try:
                rows = await asyncio.to_thread(fetch_sse_risk_rows_sync, target)
                if not rows:
                    raise RuntimeError("empty SSE risk response")
                return target, rows, None
            except Exception as exc:
                return target, [], exc

        concurrency = 8
        for offset in range(0, len(pending["SSE"]), concurrency):
            batch_dates = pending["SSE"][offset : offset + concurrency]
            results = await asyncio.gather(
                *(fetch_sse_one(target) for target in batch_dates)
            )
            for target, rows, error in results:
                if error is not None:
                    LOGGER.error(
                        "SSE option risk backfill failed [%s]: %s",
                        target,
                        error,
                    )
                    failed["SSE"] += 1
                    continue
                contract_count["SSE"] += (
                    await db.batch_exchange_option_contract_daily_data(rows)
                )
                processed["SSE"] += 1
            if processed["SSE"] and processed["SSE"] % 100 < concurrency:
                print(
                    f"exchange option SSE risk: dates={processed['SSE']}/"
                    f"{len(pending['SSE'])}, rows={contract_count['SSE']}"
                )

        consecutive_szse_failures = 0
        for index, target in enumerate(pending["SZSE"], start=1):
            started_at = time.monotonic()
            try:
                rows = await asyncio.to_thread(fetch_szse_risk_rows_sync, target)
                if not rows:
                    raise RuntimeError("empty SZSE risk response")
                contract_count["SZSE"] += (
                    await db.batch_exchange_option_contract_daily_data(rows)
                )
                processed["SZSE"] += 1
                consecutive_szse_failures = 0
            except Exception as exc:
                LOGGER.error(
                    "SZSE option risk backfill failed [%s]: %s",
                    target,
                    exc,
                )
                failed["SZSE"] += 1
                consecutive_szse_failures += 1
                if consecutive_szse_failures >= 3:
                    LOGGER.error(
                        "SZSE risk source appears rate limited; paused after %s "
                        "consecutive failures",
                        consecutive_szse_failures,
                    )
                    break
            elapsed = time.monotonic() - started_at
            if elapsed < 2:
                await asyncio.sleep(2 - elapsed)
            if index % 25 == 0:
                print(
                    f"exchange option SZSE risk: dates={processed['SZSE']}/"
                    f"{len(pending['SZSE'])}, rows={contract_count['SZSE']}"
                )

        remaining = {
            exchange: max(0, len(pending[exchange]) - processed[exchange])
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }
        is_complete = all(value == 0 for value in remaining.values())
        return {
            "status": "SUCCESS" if is_complete else "PARTIAL",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "processed_days": processed,
            "failed_days": failed,
            "remaining_days": remaining,
            "contract_rows": contract_count,
        }
    finally:
        await db.close()


async def backfill_klines(limit=None):
    db = DbTools()
    await db.init_pool()
    completed = KLINE_PROGRESS.load()
    semaphore = asyncio.Semaphore(HISTORY_CONCURRENCY)
    inserted = 0
    processed = 0
    failed = 0
    consecutive_failed_batches = 0

    async def fetch_one(exchange, contract_code):
        async with semaphore:
            try:
                rows = await asyncio.to_thread(
                    fetch_sina_history_sync,
                    exchange,
                    contract_code,
                )
                return exchange, contract_code, rows, None
            except Exception as exc:
                return exchange, contract_code, [], exc

    try:
        await db.ensure_exchange_option_tables()
        trade_dates = await db.list_cn_trade_dates(
            EXCHANGE_LISTED_DATES["SSE"],
            date.today(),
        )
        if not trade_dates:
            trade_dates = list(
                iter_weekdays(EXCHANGE_LISTED_DATES["SSE"], date.today())
            )
        contracts = await db.list_exchange_option_contract_codes()
        pending = [
            item for item in contracts
            if f"{item[0]}:{item[1]}" not in completed
        ]
        if limit:
            pending = pending[: int(limit)]
        for offset in range(0, len(pending), HISTORY_CONCURRENCY):
            batch = pending[offset : offset + HISTORY_CONCURRENCY]
            results = await asyncio.gather(
                *(fetch_one(exchange, code) for exchange, code in batch)
            )
            batch_processed = 0
            for exchange, contract_code, rows, error in results:
                key = f"{exchange}:{contract_code}"
                if error is not None:
                    LOGGER.error("option kline backfill failed [%s]: %s", key, error)
                    failed += 1
                    continue
                filtered_rows = filter_history_rows_to_trade_dates(
                    rows,
                    trade_dates,
                )
                inserted += await db.batch_exchange_option_contract_daily_data(
                    filtered_rows
                )
                KLINE_PROGRESS.append(key)
                processed += 1
                batch_processed += 1
            if batch_processed:
                consecutive_failed_batches = 0
            else:
                consecutive_failed_batches += 1
            print(
                f"exchange option kline backfill: contracts={processed}/{len(pending)}, "
                f"rows={inserted}"
            )
            if consecutive_failed_batches >= 3:
                LOGGER.error(
                    "option kline source appears rate limited; paused after %s "
                    "consecutive failed batches",
                    consecutive_failed_batches,
                )
                break
        return {
            "status": "SUCCESS" if processed == len(pending) else "PARTIAL",
            "processed_contracts": processed,
            "total_contracts": len(pending),
            "failed_contracts": failed,
            "rows": inserted,
        }
    finally:
        await db.close()


async def backfill_official_klines(limit=None):
    db = DbTools()
    await db.init_pool()
    completed = OFFICIAL_KLINE_PROGRESS.load()
    inserted = {"SSE": 0, "SZSE": 0}
    processed = {"SSE": 0, "SZSE": 0}
    failed = {"SSE": 0, "SZSE": 0}
    try:
        await db.ensure_exchange_option_tables()
        trade_dates = await db.list_cn_trade_dates(
            EXCHANGE_LISTED_DATES["SSE"],
            date.today(),
        )
        if not trade_dates:
            trade_dates = list(
                iter_weekdays(EXCHANGE_LISTED_DATES["SSE"], date.today())
            )
        contracts = await db.list_exchange_option_contract_codes()
        pending = {
            exchange: [
                (item_exchange, contract_code)
                for item_exchange, contract_code in contracts
                if item_exchange == exchange
                and f"{item_exchange}:{contract_code}" not in completed
            ]
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }
        if limit:
            remaining_limit = int(limit)
            for exchange in ("SSE", "SZSE"):
                pending[exchange] = pending[exchange][:remaining_limit]
                remaining_limit = max(0, remaining_limit - len(pending[exchange]))

        async def process_exchange(exchange):
            consecutive_failures = 0
            for index, (_item_exchange, contract_code) in enumerate(
                pending[exchange],
                start=1,
            ):
                started_at = time.monotonic()
                key = f"{exchange}:{contract_code}"
                try:
                    rows = await asyncio.to_thread(
                        fetch_official_history_sync,
                        exchange,
                        contract_code,
                    )
                    rows = filter_history_rows_to_trade_dates(rows, trade_dates)
                    inserted[exchange] += (
                        await db.batch_exchange_option_contract_daily_data(rows)
                    )
                    OFFICIAL_KLINE_PROGRESS.append(key)
                    processed[exchange] += 1
                    consecutive_failures = 0
                except Exception as exc:
                    LOGGER.error(
                        "official option kline backfill failed [%s]: %s",
                        key,
                        exc,
                    )
                    failed[exchange] += 1
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        LOGGER.error(
                            "%s official option kline source paused after %s "
                            "consecutive failures",
                            exchange,
                            consecutive_failures,
                        )
                        break
                elapsed = time.monotonic() - started_at
                if elapsed < OFFICIAL_HISTORY_REQUEST_INTERVAL_SECONDS:
                    await asyncio.sleep(
                        OFFICIAL_HISTORY_REQUEST_INTERVAL_SECONDS - elapsed
                    )
                if index % 50 == 0:
                    print(
                        f"exchange option {exchange} official kline: "
                        f"contracts={processed[exchange]}/{len(pending[exchange])}, "
                        f"rows={inserted[exchange]}"
                    )

        await asyncio.gather(
            process_exchange("SSE"),
            process_exchange("SZSE"),
        )
        remaining = {
            exchange: max(
                0,
                len(pending[exchange]) - processed[exchange],
            )
            for exchange in EXCHANGE_OPTION_PRODUCTS
        }
        return {
            "status": (
                "SUCCESS"
                if all(value == 0 for value in remaining.values())
                else "PARTIAL"
            ),
            "processed_contracts": processed,
            "failed_contracts": failed,
            "remaining_contracts": remaining,
            "rows": inserted,
        }
    finally:
        await db.close()


async def backfill_all(start_date=None, end_date=None, kline_limit=None):
    contract_info = await backfill_contract_info(
        start_date=start_date,
        end_date=end_date,
    )
    klines = await backfill_official_klines(limit=kline_limit)
    stats = await backfill_stats(start_date=start_date, end_date=end_date)
    return {"contract_info": contract_info, "klines": klines, "stats": stats}


async def backfill_pre_settle_prices():
    db = DbTools()
    await db.init_pool()
    try:
        result = await db.backfill_exchange_option_pre_settle_prices()
        print(f"exchange option pre-settle backfill finished: {result}")
        return result
    finally:
        await db.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "daily"
    args = sys.argv[2:]
    if command == "daily":
        target_date = args[0] if args else None
        print(await sync_daily(target_date=target_date))
        return
    if command == "backfill":
        start_date = args[0] if len(args) >= 1 else None
        end_date = args[1] if len(args) >= 2 else None
        print(await backfill_all(start_date=start_date, end_date=end_date))
        return
    if command == "backfill-official":
        start_date = args[0] if len(args) >= 1 else None
        end_date = args[1] if len(args) >= 2 else None
        print(await backfill_official(start_date=start_date, end_date=end_date))
        return
    if command == "backfill-contracts":
        start_date = args[0] if len(args) >= 1 else None
        end_date = args[1] if len(args) >= 2 else None
        print(await backfill_contract_info(start_date=start_date, end_date=end_date))
        return
    if command == "backfill-stats":
        start_date = args[0] if len(args) >= 1 else None
        end_date = args[1] if len(args) >= 2 else None
        print(await backfill_stats(start_date=start_date, end_date=end_date))
        return
    if command == "backfill-klines":
        limit = int(args[0]) if args else None
        print(await backfill_klines(limit=limit))
        return
    if command == "backfill-official-klines":
        limit = int(args[0]) if args else None
        print(await backfill_official_klines(limit=limit))
        return
    if command == "backfill-pre-settle":
        print(await backfill_pre_settle_prices())
        return
    raise ValueError(
        "exchange-option supports: daily [date] | backfill [start] [end] | "
        "backfill-contracts [start] [end] | backfill-stats [start] [end] | "
        "backfill-official [start] [end] | backfill-klines [limit] | "
        "backfill-official-klines [limit] | backfill-pre-settle"
    )


if __name__ == "__main__":
    asyncio.run(main())
