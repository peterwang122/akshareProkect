import asyncio
import ast
import html
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta

import akshare as ak
import pandas as pd
import requests

from akshare_project.collectors import index as index_collector
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools


LOGGER = get_logger("global_risk")

FRED_ASSETS = {
    "SOX": ("费城半导体指数", "NASDAQSOX"),
    "WTI": ("WTI原油", "DCOILWTICO"),
    "BRENT": ("布伦特原油", "DCOILBRENTEU"),
}
ISHARES_ASSETS = {
    "IXN_NAV": ("iShares全球科技ETF NAV", "239750"),
    "ACWI_NAV": ("iShares全球股票ETF NAV", "239600"),
}
ISHARES_DOWNLOAD_URL = (
    "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/"
    "api/v1/get-fund-document"
)
KRX_KOSPI_API_URL = "https://data-dbg.krx.co.kr/svc/apis/idx/kospi_dd_trd"
KRX_SERVICE_URL = "https://openapi.krx.co.kr/contents/OPP/INFO/service/OPPINFO004.cmd"
EASTMONEY_KOSPI_URL = "https://quote.eastmoney.com/center/hszs.html"
EASTMONEY_COPPER_URL = "https://quote.eastmoney.com/globalfuture/HG00Y.html"
NAVER_KOSPI_URL = "https://api.finance.naver.com/siseJson.naver"
SINA_COPPER_URL = "https://finance.sina.com.cn/futures/quotes/HG.shtml"
CSI_INDEX_PERF_URL = "https://www.csindex.com.cn/csindex-home/perf/index-perf"
CSI_TECH_INDEXES = {
    "000985": ("sh000985", "中证全指"),
    "000993": ("sh000993", "全指信息"),
}
PEAKSTONE_PANEL_URL = "https://panel.peakstone-labs.com/"
TURNOVER_CONCENTRATION_HISTORY_START = date(2016, 9, 26)
GLOBAL_HISTORY_START = date(2004, 1, 1)
TECH_HISTORY_START = date(2005, 1, 1)
RECENT_REPAIR_CALENDAR_DAYS = 24


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def _to_float(value):
    try:
        if value in (None, "", "--", "."):
            return None
        number = float(str(value).replace(",", "").strip())
        if not pd.notna(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def _date_text(value):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.date().isoformat()


def _available_at(trade_date, asset_code):
    source_date = datetime.strptime(trade_date, "%Y-%m-%d")
    if asset_code == "KOSPI":
        return source_date.replace(hour=16, minute=30, second=0)
    return (source_date + timedelta(days=1)).replace(hour=8, minute=0, second=0)


def _row(
    asset_code,
    asset_name,
    trade_date,
    close_value,
    *,
    open_value=None,
    high_value=None,
    low_value=None,
    volume=None,
    data_source,
    source_url,
    raw_json=None,
):
    return {
        "asset_code": asset_code,
        "asset_name": asset_name,
        "trade_date": trade_date,
        "open_value": open_value,
        "high_value": high_value,
        "low_value": low_value,
        "close_value": close_value,
        "volume": volume,
        "source_date": trade_date,
        "available_at": _available_at(trade_date, asset_code),
        "data_source": data_source,
        "source_url": source_url,
        "raw_json": raw_json,
    }


def build_fred_asset_rows(csv_text, asset_code, asset_name, series_id):
    points = index_collector.build_fred_series_points(csv_text, series_id)
    return [
        _row(
            asset_code,
            asset_name,
            trade_date,
            value,
            data_source="fred_public_csv",
            source_url=f"{index_collector.FRED_CSV_URL}?id={series_id}",
            raw_json={"series_id": series_id, "value": value},
        )
        for trade_date, value in sorted(points.items())
        if value is not None
    ]


def _extract_xml_values(row_xml):
    values = []
    for value in re.findall(r"<ss:Data[^>]*>(.*?)</ss:Data>", row_xml, flags=re.I | re.S):
        value = re.sub(r"<[^>]+>", "", value)
        values.append(html.unescape(value).strip())
    return values


def build_ishares_nav_rows(xml_text, asset_code, asset_name, portfolio_id):
    worksheet_match = re.search(
        r'<ss:Worksheet\s+ss:Name="Historical"[^>]*>(.*?)</ss:Worksheet>',
        xml_text or "",
        flags=re.I | re.S,
    )
    if not worksheet_match:
        raise ValueError(f"iShares {asset_code} document has no Historical worksheet")

    headers = None
    rows = []
    source_url = f"{ISHARES_DOWNLOAD_URL}?portfolioId={portfolio_id}"
    for row_xml in re.findall(r"<ss:Row[^>]*>(.*?)</ss:Row>", worksheet_match.group(1), flags=re.I | re.S):
        values = _extract_xml_values(row_xml)
        if not values:
            continue
        normalized = [re.sub(r"\s+", " ", value).strip().lower() for value in values]
        if "as of" in normalized and any("nav per share" in value for value in normalized):
            headers = normalized
            continue
        if headers is None:
            continue
        mapped = {headers[index]: value for index, value in enumerate(values[: len(headers)])}
        trade_date = _date_text(mapped.get("as of"))
        nav_key = next((key for key in headers if "nav per share" in key), None)
        nav_value = _to_float(mapped.get(nav_key)) if nav_key else None
        if not trade_date or nav_value is None or nav_value <= 0:
            continue
        rows.append(
            _row(
                asset_code,
                asset_name,
                trade_date,
                nav_value,
                data_source="blackrock_ishares_historical_nav",
                source_url=source_url,
                raw_json={"as_of": mapped.get("as of"), "nav_per_share": nav_value},
            )
        )
    deduped = {item["trade_date"]: item for item in rows}
    return [deduped[key] for key in sorted(deduped)]


def fetch_ishares_nav_document(portfolio_id):
    response = requests.get(
        ISHARES_DOWNLOAD_URL,
        params={
            "appSubType": "ISHARES",
            "appType": "PRODUCT_PAGE",
            "component": "fundDownload",
            "locale": "en_US",
            "portfolioId": portfolio_id,
            "targetSite": "us-ishares",
            "userType": "individual",
        },
        headers=index_collector.DEFAULT_HTTP_HEADERS,
        timeout=120,
    )
    response.raise_for_status()
    return response.text


def _find_column(dataframe, aliases):
    normalized = {str(column).strip().lower(): column for column in dataframe.columns}
    for alias in aliases:
        if alias.lower() in normalized:
            return normalized[alias.lower()]
    return None


def build_dataframe_asset_rows(dataframe, asset_code, asset_name, data_source, source_url):
    if dataframe is None or dataframe.empty:
        return []
    date_column = _find_column(dataframe, ("日期", "date", "trade_date"))
    close_column = _find_column(dataframe, ("收盘", "最新价", "close", "latest"))
    if date_column is None or close_column is None:
        raise ValueError(f"{asset_code} source columns are unsupported: {list(dataframe.columns)}")
    open_column = _find_column(dataframe, ("开盘", "open"))
    high_column = _find_column(dataframe, ("最高", "high"))
    low_column = _find_column(dataframe, ("最低", "low"))
    volume_column = _find_column(dataframe, ("成交量", "总量", "volume"))
    rows = []
    for _, source_row in dataframe.iterrows():
        trade_date = _date_text(source_row.get(date_column))
        close_value = _to_float(source_row.get(close_column))
        if not trade_date or close_value is None or close_value <= 0:
            continue
        raw_payload = {
            str(key): None if pd.isna(value) else str(value)
            for key, value in source_row.to_dict().items()
        }
        rows.append(
            _row(
                asset_code,
                asset_name,
                trade_date,
                close_value,
                open_value=_to_float(source_row.get(open_column)) if open_column else None,
                high_value=_to_float(source_row.get(high_column)) if high_column else None,
                low_value=_to_float(source_row.get(low_column)) if low_column else None,
                volume=_to_float(source_row.get(volume_column)) if volume_column else None,
                data_source=data_source,
                source_url=source_url,
                raw_json=raw_payload,
            )
        )
    return sorted(rows, key=lambda item: item["trade_date"])


def fetch_krx_kospi_row(trade_date, auth_key):
    response = requests.get(
        KRX_KOSPI_API_URL,
        params={"basDd": trade_date.replace("-", "")},
        headers={"AUTH_KEY": auth_key, **index_collector.DEFAULT_HTTP_HEADERS},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("OutBlock_1") if isinstance(payload, dict) else None
    kospi = next(
        (item for item in (rows or []) if str(item.get("IDX_NM") or "").strip().upper() == "KOSPI"),
        None,
    )
    if not kospi:
        return None
    source_date = _date_text(kospi.get("BAS_DD") or trade_date)
    close_value = _to_float(kospi.get("CLSPRC_IDX"))
    if not source_date or close_value is None:
        return None
    return _row(
        "KOSPI",
        "韩国KOSPI",
        source_date,
        close_value,
        open_value=_to_float(kospi.get("OPNPRC_IDX")),
        high_value=_to_float(kospi.get("HGPRC_IDX")),
        low_value=_to_float(kospi.get("LWPRC_IDX")),
        volume=_to_float(kospi.get("ACC_TRDVOL")),
        data_source="krx_open_api",
        source_url=KRX_KOSPI_API_URL,
        raw_json=kospi,
    )


def fetch_kospi_rows(start_date, end_date, daily_only=False):
    auth_key = str(os.getenv("KRX_OPEN_API_KEY") or "").strip()
    if auth_key and daily_only:
        current = end_date
        while current >= start_date and current.weekday() >= 5:
            current -= timedelta(days=1)
        for offset in range(10):
            candidate = current - timedelta(days=offset)
            if candidate.weekday() >= 5:
                continue
            row = fetch_krx_kospi_row(candidate.isoformat(), auth_key)
            if row:
                return [row]
            time.sleep(0.1)

    response = requests.get(
        NAVER_KOSPI_URL,
        params={
            "symbol": "KOSPI",
            "requestType": 1,
            "startTime": start_date.strftime("%Y%m%d"),
            "endTime": end_date.strftime("%Y%m%d"),
            "timeframe": "day",
        },
        headers=index_collector.DEFAULT_HTTP_HEADERS,
        timeout=60,
    )
    response.raise_for_status()
    payload = ast.literal_eval(response.text.strip())
    rows = []
    for source in payload[1:]:
        if not isinstance(source, (list, tuple)) or len(source) < 6:
            continue
        trade_date = _date_text(source[0])
        close_value = _to_float(source[4])
        if not trade_date or close_value is None:
            continue
        rows.append(_row(
            "KOSPI",
            "韩国KOSPI",
            trade_date,
            close_value,
            open_value=_to_float(source[1]),
            high_value=_to_float(source[2]),
            low_value=_to_float(source[3]),
            volume=_to_float(source[5]),
            data_source="naver_finance_kospi_fallback",
            source_url=NAVER_KOSPI_URL,
            raw_json={
                "date": source[0], "open": source[1], "high": source[2],
                "low": source[3], "close": source[4], "volume": source[5],
            },
        ))
    return [
        row for row in rows if start_date.isoformat() <= row["trade_date"] <= end_date.isoformat()
    ]


def fetch_copper_rows(start_date, end_date):
    dataframe = ak.futures_foreign_hist(symbol="HG")
    rows = build_dataframe_asset_rows(
        dataframe,
        "COPPER_HG",
        "COMEX铜连续",
        "sina_comex_hg_continuous",
        SINA_COPPER_URL,
    )
    return [
        row for row in rows if start_date.isoformat() <= row["trade_date"] <= end_date.isoformat()
    ]


def fetch_csi_index_perf(simple_code, start_date, end_date):
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        CSI_INDEX_PERF_URL,
        params={
            "indexCode": simple_code,
            "startDate": start_date.strftime("%Y%m%d"),
            "endDate": end_date.strftime("%Y%m%d"),
        },
        headers={
            **index_collector.DEFAULT_HTTP_HEADERS,
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.csindex.com.cn/",
        },
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError(f"CSI {simple_code} performance response has no data rows")
    return index_collector.drop_csi_index_perf_boundary_duplicate(rows, start_date)


def build_csi_tech_index_rows(source_rows, simple_code, prefixed_code):
    rows = []
    for source in source_rows or []:
        trade_date = _date_text(source.get("tradeDate"))
        close_value = _to_float(source.get("close"))
        if not trade_date or close_value is None or close_value <= 0:
            continue
        open_value = _to_float(source.get("open"))
        high_value = _to_float(source.get("high"))
        low_value = _to_float(source.get("low"))
        change_value = _to_float(source.get("change"))
        previous_close = close_value - change_value if change_value is not None else None
        trading_value = _to_float(source.get("tradingValue"))
        rows.append({
            "index_code": prefixed_code,
            "open_price": open_value,
            "close_price": close_value,
            "high_price": high_value,
            "low_price": low_value,
            "volume": _to_float(source.get("tradingVol")),
            "turnover": round(trading_value * 100_000_000, 2) if trading_value is not None else None,
            "amplitude": index_collector.calculate_amplitude(high_value, low_value, previous_close),
            "price_change_rate": _to_float(source.get("changePct")),
            "price_change_amount": change_value,
            "turnover_rate": None,
            "trade_date": trade_date,
            "data_source": "csindex_official_index_perf",
        })
    return sorted(rows, key=lambda item: item["trade_date"])


def build_peakstone_turnover_concentration_rows(page_html):
    match = re.search(
        r'<script[^>]+id=["\']panel-data["\'][^>]*>(.*?)</script>',
        page_html or "",
        flags=re.I | re.S,
    )
    if not match:
        raise ValueError("Peakstone panel response has no panel-data payload")
    payload = json.loads(html.unescape(match.group(1)).strip())
    concentration = payload.get("concentration") if isinstance(payload, dict) else None
    dates = concentration.get("dates") if isinstance(concentration, dict) else None
    values = concentration.get("values") if isinstance(concentration, dict) else None
    if not isinstance(dates, list) or not isinstance(values, list) or len(dates) != len(values):
        raise ValueError("Peakstone panel concentration payload is incomplete")

    rows = []
    for raw_date, raw_value in zip(dates, values):
        trade_date = _date_text(raw_date)
        value = _to_float(raw_value)
        if not trade_date or value is None or not 0 <= value <= 100:
            continue
        source_date = datetime.strptime(trade_date, "%Y-%m-%d")
        rows.append({
            "trade_date": trade_date,
            "top5_pct": round(value, 6),
            "top5_data_source": "peakstone_top5_turnover_concentration_ma5",
            "top5_source_url": PEAKSTONE_PANEL_URL,
            "source_date": trade_date,
            "available_at": source_date.replace(hour=22, minute=0, second=0),
            "raw_json": {
                "top5": {
                    "value": value,
                    "definition": "每日成交额最高前5%个股占全市场比例（MA5平滑）",
                    "source": "peakstone_panel",
                }
            },
        })
    deduped = {row["trade_date"]: row for row in rows}
    return [deduped[key] for key in sorted(deduped)]


def fetch_peakstone_turnover_concentration_page():
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        PEAKSTONE_PANEL_URL,
        headers={
            **index_collector.DEFAULT_HTTP_HEADERS,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=120,
    )
    response.raise_for_status()
    return response.text


def build_top1_turnover_concentration_rows(source_rows):
    normalized = []
    for source in source_rows or []:
        trade_date = _date_text(source.get("trade_date"))
        total = _to_float(source.get("total_turnover_amount"))
        top1 = _to_float(source.get("top1_turnover_amount"))
        stock_count = int(source.get("stock_count") or 0)
        top1_stock_count = int(source.get("top1_stock_count") or 0)
        if (
            not trade_date
            or total is None
            or top1 is None
            or total < 100_000_000_000
            or top1 <= 0
            or stock_count < 1000
            or top1_stock_count <= 0
        ):
            continue
        normalized.append({
            "trade_date": trade_date,
            "raw_pct": top1 / total * 100.0,
            "stock_count": stock_count,
            "top1_stock_count": top1_stock_count,
            "total_turnover_amount": total,
            "top1_turnover_amount": top1,
        })
    normalized = sorted(
        {row["trade_date"]: row for row in normalized}.values(),
        key=lambda row: row["trade_date"],
    )
    rows = []
    raw_values = []
    for item in normalized:
        raw_values.append(item["raw_pct"])
        top1_pct = (
            sum(raw_values[-5:]) / 5.0
            if len(raw_values) >= 5
            else None
        )
        rows.append({
            "trade_date": item["trade_date"],
            "top1_pct": round(top1_pct, 6) if top1_pct is not None else None,
            "top1_raw_pct": round(item["raw_pct"], 6),
            "stock_count": item["stock_count"],
            "top1_stock_count": item["top1_stock_count"],
            "total_turnover_amount": round(item["total_turnover_amount"], 2),
            "top1_turnover_amount": round(item["top1_turnover_amount"], 2),
            "top1_data_source": "local_a_share_top1_turnover_concentration_ma5",
            "raw_json": {
                "top1": {
                    "raw_pct": round(item["raw_pct"], 6),
                    "ma5_pct": round(top1_pct, 6) if top1_pct is not None else None,
                    "stock_count": item["stock_count"],
                    "top1_stock_count": item["top1_stock_count"],
                    "definition": "每日成交额最高前1%个股占全市场比例（MA5平滑）",
                }
            },
        })
    return rows


async def collect_global_rows(start_date, end_date, daily_only=False):
    tasks = []
    for asset_code, (asset_name, series_id) in FRED_ASSETS.items():
        tasks.append(asyncio.to_thread(index_collector.fetch_fred_series_csv, series_id))
    fred_payloads = await asyncio.gather(*tasks)
    rows = []
    for (asset_code, (asset_name, series_id)), csv_text in zip(FRED_ASSETS.items(), fred_payloads):
        rows.extend(build_fred_asset_rows(csv_text, asset_code, asset_name, series_id))

    for asset_code, (asset_name, portfolio_id) in ISHARES_ASSETS.items():
        document = await asyncio.to_thread(fetch_ishares_nav_document, portfolio_id)
        rows.extend(build_ishares_nav_rows(document, asset_code, asset_name, portfolio_id))

    kospi_rows, copper_rows = await asyncio.gather(
        asyncio.to_thread(fetch_kospi_rows, start_date, end_date, daily_only),
        asyncio.to_thread(fetch_copper_rows, start_date, end_date),
    )
    rows.extend(kospi_rows)
    rows.extend(copper_rows)
    start_text = start_date.isoformat()
    end_text = end_date.isoformat()
    filtered = [row for row in rows if start_text <= row["trade_date"] <= end_text]
    if daily_only:
        by_asset = {}
        for row in filtered:
            by_asset.setdefault(row["asset_code"], []).append(row)
        filtered = [
            row
            for asset_rows in by_asset.values()
            for row in sorted(asset_rows, key=lambda item: item["trade_date"])[-10:]
        ]
    return filtered


async def backfill_global_risk_assets(start_date=None, end_date=None):
    start_date = start_date or GLOBAL_HISTORY_START
    end_date = end_date or date.today()
    rows = await collect_global_rows(start_date, end_date, daily_only=False)
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        affected = await db_tools.upsert_global_risk_asset_daily_rows(rows)
    finally:
        await db_tools.close()
    counts = {}
    for row in rows:
        counts[row["asset_code"]] = counts.get(row["asset_code"], 0) + 1
    result = {"status": "SUCCESS", "affected": affected, "counts": counts}
    print(f"global risk asset backfill finished: {json.dumps(result, ensure_ascii=False)}")
    return result


async def sync_global_risk_daily(target_date=None):
    end_date = pd.to_datetime(target_date).date() if target_date else date.today()
    start_date = end_date - timedelta(days=RECENT_REPAIR_CALENDAR_DAYS)
    rows = await collect_global_rows(start_date, end_date, daily_only=True)
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        affected = await db_tools.upsert_global_risk_asset_daily_rows(rows)
    finally:
        await db_tools.close()
    latest_dates = {}
    for row in rows:
        latest_dates[row["asset_code"]] = max(
            latest_dates.get(row["asset_code"], ""), row["trade_date"]
        )
    required = set(FRED_ASSETS) | set(ISHARES_ASSETS) | {"KOSPI", "COPPER_HG"}
    missing = sorted(required - set(latest_dates))
    if missing:
        raise RuntimeError(f"global risk daily missing assets: {missing}")
    return {
        "status": "SUCCESS",
        "affected": affected,
        "latest_dates": latest_dates,
        "repair_rows": len(rows),
    }


async def backfill_csi_tech_indexes(start_date=None, end_date=None):
    start_date = start_date or TECH_HISTORY_START
    end_date = end_date or date.today()
    db_tools = DbTools()
    await db_tools.init_pool()
    counts = {}
    try:
        for simple_code, (prefixed_code, name) in CSI_TECH_INDEXES.items():
            source_rows = await asyncio.to_thread(
                fetch_csi_index_perf, simple_code, start_date, end_date
            )
            rows = build_csi_tech_index_rows(source_rows, simple_code, prefixed_code)
            await db_tools.upsert_index_basic_info([{
                "index_code": prefixed_code,
                "simple_code": simple_code,
                "market": "sh",
                "index_name": name,
                "data_source": "csindex_official_index_perf",
            }])
            counts[prefixed_code] = await db_tools.upsert_index_daily_data(rows)
    finally:
        await db_tools.close()
    return {"status": "SUCCESS", "counts": counts}


async def sync_csi_tech_concentration_daily(target_date=None):
    end_date = pd.to_datetime(target_date).date() if target_date else date.today()
    start_date = end_date - timedelta(days=RECENT_REPAIR_CALENDAR_DAYS)
    result = await backfill_csi_tech_indexes(start_date=start_date, end_date=end_date)
    result["target_date"] = end_date.isoformat()
    return result


async def backfill_a_share_turnover_concentration(start_date=None, end_date=None):
    start_date = start_date or TURNOVER_CONCENTRATION_HISTORY_START
    end_date = end_date or date.today()
    page_html = await asyncio.to_thread(fetch_peakstone_turnover_concentration_page)
    top5_rows = [
        row for row in build_peakstone_turnover_concentration_rows(page_html)
        if start_date.isoformat() <= row["trade_date"] <= end_date.isoformat()
    ]
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        top1_source_rows = await db_tools.calculate_a_share_top1_turnover_concentration_rows(
            start_date, end_date
        )
        top1_rows = build_top1_turnover_concentration_rows(top1_source_rows)
        top5_affected = await db_tools.upsert_a_share_turnover_concentration_daily_rows(top5_rows)
        top1_affected = await db_tools.upsert_a_share_turnover_concentration_daily_rows(top1_rows)
    finally:
        await db_tools.close()
    return {
        "status": "SUCCESS",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "top5_rows": len(top5_rows),
        "top1_rows": len(top1_rows),
        "affected": top5_affected + top1_affected,
    }


async def sync_a_share_turnover_concentration_daily(target_date=None):
    end_date = pd.to_datetime(target_date).date() if target_date else date.today()
    start_date = end_date - timedelta(days=RECENT_REPAIR_CALENDAR_DAYS)
    result = await backfill_a_share_turnover_concentration(start_date, end_date)
    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        target_rows = await db_tools.get_a_share_turnover_concentration_daily_rows(
            end_date, end_date
        )
    finally:
        await db_tools.close()
    target_row = target_rows[0] if target_rows else {}
    has_top5 = _to_float(target_row.get("top5_pct")) is not None
    has_top1 = _to_float(target_row.get("top1_pct")) is not None
    result.update({
        "target_date": end_date.isoformat(),
        "top5_pct": _to_float(target_row.get("top5_pct")),
        "top1_pct": _to_float(target_row.get("top1_pct")),
    })
    if not has_top5 or not has_top1:
        result["status"] = "SOURCE_NOT_READY"
        result["missing"] = [
            label for label, available in (("前5%公开序列", has_top5), ("前1%逐股计算", has_top1))
            if not available
        ]
    return result


def _parse_date(value, fallback):
    if not value:
        return fallback
    return datetime.strptime(str(value), "%Y-%m-%d").date()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "backfill"
    args = sys.argv[2:]
    if command == "backfill":
        await backfill_global_risk_assets(
            _parse_date(args[0], GLOBAL_HISTORY_START) if args else GLOBAL_HISTORY_START,
            _parse_date(args[1], date.today()) if len(args) > 1 else date.today(),
        )
        return
    if command == "daily":
        await sync_global_risk_daily(args[0] if args else None)
        return
    if command == "backfill-tech":
        await backfill_csi_tech_indexes(
            _parse_date(args[0], TECH_HISTORY_START) if args else TECH_HISTORY_START,
            _parse_date(args[1], date.today()) if len(args) > 1 else date.today(),
        )
        return
    if command == "daily-tech":
        await sync_csi_tech_concentration_daily(args[0] if args else None)
        return
    if command == "backfill-concentration":
        await backfill_a_share_turnover_concentration(
            _parse_date(args[0], TURNOVER_CONCENTRATION_HISTORY_START)
            if args else TURNOVER_CONCENTRATION_HISTORY_START,
            _parse_date(args[1], date.today()) if len(args) > 1 else date.today(),
        )
        return
    if command == "daily-concentration":
        await sync_a_share_turnover_concentration_daily(args[0] if args else None)
        return
    raise ValueError(
        "global-risk supports: backfill [start_date] [end_date] | daily [target_date] | "
        "backfill-tech [start_date] [end_date] | daily-tech [target_date] | "
        "backfill-concentration [start_date] [end_date] | daily-concentration [target_date]"
    )


if __name__ == "__main__":
    asyncio.run(main())
