import asyncio
import json
import re
import sys
import time
import warnings
from bisect import bisect_right
from datetime import date, datetime, timedelta
from io import BytesIO, StringIO
from statistics import median
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from akshare.stock_feature.stock_a_indicator import get_token_lg

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools


LOGGER = get_logger("cn_macro")
CSI_PERF_URL = "https://www.csindex.com.cn/csindex-home/perf/index-perf"
CHINAMONEY_YIELD_URL = (
    "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/ClsYldCurvHis"
)
CHINABOND_YIELD_URL = "https://yield.chinabond.com.cn/cbweb-mn/pgxh/historyQuery"
SSE_DAILY_URL = "https://query.sse.com.cn/commonQuery.do"
SZSE_DAILY_URL = "http://www.szse.cn/api/report/ShowReport"
BSE_DAILY_PAGE_URL = "https://www.bse.cn/static/statisticdata.html"
BSE_DAILY_API_PATH = "/marketStatController/dailyReport.do"
PBC_STATS_ROOT_URL = (
    "https://www.pbc.gov.cn/diaochatongjisi/116219/116319/index.html"
)
NBS_RELEASE_ROOT_URL = "https://www.stats.gov.cn/sj/zxfb/"
LEGULEGU_MACRO_PAGE_URL = "https://legulegu.com/stockdata/marketcap-gdp"
LEGULEGU_MACRO_API_URL = (
    "https://legulegu.com/api/stockdata/marketcap-gdp/get-marketcap-gdp"
)
INDEX_SERIES = {
    "000300": "沪深300",
    "000852": "中证1000",
}
DEFAULT_START_DATE = date(2005, 4, 8)
BSE_OPEN_DATE = date(2021, 11, 15)
OFFICIAL_MARKET_CAP_START_DATE = date(2022, 1, 4)
BOND_HISTORY_START_DATE = date(2006, 3, 1)
CSI1000_LAUNCH_DATE = date(2014, 10, 17)
DEFAULT_AGGREGATE_MARKET_CAP_ADJUSTMENT = 0.886497


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    for pattern in ("%Y-%m-%d", "%Y%m%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value}")


def direct_session():
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/138 Safari/537.36"
            )
        }
    )
    return session


def parse_number(value):
    if value is None:
        return None
    text = str(value).replace(",", "").replace("%", "").strip()
    if not text or text.lower() in {"nan", "none", "--", "-"}:
        return None
    try:
        result = float(text)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def parse_csindex_payload(payload):
    rows = []
    for record in (payload or {}).get("data") or []:
        index_code = str(record.get("indexCode") or "").strip()
        if index_code not in INDEX_SERIES:
            continue
        try:
            trade_date = parse_date(record.get("tradeDate")).isoformat()
        except ValueError:
            continue
        pe_ttm = parse_number(record.get("peg"))
        if pe_ttm is None or pe_ttm <= 0:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "index_code": index_code,
                "index_name": INDEX_SERIES[index_code],
                "pe_ttm": pe_ttm,
                "earnings_yield_pct": 100.0 / pe_ttm,
                "data_source": "csindex_official",
                "source_url": CSI_PERF_URL,
                "raw_json": record,
            }
        )
    return rows


def fetch_csindex_rows_sync(start_date, end_date):
    start = parse_date(start_date)
    end = parse_date(end_date)
    session = direct_session()
    rows = []
    for index_code in INDEX_SERIES:
        response = session.get(
            CSI_PERF_URL,
            params={
                "indexCode": index_code,
                "startDate": start.strftime("%Y%m%d"),
                "endDate": end.strftime("%Y%m%d"),
            },
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(
                f"CSI valuation request failed for {index_code}: {payload.get('msg')}"
            )
        rows.extend(parse_csindex_payload(payload))
    return rows


def parse_chinamoney_yield_payload(payload):
    rows = []
    for record in (payload or {}).get("records") or []:
        if abs((parse_number(record.get("yearTermStr")) or -1) - 10.0) > 1e-8:
            continue
        try:
            trade_date = parse_date(record.get("newDateValueCN")).isoformat()
        except ValueError:
            continue
        maturity_yield_pct = parse_number(record.get("maturityYieldStr"))
        if maturity_yield_pct is None or maturity_yield_pct < 0:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "tenor_years": 10,
                "maturity_yield_pct": maturity_yield_pct,
                "spot_yield_pct": parse_number(record.get("currentYieldStr")),
                "forward_yield_pct": parse_number(record.get("futureYieldStr")),
                "data_source": "chinamoney_official",
                "source_url": CHINAMONEY_YIELD_URL,
                "raw_json": record,
            }
        )
    return rows


def fetch_chinamoney_rows_sync(start_date, end_date):
    start = parse_date(start_date)
    end = parse_date(end_date)
    session = direct_session()
    response = session.post(
        CHINABOND_YIELD_URL,
        params={
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "gjqx": "10",
            "locale": "",
        },
        headers={"Referer": "https://yield.chinabond.com.cn/cbweb-mn/pgxh/showHistory"},
        timeout=60,
    )
    response.raise_for_status()
    rows = []
    for record in response.json() or []:
        maturity_yield_pct = parse_number(record.get("tenYear"))
        if maturity_yield_pct is None or maturity_yield_pct < 0:
            continue
        try:
            trade_date = parse_date(record.get("workTime")).isoformat()
        except ValueError:
            continue
        rows.append(
            {
                "trade_date": trade_date,
                "tenor_years": 10,
                "maturity_yield_pct": maturity_yield_pct,
                "spot_yield_pct": None,
                "forward_yield_pct": None,
                "data_source": "chinabond_mof_government_curve",
                "source_url": CHINABOND_YIELD_URL,
                "raw_json": record,
            }
        )
    return rows


def parse_sse_daily_payload(payload, trade_date):
    result = (payload or {}).get("result") or []
    if not result:
        return None
    by_product = {
        str(item.get("PRODUCT_CODE") or "").strip(): item
        for item in result
    }
    main_a_row = by_product.get("01")
    star_row = by_product.get("03")
    if not main_a_row or not star_row:
        return None
    main_a = parse_number(main_a_row.get("TOTAL_VALUE"))
    star = parse_number(star_row.get("TOTAL_VALUE"))
    main_a_float = parse_number(main_a_row.get("NEGO_VALUE"))
    star_float = parse_number(star_row.get("NEGO_VALUE"))
    if main_a is None or star is None:
        return None
    return {
        "trade_date": parse_date(trade_date).isoformat(),
        "exchange": "SSE",
        "total_market_cap_cny": (main_a + star) * 100_000_000,
        "circulating_market_cap_cny": (
            (main_a_float + star_float) * 100_000_000
            if main_a_float is not None and star_float is not None
            else None
        ),
        "data_source": "sse_official_daily_overview",
        "source_url": SSE_DAILY_URL,
        "raw_json": result,
    }


def fetch_sse_market_cap_sync(trade_date):
    target = parse_date(trade_date)
    session = direct_session()
    response = session.get(
        SSE_DAILY_URL,
        params={
            "sqlId": "COMMON_SSE_SJ_GPSJ_CJGK_MRGK_C",
            "PRODUCT_CODE": "01,02,03,11,17",
            "type": "inParams",
            "SEARCH_DATE": target.isoformat(),
        },
        headers={"Referer": "https://www.sse.com.cn/"},
        timeout=60,
    )
    response.raise_for_status()
    return parse_sse_daily_payload(response.json(), target)


def parse_szse_summary_frame(frame, trade_date):
    normalized = frame.copy()
    normalized.columns = [str(item).strip() for item in normalized.columns]
    category_column = normalized.columns[0]
    total_column = next(
        (item for item in normalized.columns if str(item).strip().startswith("总市值")),
        None,
    )
    float_column = next(
        (item for item in normalized.columns if str(item).strip().startswith("流通市值")),
        None,
    )
    if total_column is None:
        return None
    categories = normalized[category_column].astype(str).str.strip()
    a_share_mask = categories.isin(["主板A股", "创业板A股"])
    selected = normalized[a_share_mask]
    if selected.empty:
        return None
    total_market_cap = pd.to_numeric(
        selected[total_column].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).sum(min_count=1)
    circulating_market_cap = (
        pd.to_numeric(
            selected[float_column].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        ).sum(min_count=1)
        if float_column is not None
        else None
    )
    if pd.isna(total_market_cap):
        return None
    return {
        "trade_date": parse_date(trade_date).isoformat(),
        "exchange": "SZSE",
        "total_market_cap_cny": float(total_market_cap),
        "circulating_market_cap_cny": (
            None if circulating_market_cap is None or pd.isna(circulating_market_cap)
            else float(circulating_market_cap)
        ),
        "data_source": "szse_official_daily_overview",
        "source_url": SZSE_DAILY_URL,
        "raw_json": selected.to_dict(orient="records"),
    }


def fetch_szse_market_cap_sync(trade_date):
    target = parse_date(trade_date)
    session = direct_session()
    response = session.get(
        SZSE_DAILY_URL,
        params={
            "SHOWTYPE": "xlsx",
            "CATALOGID": "1803_sczm",
            "TABKEY": "tab1",
            "txtQueryDate": target.isoformat(),
            "random": "0.39339437497296137",
        },
        timeout=60,
    )
    response.raise_for_status()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style",
            category=UserWarning,
        )
        frame = pd.read_excel(BytesIO(response.content), engine="openpyxl")
    return parse_szse_summary_frame(frame, target)


def parse_jsonp(text):
    payload = str(text or "").strip()
    start = payload.find("(")
    end = payload.rfind(")")
    if start >= 0 and end > start:
        payload = payload[start + 1:end]
    return json.loads(payload)


def parse_bse_daily_records(records, trade_date):
    summary = next(
        (
            item for item in (records or [])
            if str(item.get("xxzrlx") or "").strip() == "2"
        ),
        None,
    )
    if not summary:
        return None
    total_market_cap = parse_number(summary.get("zsz"))
    if total_market_cap is None or total_market_cap <= 0:
        return None
    return {
        "trade_date": parse_date(trade_date).isoformat(),
        "exchange": "BSE",
        "total_market_cap_cny": total_market_cap,
        "circulating_market_cap_cny": parse_number(summary.get("ltsz")),
        "data_source": "bse_official_daily_overview",
        "source_url": BSE_DAILY_PAGE_URL,
        "raw_json": records,
    }


def fetch_bse_market_caps_sync(trade_dates):
    targets = [parse_date(item) for item in trade_dates]
    targets = [item for item in targets if item >= BSE_OPEN_DATE]
    if not targets:
        return []
    rows = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(BSE_DAILY_PAGE_URL, wait_until="networkidle", timeout=60_000)
        for target in targets:
            text = page.evaluate(
                """
                async ({ path, dateValue }) => {
                  const callback = `macroCallback${Date.now()}`;
                  const body = new URLSearchParams({ HQJSRQ: dateValue });
                  const response = await fetch(`${path}?callback=${callback}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
                    body: body.toString(),
                  });
                  return await response.text();
                }
                """,
                {
                    "path": BSE_DAILY_API_PATH,
                    "dateValue": target.strftime("%Y%m%d"),
                },
            )
            parsed = parse_bse_daily_records(parse_jsonp(text), target)
            if parsed:
                rows.append(parsed)
            time.sleep(0.15)
        browser.close()
    return rows


def fetch_legulegu_macro_rows_sync(start_date, end_date):
    start = parse_date(start_date)
    end = parse_date(end_date)
    last_error = None
    for attempt in range(3):
        try:
            session = direct_session()
            page_response = session.get(LEGULEGU_MACRO_PAGE_URL, timeout=60)
            page_response.raise_for_status()
            soup = BeautifulSoup(page_response.text, "lxml")
            csrf_tag = soup.find("meta", attrs={"name": "_csrf"})
            headers = {"Referer": LEGULEGU_MACRO_PAGE_URL}
            if csrf_tag and csrf_tag.get("content"):
                headers["X-CSRF-Token"] = str(csrf_tag.get("content"))
            response = session.get(
                LEGULEGU_MACRO_API_URL,
                params={"token": get_token_lg()},
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            rows = []
            for record in (response.json() or {}).get("data") or []:
                try:
                    trade_date = parse_date(record.get("date"))
                except ValueError:
                    continue
                # A subset of the long series is stamped on Sunday although
                # the observation belongs to the preceding Friday session.
                # Keep the source date in raw_json and normalize only the key.
                if trade_date.weekday() == 6:
                    trade_date -= timedelta(days=2)
                if trade_date < start or trade_date > end:
                    continue
                market_cap_100m = parse_number(record.get("marketCap"))
                gdp_100m = parse_number(record.get("gdp"))
                if market_cap_100m is None or market_cap_100m <= 0:
                    continue
                rows.append(
                    {
                        "trade_date": trade_date.isoformat(),
                        "exchange": "A_AGGREGATE",
                        "total_market_cap_cny": market_cap_100m * 100_000_000,
                        "circulating_market_cap_cny": None,
                        "reference_gdp_cny": (
                            gdp_100m * 100_000_000
                            if gdp_100m is not None and gdp_100m > 0 else None
                        ),
                        "data_source": "legulegu_marketcap_gdp",
                        "source_url": LEGULEGU_MACRO_PAGE_URL,
                        "raw_json": record,
                    }
                )
            return rows
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(1.0 + attempt)
    raise RuntimeError(f"failed to fetch long-term A-share market cap: {last_error}")


def find_pbc_credit_pages_sync(min_year, max_year):
    session = direct_session()
    response = session.get(PBC_STATS_ROOT_URL, timeout=60)
    response.raise_for_status()
    response.encoding = response.apparent_encoding
    soup = BeautifulSoup(response.text, "lxml")
    result = {}
    year_pages = {}
    for link in soup.find_all("a", href=True):
        href = str(link.get("href") or "")
        text_value = "".join(link.stripped_strings)
        year_match = re.search(r"/(20\d{2})ntjsj/", href)
        if year_match:
            year = int(year_match.group(1))
        else:
            parent = link.find_parent()
            context = parent.get_text(" ", strip=True) if parent else ""
            context_match = re.search(r"(20\d{2})年", context)
            if not context_match:
                continue
            year = int(context_match.group(1))
        if not (min_year <= year <= max_year):
            continue
        absolute_url = urljoin(PBC_STATS_ROOT_URL, href)
        if "金融机构信贷收支统计" in text_value:
            result[year] = absolute_url
        elif "统计数据" in text_value or re.search(r"20\d{2}ntjsj", href):
            year_pages[year] = absolute_url

    for year, year_url in sorted(year_pages.items()):
        if year in result:
            continue
        year_response = session.get(year_url, timeout=60)
        year_response.raise_for_status()
        year_response.encoding = year_response.apparent_encoding
        year_soup = BeautifulSoup(year_response.text, "lxml")
        target_link = next(
            (
                link for link in year_soup.find_all("a", href=True)
                if "金融机构信贷收支统计" in "".join(link.stripped_strings)
            ),
            None,
        )
        if target_link:
            result[year] = urljoin(year_url, target_link.get("href"))
        time.sleep(0.08)
    return result


def parse_pbc_household_deposit_table(html, source_url, source_updated_at=None):
    tables = pd.read_html(StringIO(html))
    table = next(
        (
            item for item in tables
            if item.shape[1] >= 2
            and item.iloc[:, 0].astype(str).str.contains(
                r"住户存款|储蓄存款", regex=True, na=False
            ).any()
        ),
        None,
    )
    if table is None:
        return []
    label_column = table.iloc[:, 0].astype(str)
    header_index = next(
        (
            index for index, value in label_column.items()
            if "项目" in value and "Item" in value
        ),
        None,
    )
    modern_household_index = next(
        (index for index, value in label_column.items() if "住户存款" in value),
        None,
    )
    legacy_household_index = next(
        (
            index for index, value in label_column.items()
            if "储蓄存款" in str(value)
            and "活期" not in str(value)
            and "定期" not in str(value)
        ),
        None,
    )
    household_index = (
        modern_household_index
        if modern_household_index is not None else legacy_household_index
    )
    is_legacy = modern_household_index is None and legacy_household_index is not None
    demand_index = next(
        (
            index for index, value in label_column.items()
            if ("活期储蓄" if is_legacy else "活期存款") in value
        ),
        None,
    )
    time_index = next(
        (
            index for index, value in label_column.items()
            if ("定期储蓄" if is_legacy else "定期及其他存款") in value
        ),
        None,
    )
    if header_index is None or household_index is None:
        return []
    rows = []
    for column in range(1, table.shape[1]):
        period_value = str(table.iloc[header_index, column]).strip()
        match = re.fullmatch(r"(20\d{2})\.(0[1-9]|1[0-2])", period_value)
        if not match:
            continue
        year = int(match.group(1))
        month = int(match.group(2))
        period_end = (
            date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
            - timedelta(days=1)
        )
        balance_100m = parse_number(table.iloc[household_index, column])
        if balance_100m is None:
            continue
        demand_100m = (
            parse_number(table.iloc[demand_index, column])
            if demand_index is not None
            else None
        )
        time_100m = (
            parse_number(table.iloc[time_index, column])
            if time_index is not None
            else None
        )
        rows.append(
            {
                "period_end": period_end.isoformat(),
                "household_deposit_cny": balance_100m * 100_000_000,
                "demand_deposit_cny": (
                    demand_100m * 100_000_000 if demand_100m is not None else None
                ),
                "time_other_deposit_cny": (
                    time_100m * 100_000_000 if time_100m is not None else None
                ),
                "source_updated_at": source_updated_at,
                "data_source": (
                    "pbc_legacy_savings_deposit"
                    if is_legacy else "pbc_rmb_credit_balance"
                ),
                "source_url": source_url,
                "raw_json": {
                    "period": period_value,
                    "household_deposit_100m_cny": balance_100m,
                    "demand_deposit_100m_cny": demand_100m,
                    "time_other_deposit_100m_cny": time_100m,
                },
            }
        )
    return rows


def fetch_pbc_household_deposit_rows_sync(min_year, max_year):
    session = direct_session()
    category_pages = find_pbc_credit_pages_sync(min_year, max_year)
    rows = []
    for year, page_url in sorted(category_pages.items()):
        response = session.get(page_url, timeout=60)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "lxml")
        source_updated = None
        meta = soup.find("meta", attrs={"name": "createDate"})
        if meta and meta.get("content"):
            source_updated = str(meta.get("content")).strip()
        source_url = None
        for table in soup.select("table.a2015"):
            title = table.get_text(" ", strip=True)
            if "金融机构人民币信贷收支表" not in title or "存款类" in title:
                continue
            link = table.find("a", string=lambda value: value and value.strip().lower() == "htm")
            if link and link.get("href"):
                source_url = urljoin(page_url, link.get("href"))
                break
        if not source_url:
            legacy_link = next(
                (
                    link for link in soup.find_all("a", href=True)
                    if "金融机构人民币信贷收支表" in "".join(link.stripped_strings)
                    and "按部门" not in "".join(link.stripped_strings)
                ),
                None,
            )
            if legacy_link:
                source_url = urljoin(page_url, legacy_link.get("href"))
        if not source_url:
            continue
        detail = None
        last_error = None
        for attempt in range(3):
            try:
                detail = session.get(source_url, timeout=60)
                detail.raise_for_status()
                detail.encoding = detail.apparent_encoding
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                detail = None
                time.sleep(0.8 + attempt)
        if detail is None:
            print(f"macro PBC {year} skipped after retries: {last_error}")
            continue
        rows.extend(
            parse_pbc_household_deposit_table(
                detail.text,
                source_url,
                source_updated_at=source_updated,
            )
        )
        time.sleep(0.15)
    return rows


QUARTER_LABELS = {
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
}


def parse_nbs_gdp_page(html, source_url):
    soup = BeautifulSoup(html, "lxml")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    match = re.search(r"(20\d{2})年(?:第?([一二三四1234])|([一二三四1234]))季度", title)
    if not match:
        return None
    year = int(match.group(1))
    quarter = QUARTER_LABELS.get(match.group(2) or match.group(3))
    if not quarter:
        return None
    tables = pd.read_html(StringIO(html))
    gdp_table = next(
        (
            item for item in tables
            if item.shape[1] >= 2
            and item.iloc[:, 0].astype(str).str.fullmatch(r"GDP|国内生产总值", na=False).any()
        ),
        None,
    )
    if gdp_table is None:
        return None
    label_values = gdp_table.iloc[:, 0].astype(str)
    gdp_index = next(
        (
            index for index, value in label_values.items()
            if re.fullmatch(r"GDP|国内生产总值", value.strip())
        ),
        None,
    )
    if gdp_index is None:
        return None
    numeric_values = [
        parse_number(gdp_table.iloc[gdp_index, column])
        for column in range(1, gdp_table.shape[1])
    ]
    numeric_values = [value for value in numeric_values if value is not None]
    if not numeric_values:
        return None
    nominal_100m = numeric_values[0]
    cumulative_100m = (
        numeric_values[1] if quarter > 1 and len(numeric_values) >= 2 else nominal_100m
    )
    release_match = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", soup.get_text(" ", strip=True))
    release_date = (
        date(int(release_match.group(1)), int(release_match.group(2)), int(release_match.group(3))).isoformat()
        if release_match
        else None
    )
    period_end = date(year, quarter * 3, 1)
    period_end = (
        date(year + (1 if period_end.month == 12 else 0), 1 if period_end.month == 12 else period_end.month + 1, 1)
        - timedelta(days=1)
    )
    return {
        "period_end": period_end.isoformat(),
        "year": year,
        "quarter": quarter,
        "nominal_gdp_cny": nominal_100m * 100_000_000,
        "cumulative_gdp_cny": cumulative_100m * 100_000_000,
        "release_date": release_date,
        "data_source": "nbs_quarterly_gdp_release",
        "source_url": source_url,
        "raw_json": {
            "title": title,
            "nominal_gdp_100m_cny": nominal_100m,
            "cumulative_gdp_100m_cny": cumulative_100m,
        },
    }


def fetch_nbs_gdp_rows_sync(min_year, max_pages=180):
    session = direct_session()
    page_urls = []
    seen = set()
    for page_number in range(max_pages):
        list_url = (
            NBS_RELEASE_ROOT_URL
            if page_number == 0
            else urljoin(NBS_RELEASE_ROOT_URL, f"index_{page_number}.html")
        )
        response = session.get(list_url, timeout=60)
        if response.status_code == 404:
            break
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "lxml")
        found_old_year = False
        for link in soup.find_all("a", href=True):
            title = str(link.get("title") or link.get_text(" ", strip=True))
            year_match = re.search(r"(20\d{2})年", title)
            if year_match and int(year_match.group(1)) < min_year:
                found_old_year = True
            if "国内生产总值" not in title or "初步核算结果" not in title:
                continue
            href = urljoin(list_url, link.get("href"))
            if href not in seen:
                seen.add(href)
                page_urls.append(href)
        if found_old_year and page_number > 6:
            break
        time.sleep(0.08)
    rows = []
    for page_url in page_urls:
        response = session.get(page_url, timeout=60)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        parsed = parse_nbs_gdp_page(response.text, page_url)
        if parsed and parsed["year"] >= min_year:
            rows.append(parsed)
        time.sleep(0.08)
    deduped = {row["period_end"]: row for row in rows}
    return [deduped[key] for key in sorted(deduped)]


def latest_on_or_before(sorted_dates, mapping, target):
    index = bisect_right(sorted_dates, target) - 1
    return mapping[sorted_dates[index]] if index >= 0 else None


def latest_released_gdp(gdp_rows, target):
    eligible = []
    for row in gdp_rows:
        period_end = parse_date(row["period_end"])
        release_date = parse_date(row.get("release_date") or period_end)
        if release_date <= target:
            eligible.append((period_end, row))
    return max(eligible, key=lambda item: item[0])[1] if eligible else None


def latest_available_deposit(deposit_rows, target):
    eligible = []
    for row in deposit_rows:
        period_end = parse_date(row["period_end"])
        # PBOC monthly balance sheets are released after month end. The yearly
        # page only exposes one page-update timestamp, so use a conservative
        # fixed publication lag instead of leaking future month-end values.
        available_date = period_end + timedelta(days=20)
        if available_date <= target:
            eligible.append((period_end, row))
    return max(eligible, key=lambda item: item[0])[1] if eligible else None


def interpolate_short_aggregate_market_cap_gaps(trade_dates, market_caps, max_gap=5):
    targets = sorted({parse_date(item) for item in trade_dates})
    target_indexes = {target: index for index, target in enumerate(targets)}
    known_values = {}
    for target in targets:
        aggregate = market_caps.get(target, {}).get("A_AGGREGATE")
        value = (
            parse_number(aggregate.get("total_market_cap_cny"))
            if aggregate else None
        )
        if value is not None and value > 0:
            known_values[target] = value

    known_dates = sorted(known_values)
    interpolated = {}
    for previous, following in zip(known_dates, known_dates[1:]):
        previous_index = target_indexes[previous]
        following_index = target_indexes[following]
        missing_count = following_index - previous_index - 1
        if missing_count <= 0 or missing_count > max_gap:
            continue
        previous_value = known_values[previous]
        following_value = known_values[following]
        for index in range(previous_index + 1, following_index):
            target = targets[index]
            if target in known_values:
                continue
            progress = (index - previous_index) / (following_index - previous_index)
            interpolated[target] = previous_value + (
                following_value - previous_value
            ) * progress
    return interpolated


def build_macro_indicator_rows(
    trade_dates,
    valuation_rows,
    yield_rows,
    market_cap_rows,
    gdp_rows,
    deposit_rows,
):
    valuations = {
        (parse_date(row["trade_date"]), str(row["index_code"])): row
        for row in valuation_rows
    }
    yields = {parse_date(row["trade_date"]): row for row in yield_rows}
    market_caps = {}
    for row in market_cap_rows:
        trade_date = parse_date(row["trade_date"])
        market_caps.setdefault(trade_date, {})[str(row["exchange"])] = row
    overlap_ratios = []
    for exchange_rows in market_caps.values():
        aggregate = exchange_rows.get("A_AGGREGATE")
        if not aggregate or not {"SSE", "SZSE", "BSE"}.issubset(exchange_rows):
            continue
        official_values = [
            parse_number(exchange_rows[key].get("total_market_cap_cny"))
            for key in ("SSE", "SZSE", "BSE")
        ]
        aggregate_value = parse_number(aggregate.get("total_market_cap_cny"))
        if all(value is not None and value > 0 for value in official_values) \
                and aggregate_value is not None and aggregate_value > 0:
            overlap_ratios.append(sum(official_values) / aggregate_value)
    aggregate_adjustment = (
        median(overlap_ratios)
        if overlap_ratios else DEFAULT_AGGREGATE_MARKET_CAP_ADJUSTMENT
    )
    interpolated_aggregate_values = interpolate_short_aggregate_market_cap_gaps(
        trade_dates, market_caps
    )
    gdp_map = {parse_date(row["period_end"]): row for row in gdp_rows}
    gdp_dates = sorted(gdp_map)
    deposit_map = {parse_date(row["period_end"]): row for row in deposit_rows}
    deposit_dates = sorted(deposit_map)
    results = []
    for target in sorted({parse_date(item) for item in trade_dates}):
        hs300 = valuations.get((target, "000300"))
        csi1000 = valuations.get((target, "000852"))
        bond = yields.get(target)
        exchange_rows = market_caps.get(target, {})
        required_exchanges = {"SSE", "SZSE"}
        if target >= BSE_OPEN_DATE:
            required_exchanges.add("BSE")
        total_market_cap = None
        market_cap_source = None
        market_cap_adjustment_factor = None
        if required_exchanges.issubset(exchange_rows):
            values = [
                parse_number(exchange_rows[key].get("total_market_cap_cny"))
                for key in sorted(required_exchanges)
            ]
            if all(value is not None for value in values):
                total_market_cap = sum(values)
                market_cap_source = "exchange_official"
                market_cap_adjustment_factor = 1.0
        if total_market_cap is None:
            aggregate_row = exchange_rows.get("A_AGGREGATE")
            aggregate_value = (
                parse_number(aggregate_row.get("total_market_cap_cny"))
                if aggregate_row else None
            )
            aggregate_interpolated = False
            if aggregate_value is None:
                aggregate_value = interpolated_aggregate_values.get(target)
                aggregate_interpolated = aggregate_value is not None
            if aggregate_value is not None and aggregate_value > 0:
                total_market_cap = aggregate_value * aggregate_adjustment
                market_cap_source = (
                    "legulegu_interpolated_adjusted_to_exchange_official"
                    if aggregate_interpolated
                    else "legulegu_adjusted_to_exchange_official"
                )
                market_cap_adjustment_factor = aggregate_adjustment
        latest_gdp = latest_released_gdp(gdp_rows, target)
        latest_deposit = latest_available_deposit(deposit_rows, target)
        trailing_gdp = None
        gdp_source = None
        gdp_period_end = None
        if latest_gdp:
            gdp_period_end = latest_gdp.get("period_end")
            latest_period = parse_date(latest_gdp["period_end"])
            latest_index = gdp_dates.index(latest_period)
            periods = gdp_dates[max(0, latest_index - 3): latest_index + 1]
            if len(periods) == 4:
                gdp_values = [
                    parse_number(gdp_map[period].get("nominal_gdp_cny"))
                    for period in periods
                ]
                if all(value is not None for value in gdp_values):
                    trailing_gdp = sum(gdp_values)
                    gdp_source = "nbs_trailing_4q"
        if trailing_gdp is None:
            aggregate_row = exchange_rows.get("A_AGGREGATE")
            reference_gdp = (
                parse_number(aggregate_row.get("reference_gdp_cny"))
                if aggregate_row else None
            )
            if reference_gdp is not None and reference_gdp > 0:
                trailing_gdp = reference_gdp
                gdp_source = "legulegu_nbs_annual_reference"
                gdp_period_end = date(target.year - 1, 12, 31).isoformat()
        hs300_pe = parse_number(hs300.get("pe_ttm")) if hs300 else None
        csi1000_pe = (
            parse_number(csi1000.get("pe_ttm"))
            if csi1000 and target >= CSI1000_LAUNCH_DATE else None
        )
        bond_yield = parse_number(bond.get("maturity_yield_pct")) if bond else None
        household_deposit = (
            parse_number(latest_deposit.get("household_deposit_cny"))
            if latest_deposit else None
        )
        results.append(
            {
                "trade_date": target.isoformat(),
                "hs300_pe_ttm": hs300_pe,
                "csi1000_pe_ttm": csi1000_pe,
                "cn_gov_bond_10y_yield_pct": bond_yield,
                "a_share_total_market_cap_cny": total_market_cap,
                "trailing_4q_nominal_gdp_cny": trailing_gdp,
                "household_deposit_cny": household_deposit,
                "hs300_equity_bond_spread_pp": (
                    100.0 / hs300_pe - bond_yield
                    if hs300_pe and bond_yield is not None else None
                ),
                "csi1000_equity_bond_spread_pp": (
                    100.0 / csi1000_pe - bond_yield
                    if csi1000_pe and bond_yield is not None else None
                ),
                "buffett_indicator_pct": (
                    total_market_cap / trailing_gdp * 100.0
                    if total_market_cap and trailing_gdp else None
                ),
                "household_deposit_market_cap_ratio_pct": (
                    household_deposit / total_market_cap * 100.0
                    if household_deposit and total_market_cap else None
                ),
                "gdp_period_end": gdp_period_end,
                "deposit_period_end": latest_deposit.get("period_end") if latest_deposit else None,
                "market_cap_source": market_cap_source,
                "market_cap_adjustment_factor": market_cap_adjustment_factor,
                "gdp_source": gdp_source,
                "data_source": "official_sources_derived",
            }
        )
    return results


async def collect_market_caps(trade_dates, progress_label=None):
    dates = list(trade_dates)

    async def collect_exchange(handler, exchange):
        rows = []
        for index, trade_date in enumerate(dates, start=1):
            row = await asyncio.to_thread(handler, trade_date)
            if row:
                rows.append(row)
            if progress_label and (index % 20 == 0 or index == len(dates)):
                print(
                    f"{progress_label} {exchange}: {index}/{len(dates)}, "
                    f"rows={len(rows)}"
                )
            await asyncio.sleep(0.12)
        return rows

    sse_rows, szse_rows, bse_rows = await asyncio.gather(
        collect_exchange(fetch_sse_market_cap_sync, "SSE"),
        collect_exchange(fetch_szse_market_cap_sync, "SZSE"),
        asyncio.to_thread(fetch_bse_market_caps_sync, dates),
    )
    results = [*sse_rows, *szse_rows, *bse_rows]
    return results


async def refresh_derived(db, start_date, end_date):
    source = await db.get_cn_macro_source_rows(start_date, end_date)
    indicator_rows = build_macro_indicator_rows(
        source["trade_dates"],
        source["valuations"],
        source["yields"],
        source["market_caps"],
        source["gdp"],
        source["deposits"],
    )
    upserted = await db.upsert_cn_macro_indicator_daily(indicator_rows)
    return {"rows": upserted, "calculated": len(indicator_rows), "items": indicator_rows}


async def sync_daily(target_date=None):
    target = parse_date(target_date or date.today())
    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_cn_macro_tables()
        trade_dates = await db.get_cn_trade_dates(target - timedelta(days=18), target)
        if target not in trade_dates:
            trade_dates.append(target)
        trade_dates = sorted(set(trade_dates))[-10:]
        start = min(trade_dates)

        valuation_rows, yield_rows, aggregate_market_cap_rows = await asyncio.gather(
            asyncio.to_thread(fetch_csindex_rows_sync, start, target),
            asyncio.to_thread(fetch_chinamoney_rows_sync, start, target),
            asyncio.to_thread(fetch_legulegu_macro_rows_sync, start, target),
        )
        market_cap_rows = await collect_market_caps(trade_dates)
        market_cap_rows.extend(aggregate_market_cap_rows)
        deposit_rows, gdp_rows = await asyncio.gather(
            asyncio.to_thread(
                fetch_pbc_household_deposit_rows_sync,
                max(DEFAULT_START_DATE.year, target.year - 1),
                target.year,
            ),
            asyncio.to_thread(fetch_nbs_gdp_rows_sync, max(DEFAULT_START_DATE.year, target.year - 3), 45),
        )

        inserted = {
            "valuations": await db.upsert_cn_index_valuation_daily(valuation_rows),
            "yields": await db.upsert_cn_government_bond_yield_daily(yield_rows),
            "market_caps": await db.upsert_cn_stock_market_cap_daily(market_cap_rows),
            "gdp": await db.upsert_cn_gdp_quarterly(gdp_rows),
            "deposits": await db.upsert_cn_household_deposit_monthly(deposit_rows),
        }
        derived = await refresh_derived(db, start, target)
        target_row = next(
            (row for row in derived["items"] if row["trade_date"] == target.isoformat()),
            None,
        )
        required_fields = (
            "hs300_equity_bond_spread_pp",
            "csi1000_equity_bond_spread_pp",
            "buffett_indicator_pct",
            "household_deposit_market_cap_ratio_pct",
        )
        missing_fields = [
            field for field in required_fields
            if target_row is None or target_row.get(field) is None
        ]
        if missing_fields:
            raise RuntimeError(
                f"macro target date {target.isoformat()} is incomplete: "
                + ", ".join(missing_fields)
            )
        derived.pop("items", None)
        return {
            "status": "SUCCESS",
            "target_date": target.isoformat(),
            "recent_start_date": start.isoformat(),
            "source_rows": inserted,
            "indicator_rows": derived,
        }
    finally:
        await db.close()


async def backfill(start_date=DEFAULT_START_DATE, end_date=None):
    start = parse_date(start_date)
    end = parse_date(end_date or date.today())
    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_cn_macro_tables()
        trade_dates = await db.get_cn_trade_dates(start, end)
        if not trade_dates:
            raise RuntimeError("no A-share trade dates available for macro backfill")

        valuation_count = 0
        for year in range(start.year, end.year + 1):
            range_start = max(start, date(year, 1, 1))
            range_end = min(end, date(year, 12, 31))
            year_rows = await asyncio.to_thread(
                fetch_csindex_rows_sync, range_start, range_end
            )
            valuation_count += await db.upsert_cn_index_valuation_daily(year_rows)
            print(f"macro backfill valuation {year}: rows={len(year_rows)}")
            await asyncio.sleep(0.3)

        yield_count = 0
        bond_start = max(start, BOND_HISTORY_START_DATE)
        cursor = date(bond_start.year, bond_start.month, 1)
        while cursor <= end:
            next_month = (
                date(cursor.year + 1, 1, 1)
                if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
            )
            range_start = max(bond_start, cursor)
            range_end = min(end, next_month - timedelta(days=1))
            month_rows = await asyncio.to_thread(
                fetch_chinamoney_rows_sync, range_start, range_end
            )
            yield_count += await db.upsert_cn_government_bond_yield_daily(month_rows)
            print(
                f"macro backfill bond {range_start:%Y-%m}: rows={len(month_rows)}"
            )
            cursor = next_month
            await asyncio.sleep(0.2)

        aggregate_market_cap_rows = await asyncio.to_thread(
            fetch_legulegu_macro_rows_sync, start, end
        )
        market_cap_count = await db.upsert_cn_stock_market_cap_daily(
            aggregate_market_cap_rows
        )
        official_trade_dates = [
            item for item in trade_dates if item >= OFFICIAL_MARKET_CAP_START_DATE
        ]
        complete_dates = await db.get_cn_official_market_cap_complete_dates(
            OFFICIAL_MARKET_CAP_START_DATE, end
        )
        missing_official_dates = [
            item for item in official_trade_dates if item not in complete_dates
        ]
        for offset in range(0, len(missing_official_dates), 20):
            chunk = missing_official_dates[offset: offset + 20]
            market_cap_rows = await collect_market_caps(
                chunk,
                progress_label=(
                    f"macro backfill market-cap "
                    f"{offset + 1}-{min(offset + len(chunk), len(missing_official_dates))}"
                ),
            )
            market_cap_count += await db.upsert_cn_stock_market_cap_daily(
                market_cap_rows
            )

        deposit_rows, gdp_rows = await asyncio.gather(
            asyncio.to_thread(fetch_pbc_household_deposit_rows_sync, start.year, end.year),
            asyncio.to_thread(fetch_nbs_gdp_rows_sync, start.year, 220),
        )
        await db.upsert_cn_household_deposit_monthly(deposit_rows)
        await db.upsert_cn_gdp_quarterly(gdp_rows)
        derived = await refresh_derived(db, start, end)
        derived.pop("items", None)
        return {
            "status": "SUCCESS",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "trade_dates": len(trade_dates),
            "source_rows": {
                "valuations": valuation_count,
                "yields": yield_count,
                "market_caps": market_cap_count,
                "aggregate_market_caps": len(aggregate_market_cap_rows),
                "official_market_cap_dates_requested": len(missing_official_dates),
                "gdp": len(gdp_rows),
                "deposits": len(deposit_rows),
            },
            "indicator_rows": derived,
        }
    finally:
        await db.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "daily"
    args = sys.argv[2:]
    if command == "daily":
        print(await sync_daily(target_date=args[0] if args else None))
        return
    if command == "backfill":
        print(
            await backfill(
                start_date=args[0] if args else DEFAULT_START_DATE,
                end_date=args[1] if len(args) > 1 else None,
            )
        )
        return
    raise ValueError("macro supports: daily [date] | backfill [start] [end]")


if __name__ == "__main__":
    asyncio.run(main())
