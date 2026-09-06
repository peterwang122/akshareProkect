import asyncio
import calendar
import hashlib
import json
import math
import re
import sys
import time
from bisect import bisect_left
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.paths import get_state_path
from akshare_project.db.db_tool import DbTools


LOGGER = get_logger("bank_liquidity")
HISTORY_START = date(2017, 5, 31)
SHANGHAI_AVAILABLE_TIMES = {
    "frr": "11:30:00",
    "chinabond": "17:30:00",
    "closing_repo": "22:00:00",
}
CHINAMONEY_FRR_URL = "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-currency/FrrHis"
CHINAMONEY_FRR_PAGE = "https://www.chinamoney.com.cn/chinese/bkfrr/"
CHINAMONEY_CLOSING_REPO_URL = "https://www.chinamoney.com.cn/ags/ms/cm-u-dlrp/PrDlyBltn"
CHINAMONEY_CLOSING_REPO_PAGE = "https://www.chinamoney.com.cn/chinese/mtdexdaily/?tab=2"
CHINABOND_HISTORY_URL = "https://yield.chinabond.com.cn/cbweb-cbrc-web/cbrc/historyQuery"
CHINABOND_PAGE = "https://yield.chinabond.com.cn/cbweb-cbrc-web/cbrc/showCbrc?locale=cn_ZH"
PBC_BASE_URL = "https://www.pbc.gov.cn"
PBC_OMO_LIST_URL = (
    "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/125431/125475/index.html"
)
PBC_OMO_LIST_PATH = "/zhengcehuobisi/125207/125213/125431/125475"
PBC_MONTHLY_LIST_URL = (
    "https://www.pbc.gov.cn/zhengcehuobisi/125207/125213/5727710/index.html"
)
PBC_MONTHLY_PATH = "/zhengcehuobisi/125207/125213/5727710"
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9",
}
CORE_FACTOR_FIELDS = (
    "factor_fdr007_policy_spread_bp",
    "factor_overnight_pressure_bp",
    "factor_nonbank_layering_bp",
    "factor_bank_funding_spread_bp",
)
PERCENTILE_FIELDS = (
    "pct_fdr007_policy_spread",
    "pct_overnight_pressure",
    "pct_nonbank_layering",
    "pct_bank_funding_spread",
)
FACTOR_WEIGHTS = (0.4, 0.2, 0.2, 0.2)
MIN_PERCENTILE_SAMPLES = 252
MAX_PERCENTILE_SAMPLES = 1260
CNY_PER_YI = 100_000_000.0
PROGRESS_PATH = get_state_path("bank_liquidity_history")


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def direct_session():
    session = requests.Session()
    session.trust_env = False
    session.headers.update(HTTP_HEADERS)
    return session


def request_with_retry(method, url, attempts=3, **kwargs):
    transient_errors = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.SSLError,
    )
    for attempt in range(1, attempts + 1):
        try:
            response = direct_session().request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except transient_errors:
            if attempt >= attempts:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError(f"request retry exhausted: {url}")


def decode_pbc_html(response):
    return response.content.decode("utf-8", errors="replace")


def parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    for pattern in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date: {value}")


def parse_datetime(value, fallback_date=None, fallback_time="09:30:00"):
    text = str(value or "").strip()
    if text:
        text = text.replace("年", "-").replace("月", "-").replace("日", " ")
        text = re.sub(r"\s+", " ", text).strip()
        for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, pattern)
                if pattern == "%Y-%m-%d":
                    return datetime.combine(parsed.date(), datetime.strptime(fallback_time, "%H:%M:%S").time())
                return parsed
            except ValueError:
                continue
    if fallback_date is None:
        return None
    fallback = parse_date(fallback_date)
    return datetime.combine(fallback, datetime.strptime(fallback_time, "%H:%M:%S").time())


def parse_number(value):
    text = str(value or "").replace(",", "").replace("%", "").strip()
    if text in {"", "-", "--", "—", "–", "N/A", "不适用"}:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group())
    except ValueError:
        return None


def parse_amount_cny(value, default_unit="亿元"):
    text = str(value or "").replace(",", "").strip()
    number = parse_number(text)
    if number is None:
        return None
    if "万亿元" in text:
        return number * 10_000 * CNY_PER_YI
    if "万元" in text and "亿元" not in text:
        return number * 10_000
    if "元" in text and "亿元" not in text and "万元" not in text:
        return number
    if "亿元" in text or default_unit == "亿元":
        return number * CNY_PER_YI
    return number


def clean_text(value):
    return re.sub(r"\s+", "", str(value or "").replace("\xa0", " ")).strip()


def available_at(trade_date, source_key):
    return f"{parse_date(trade_date).isoformat()} {SHANGHAI_AVAILABLE_TIMES[source_key]}"


def midrank_percentile(prior_values, current_value, minimum=MIN_PERCENTILE_SAMPLES):
    if current_value is None:
        return None
    values = [float(value) for value in prior_values if value is not None]
    if len(values) < minimum:
        return None
    current = float(current_value)
    lower = sum(1 for value in values if value < current)
    equal = sum(1 for value in values if value == current)
    return (lower + 0.5 * equal) / len(values) * 100


def liquidity_state(score):
    if score is None:
        return None
    if score < 30:
        return "宽松"
    if score < 60:
        return "平衡"
    if score < 80:
        return "偏紧"
    return "紧张"


def parse_chinamoney_frr_payload(payload):
    records = (payload or {}).get("records") or ((payload or {}).get("data") or {}).get("records") or []
    rows = []
    for record in records:
        value_map = record.get("frValueMap") or {}
        raw_date = record.get("lfiProducDate") or value_map.get("date")
        try:
            trade_date = parse_date(raw_date).isoformat()
        except ValueError:
            continue
        row = {
            "trade_date": trade_date,
            "fr001_pct": parse_number(value_map.get("FR001")),
            "fr007_pct": parse_number(value_map.get("FR007")),
            "fdr001_pct": parse_number(value_map.get("FDR001")),
            "fdr007_pct": parse_number(value_map.get("FDR007")),
            "frr_source_date": trade_date,
            "frr_available_at": available_at(trade_date, "frr"),
            "source_url_frr": CHINAMONEY_FRR_PAGE,
            "raw_frr_json": record,
        }
        if any(row[key] is not None for key in ("fr001_pct", "fr007_pct", "fdr001_pct", "fdr007_pct")):
            rows.append(row)
    return rows


def fetch_chinamoney_frr_rows_sync(start_date, end_date):
    start = parse_date(start_date)
    end = parse_date(end_date)
    response = request_with_retry(
        "GET",
        CHINAMONEY_FRR_URL,
        params={"lang": "CN", "startDate": start.isoformat(), "endDate": end.isoformat()},
        headers={"Referer": CHINAMONEY_FRR_PAGE},
        timeout=60,
    )
    payload = response.json()
    rep_code = str(((payload or {}).get("head") or {}).get("rep_code") or "")
    if rep_code and rep_code != "200":
        raise RuntimeError(f"ChinaMoney FDR/FR returned {rep_code}")
    return parse_chinamoney_frr_payload(payload)


def parse_closing_repo_payload(payload, market):
    data = (payload or {}).get("data") or {}
    source_date_raw = data.get("lastDate") or data.get("endDate")
    try:
        source_date = parse_date(source_date_raw).isoformat()
    except ValueError:
        return None
    prefix = "dr" if market == "DR" else "r"
    row = {
        "trade_date": source_date,
        "closing_repo_source_date": source_date,
        "closing_repo_available_at": available_at(source_date, "closing_repo"),
        "source_url_closing_repo": CHINAMONEY_CLOSING_REPO_PAGE,
        "raw_closing_repo_json": {"source_date": source_date, "markets": {}},
    }
    raw_records = []
    for record in (payload or {}).get("records") or []:
        code = str(record.get("instrmntCd") or "").strip().upper()
        if code not in {f"{market}001", f"{market}007"}:
            continue
        value = parse_number(record.get("wghtdAvgRepoRate"))
        row[f"{prefix}{'001' if code.endswith('001') else '007'}_weighted_pct"] = value
        raw_records.append(record)
    if not raw_records:
        return None
    row["raw_closing_repo_json"]["markets"][market] = raw_records
    return row


def fetch_closing_repo_row_sync(target_date):
    target = parse_date(target_date)
    merged = None
    for market, index_type in (("DR", "markInterBankVOList"), ("R", "markVOList")):
        response = request_with_retry(
            "POST",
            CHINAMONEY_CLOSING_REPO_URL,
            data={
                "indexType": index_type,
                "lang": "cn",
                "searchDate": target.isoformat(),
                "publishedTime": "2200",
            },
            headers={"Referer": CHINAMONEY_CLOSING_REPO_PAGE},
            timeout=60,
        )
        payload = response.json()
        parsed = parse_closing_repo_payload(payload, market)
        if parsed is None:
            continue
        if merged is None:
            merged = parsed
        elif parsed["trade_date"] == merged["trade_date"]:
            for key, value in parsed.items():
                if key == "raw_closing_repo_json":
                    merged[key]["markets"].update(value.get("markets") or {})
                elif value is not None:
                    merged[key] = value
    return merged


def parse_chinabond_history_html(html):
    soup = BeautifulSoup(html or "", "lxml")
    rows_by_date = {}
    for tr in soup.select("#gjqxData tr, table tr"):
        cells = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all("td")]
        if len(cells) < 5:
            continue
        curve_name = cells[0]
        if curve_name not in {"中债国债收益率曲线", "中债商业银行普通债收益率曲线(AAA)"}:
            continue
        try:
            trade_date = parse_date(cells[1]).isoformat()
        except ValueError:
            continue
        one_year = parse_number(cells[4])
        if one_year is None:
            continue
        row = rows_by_date.setdefault(
            trade_date,
            {
                "trade_date": trade_date,
                "chinabond_source_date": trade_date,
                "chinabond_available_at": available_at(trade_date, "chinabond"),
                "source_url_chinabond": CHINABOND_PAGE,
                "raw_chinabond_json": {},
            },
        )
        if curve_name == "中债国债收益率曲线":
            row["cgb_1y_yield_pct"] = one_year
            row["raw_chinabond_json"]["cgb_1y"] = cells
        else:
            row["bank_bond_aaa_1y_yield_pct"] = one_year
            row["raw_chinabond_json"]["bank_aaa_1y"] = cells
    return [rows_by_date[key] for key in sorted(rows_by_date)]


def fetch_chinabond_rows_sync(start_date, end_date):
    start = parse_date(start_date)
    end = parse_date(end_date)
    response = request_with_retry(
        "GET",
        CHINABOND_HISTORY_URL,
        params={
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "gjqx": "1",
            "qxId": "ycqx",
            "locale": "cn_ZH",
            "mark": "1",
        },
        headers={"Referer": CHINABOND_PAGE},
        timeout=90,
    )
    return parse_chinabond_history_html(response.text)


def parse_pbc_listing_html(html, section):
    soup = BeautifulSoup(html or "", "lxml")
    if section == "omo":
        path_marker = "/125475/"
        title_marker = "公开市场业务交易公告"
    else:
        path_marker = "/5727710/"
        title_marker = "中央银行各项工具流动性投放情况"
    items = []
    seen = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        title = str(anchor.get("title") or anchor.get_text(" ", strip=True)).strip()
        if path_marker not in href or not href.endswith("/index.html") or title_marker not in title:
            continue
        source_url = urljoin(PBC_BASE_URL, href)
        if source_url in seen:
            continue
        parent = anchor.find_parent("td")
        date_match = re.search(r"20\d{2}-\d{2}-\d{2}", parent.get_text(" ", strip=True) if parent else "")
        source_date = date_match.group() if date_match else None
        items.append({"title": title, "source_url": source_url, "source_date": source_date})
        seen.add(source_url)
    total_pages = 1
    pager = soup.find("input", attrs={"totalpage": True})
    if pager is not None:
        try:
            total_pages = max(1, int(pager.get("totalpage") or 1))
        except (TypeError, ValueError):
            total_pages = 1
    return items, total_pages


def fetch_pbc_listing_page_sync(section="omo", page=1):
    if section == "omo":
        url = PBC_OMO_LIST_URL if page == 1 else f"{PBC_BASE_URL}{PBC_OMO_LIST_PATH}/17081-{page}.html"
    else:
        if page != 1:
            raise ValueError("PBC monthly listing currently has only one page")
        url = PBC_MONTHLY_LIST_URL
    response = request_with_retry("GET", url, timeout=90)
    items, total_pages = parse_pbc_listing_html(decode_pbc_html(response), section)
    return {"items": items, "total_pages": total_pages, "source_url": url}


def parse_tenor(value):
    text = clean_text(value)
    if "隔夜" in text:
        return "隔夜", 1
    match = re.search(r"(\d+)天", text)
    if match:
        days = int(match.group(1))
        return f"{days}天", days
    match = re.search(r"(\d+)个?月", text)
    if match:
        months = int(match.group(1))
        return f"{months}个月", months * 30
    match = re.search(r"(\d+)年", text)
    if match:
        years = int(match.group(1))
        return f"{years}年", years * 365
    return text or None, None


def pbc_article_metadata(soup, fallback_date=None):
    def meta(name):
        node = soup.find("meta", attrs={"name": name})
        return str(node.get("content") or "").strip() if node else ""

    title = meta("ArticleTitle") or (soup.title.get_text(" ", strip=True) if soup.title else "")
    pub_date = parse_date(meta("PubDate") or fallback_date)
    time_node = soup.select_one("#shijian")
    published_at = parse_datetime(
        time_node.get_text(" ", strip=True) if time_node else meta("createDate"),
        fallback_date=pub_date,
        fallback_time="09:20:00",
    )
    if published_at is None or published_at.date() != pub_date:
        published_at = parse_datetime(None, fallback_date=pub_date, fallback_time="09:20:00")
    return title, pub_date, published_at


def _pbc_tool_type(context_text, tenor_days=None, article_text=""):
    normalized_context = clean_text(context_text).upper()
    normalized_article = clean_text(article_text).upper()
    if "TMLF" in normalized_context or "定向中期借贷便利" in normalized_context:
        return "tmlf"
    if "MLF" in normalized_context or "中期借贷便利" in normalized_context:
        return "mlf"
    if (
        tenor_days is not None
        and tenor_days >= 90
        and ("MLF" in normalized_article or "中期借贷便利" in normalized_article)
    ):
        return "mlf"
    if "央行票据互换" in normalized_context or "CBS" in normalized_context:
        return "central_bank_bill_swap"
    if "买断式逆回购" in normalized_context:
        return "outright_reverse_repo"
    if "正回购" in normalized_context and "逆回购" not in normalized_context:
        return "repo"
    return "reverse_repo"


def parse_pbc_omo_article_html(html, source_url, fallback_date=None):
    soup = BeautifulSoup(html or "", "lxml")
    title, operation_date, published_at = pbc_article_metadata(soup, fallback_date=fallback_date)
    content = soup.select_one("#zoom") or soup.select_one(".content") or soup
    raw_text = re.sub(r"\s+", " ", content.get_text(" ", strip=True)).strip()
    fetched_at = datetime.now().replace(microsecond=0)
    parsed = []
    dedupe = set()
    for table_index, table in enumerate(content.find_all("table")):
        table_rows = table.find_all("tr")
        if not table_rows:
            continue
        headers = [
            clean_text(cell.get_text(" ", strip=True))
            for cell in table_rows[0].find_all(["td", "th"])
        ]
        tenor_index = next((index for index, value in enumerate(headers) if "期限" in value), None)
        rate_index = next((index for index, value in enumerate(headers) if "利率" in value), None)
        amount_index = next(
            (
                index
                for marker in ("中标量", "操作量", "交易量", "规模")
                for index, value in enumerate(headers)
                if marker in value
            ),
            None,
        )
        if tenor_index is None or amount_index is None:
            continue
        preceding = table.find_previous(["p", "strong", "h3", "h4"])
        table_text = clean_text(table.get_text(" ", strip=True))
        context_text = clean_text(preceding.get_text(" ", strip=True) if preceding else table_text[:100])
        for row_index, tr in enumerate(table_rows[1:], start=1):
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all(["td", "th"])]
            if max(tenor_index, amount_index, rate_index or 0) >= len(cells):
                continue
            tenor_label, tenor_days = parse_tenor(cells[tenor_index])
            rate = parse_number(cells[rate_index]) if rate_index is not None else None
            amount = parse_amount_cny(cells[amount_index])
            if tenor_days is None or amount is None:
                continue
            tool_type = _pbc_tool_type(
                context_text,
                tenor_days=tenor_days,
                article_text=raw_text,
            )
            signature = (tool_type, tenor_days, round(amount, 2))
            if signature in dedupe:
                continue
            dedupe.add(signature)
            parsed.append(
                {
                    "source_item_key": f"{Path(source_url).parts[-2]}:{tool_type}:{tenor_days}:{table_index}:{row_index}",
                    "operation_date": operation_date.isoformat(),
                    "tool_type": tool_type,
                    "tenor_label": tenor_label,
                    "tenor_days": tenor_days,
                    "awarded_amount_cny": amount,
                    "operation_rate_pct": rate,
                    "maturity_date": None,
                    "no_operation": False,
                    "announcement_title": title,
                    "published_at": published_at,
                    "source_url": source_url,
                    "raw_json": {
                        "cells": cells,
                        "context": context_text,
                        "contractual_target_date": (
                            operation_date + timedelta(days=tenor_days)
                        ).isoformat(),
                    },
                    "raw_text": raw_text,
                    "fetched_at": fetched_at,
                }
            )

    paragraph_pattern = re.compile(
        r"开展(?:了)?\s*([0-9,.]+)\s*亿元\s*([^，。；]{0,20}?)(买断式逆回购|逆回购)操作"
    )
    for match_index, match in enumerate(paragraph_pattern.finditer(raw_text)):
        amount = parse_amount_cny(f"{match.group(1)}亿元")
        tenor_label, tenor_days = parse_tenor(match.group(2))
        tool_type = "outright_reverse_repo" if "买断式" in match.group(3) else "reverse_repo"
        if amount is None or tenor_days is None:
            continue
        signature = (tool_type, tenor_days, round(amount, 2))
        if signature in dedupe:
            continue
        dedupe.add(signature)
        parsed.append(
            {
                "source_item_key": f"{Path(source_url).parts[-2]}:{tool_type}:{tenor_days}:text:{match_index}",
                "operation_date": operation_date.isoformat(),
                "tool_type": tool_type,
                "tenor_label": tenor_label,
                "tenor_days": tenor_days,
                "awarded_amount_cny": amount,
                "operation_rate_pct": None,
                "maturity_date": None,
                "no_operation": False,
                "announcement_title": title,
                "published_at": published_at,
                "source_url": source_url,
                "raw_json": {
                    "matched_text": match.group(0),
                    "contractual_target_date": (
                        operation_date + timedelta(days=tenor_days)
                    ).isoformat(),
                },
                "raw_text": raw_text,
                "fetched_at": fetched_at,
            }
        )

    no_operation = bool(
        re.search(
            r"(?:不开展|未开展)[^。；]{0,30}(?:逆回购|公开市场)操作"
            r"|无逆回购操作|逆回购操作(?:量)?(?:为|是)?(?:零|0)",
            raw_text,
        )
    )
    if no_operation and not parsed:
        parsed.append(
            {
                "source_item_key": f"{Path(source_url).parts[-2]}:no_operation",
                "operation_date": operation_date.isoformat(),
                "tool_type": "reverse_repo",
                "tenor_label": None,
                "tenor_days": None,
                "awarded_amount_cny": 0,
                "operation_rate_pct": None,
                "maturity_date": None,
                "no_operation": True,
                "announcement_title": title,
                "published_at": published_at,
                "source_url": source_url,
                "raw_json": {"explicit_no_operation": True},
                "raw_text": raw_text,
                "fetched_at": fetched_at,
            }
        )
    return parsed


def fetch_pbc_omo_article_sync(item):
    response = direct_session().get(item["source_url"], timeout=90)
    response.raise_for_status()
    html = decode_pbc_html(response)
    rows = parse_pbc_omo_article_html(
        html,
        item["source_url"],
        fallback_date=item.get("source_date"),
    )
    if rows:
        return rows
    soup = BeautifulSoup(html or "", "lxml")
    content = soup.select_one("#zoom") or soup.select_one(".content") or soup
    article_text = clean_text(content.get_text(" ", strip=True))
    if "逆回购" in article_text:
        raise RuntimeError(f"央行逆回购公告未解析到逐笔操作：{item['source_url']}")
    return []


def month_end(year, month):
    return date(year, month, calendar.monthrange(year, month)[1])


def monthly_tool_type(tool_name):
    normalized = clean_text(tool_name).upper()
    mappings = (
        ("MLF", "mlf"),
        ("SLF", "slf"),
        ("PSL", "psl"),
        ("买断式逆回购", "outright_reverse_repo"),
        ("逆回购", "reverse_repo"),
        ("国债买卖", "government_bond_trading"),
        ("国库现金", "treasury_cash_deposit"),
        ("支农", "structural_agriculture"),
        ("支小", "structural_small_business"),
        ("碳减排", "structural_carbon_reduction"),
        ("科技创新", "structural_technology"),
        ("保障性住房", "structural_housing"),
    )
    for marker, code in mappings:
        if marker.upper() in normalized:
            return code
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    return f"official_tool_{digest}"


def parse_pbc_monthly_article_html(html, source_url, fallback_date=None):
    soup = BeautifulSoup(html or "", "lxml")
    title, _published_date, published_at = pbc_article_metadata(soup, fallback_date=fallback_date)
    match = re.search(r"(20\d{2})年(\d{1,2})月", title)
    if not match:
        raise ValueError(f"PBC monthly article title lacks period: {title}")
    period_end = month_end(int(match.group(1)), int(match.group(2)))
    content = soup.select_one("#zoom") or soup.select_one(".content") or soup
    fetched_at = datetime.now().replace(microsecond=0)
    result = []
    for table in content.find_all("table"):
        table_text = clean_text(table.get_text(" ", strip=True))
        if "投放" not in table_text or "回笼" not in table_text or "净投放" not in table_text:
            continue
        current_category = None
        for tr in table.find_all("tr"):
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all(["td", "th"])]
            if not cells or "工具" in "".join(cells) and "投放" in "".join(cells):
                continue
            if len(cells) >= 5:
                current_category, tool_name = cells[0], cells[1]
                amount_cells = cells[2:5]
            elif len(cells) == 4:
                tool_name = cells[0]
                amount_cells = cells[1:4]
            else:
                continue
            if not tool_name or tool_name in {"合计", "总计"}:
                continue
            injection = parse_amount_cny(amount_cells[0])
            withdrawal = parse_amount_cny(amount_cells[1])
            net = parse_amount_cny(amount_cells[2])
            if injection is None and withdrawal is None and net is None:
                continue
            result.append(
                {
                    "period_end": period_end.isoformat(),
                    "category": current_category,
                    "tool_type": monthly_tool_type(tool_name),
                    "tool_name": tool_name,
                    "injection_cny": injection,
                    "withdrawal_cny": withdrawal,
                    "net_injection_cny": net,
                    "coverage_status": "official_complete",
                    "published_at": published_at,
                    "source_url": source_url,
                    "raw_json": {"article_title": title, "cells": cells, "unit": "亿元"},
                    "fetched_at": fetched_at,
                }
            )
        if result:
            break
    return result


def fetch_pbc_monthly_article_sync(item):
    response = request_with_retry("GET", item["source_url"], timeout=90)
    return parse_pbc_monthly_article_html(
        decode_pbc_html(response),
        item["source_url"],
        fallback_date=item.get("source_date"),
    )


def merge_rows_by_date(*row_groups):
    merged = {}
    for rows in row_groups:
        for raw in rows or []:
            trade_date = str(raw.get("trade_date") or "").split(" ")[0].strip()
            if not trade_date:
                continue
            target = merged.setdefault(trade_date, {"trade_date": trade_date})
            for key, value in dict(raw).items():
                if key in {"id", "created_at", "updated_at"}:
                    continue
                if value is not None:
                    target[key] = value
    return [merged[key] for key in sorted(merged)]


def resolve_operation_maturity_dates(operations, official_market_dates):
    market_dates = sorted({parse_date(value) for value in official_market_dates if value})
    resolved = []
    for raw in operations or []:
        operation = dict(raw)
        if operation.get("no_operation") or operation.get("tenor_days") is None:
            operation["maturity_date"] = None
            resolved.append(operation)
            continue
        operation_date = parse_date(operation.get("operation_date"))
        contractual_date = operation_date + timedelta(days=int(operation["tenor_days"]))
        if not market_dates or contractual_date < market_dates[0]:
            operation["maturity_date"] = None
            resolved.append(operation)
            continue
        position = bisect_left(market_dates, contractual_date)
        operation["maturity_date"] = (
            market_dates[position].isoformat() if position < len(market_dates) else None
        )
        resolved.append(operation)
    return resolved


def append_open_market_fields(rows, operations):
    operations_by_date = {}
    maturities_by_date = {}
    policy_candidates = []
    for operation in operations or []:
        try:
            operation_date = parse_date(operation.get("operation_date")).isoformat()
        except ValueError:
            continue
        operations_by_date.setdefault(operation_date, []).append(operation)
        if (
            not operation.get("no_operation")
            and str(operation.get("tool_type") or "") == "reverse_repo"
            and operation.get("maturity_date")
            and operation.get("awarded_amount_cny") is not None
        ):
            maturity_date = parse_date(operation.get("maturity_date")).isoformat()
            maturities_by_date.setdefault(maturity_date, []).append(operation)
        if (
            not operation.get("no_operation")
            and str(operation.get("tool_type") or "") == "reverse_repo"
            and int(operation.get("tenor_days") or 0) == 7
            and operation.get("operation_rate_pct") is not None
        ):
            policy_candidates.append({
                "rate": float(operation["operation_rate_pct"]),
                "source_date": operation_date,
                "available_at": operation.get("published_at"),
                "source_url": operation.get("source_url"),
            })

    policy_by_date = {}
    last_policy_rate = None
    for candidate in sorted(
        policy_candidates,
        key=lambda item: (str(item.get("source_date") or ""), str(item.get("available_at") or "")),
    ):
        rate = candidate["rate"]
        if last_policy_rate is None or not math.isclose(rate, last_policy_rate, abs_tol=1e-9):
            policy_by_date[candidate["source_date"]] = candidate
            last_policy_rate = rate

    ordered_rows = sorted(rows, key=lambda item: str(item.get("trade_date") or ""))
    first_row_date = str(ordered_rows[0].get("trade_date") or "") if ordered_rows else ""
    prior_policy_dates = [
        policy_date for policy_date in policy_by_date if policy_date <= first_row_date
    ]
    last_policy = policy_by_date[max(prior_policy_dates)] if prior_policy_dates else None
    net_window = deque(maxlen=20)
    for row in ordered_rows:
        trade_date = str(row["trade_date"])
        if row.get("reverse_repo_7d_policy_rate_pct") is not None and last_policy is None:
            last_policy = {
                "rate": float(row["reverse_repo_7d_policy_rate_pct"]),
                "source_date": row.get("reverse_repo_7d_policy_source_date"),
                "available_at": row.get("reverse_repo_7d_policy_available_at"),
                "source_url": row.get("source_url_reverse_repo_7d_policy"),
            }
        if trade_date in policy_by_date:
            last_policy = policy_by_date[trade_date]
        row["reverse_repo_7d_policy_rate_pct"] = (
            last_policy.get("rate") if last_policy else None
        )
        row["reverse_repo_7d_policy_source_date"] = (
            last_policy.get("source_date") if last_policy else None
        )
        row["reverse_repo_7d_policy_available_at"] = (
            last_policy.get("available_at") if last_policy else None
        )
        row["source_url_reverse_repo_7d_policy"] = (
            last_policy.get("source_url") if last_policy else None
        )

        daily_operations = operations_by_date.get(trade_date) or []
        reverse_repo_sources = [
            item
            for item in daily_operations
            if str(item.get("tool_type") or "") == "reverse_repo"
        ]
        regular_operations = [
            item for item in reverse_repo_sources
            if str(item.get("tool_type") or "") == "reverse_repo" and not item.get("no_operation")
        ]
        explicit_source = bool(reverse_repo_sources)
        injection = None
        if explicit_source:
            injection = sum(float(item.get("awarded_amount_cny") or 0) for item in regular_operations)
            latest_source = max(
                reverse_repo_sources,
                key=lambda item: str(item.get("published_at") or ""),
            )
            row["pbc_source_date"] = trade_date
            row["pbc_available_at"] = latest_source.get("published_at")
            row["source_url_pbc"] = latest_source.get("source_url")
        maturity_operations = maturities_by_date.get(trade_date) or []
        maturity = None
        if explicit_source or maturity_operations:
            maturity = sum(float(item.get("awarded_amount_cny") or 0) for item in maturity_operations)
        row["reverse_repo_injection_cny"] = injection
        row["reverse_repo_maturity_cny"] = maturity
        net = injection - maturity if injection is not None and maturity is not None else None
        row["reverse_repo_net_cny"] = net
        is_official_money_market_day = (
            row.get("fdr001_pct") is not None or row.get("fdr007_pct") is not None
        )
        if is_official_money_market_day:
            net_window.append(net)
            last_five = list(net_window)[-5:]
            row["reverse_repo_net_5d_cny"] = (
                sum(last_five)
                if len(last_five) == 5 and all(value is not None for value in last_five)
                else None
            )
            row["reverse_repo_net_20d_cny"] = (
                sum(net_window)
                if len(net_window) == 20 and all(value is not None for value in net_window)
                else None
            )
        else:
            row["reverse_repo_net_5d_cny"] = None
            row["reverse_repo_net_20d_cny"] = None
    return rows


def compute_liquidity_scores(rows):
    factor_windows = [deque(maxlen=MAX_PERCENTILE_SAMPLES) for _ in CORE_FACTOR_FIELDS]
    prior_scores = deque(maxlen=5)
    calculated = []
    for raw in sorted(rows, key=lambda item: str(item.get("trade_date") or "")):
        row = dict(raw)
        fdr007 = parse_number(row.get("fdr007_pct"))
        policy = parse_number(row.get("reverse_repo_7d_policy_rate_pct"))
        fdr001 = parse_number(row.get("fdr001_pct"))
        fr007 = parse_number(row.get("fr007_pct"))
        bank_yield = parse_number(row.get("bank_bond_aaa_1y_yield_pct"))
        cgb_yield = parse_number(row.get("cgb_1y_yield_pct"))
        factors = (
            (fdr007 - policy) * 100 if fdr007 is not None and policy is not None else None,
            (fdr001 - fdr007) * 100 if fdr001 is not None and fdr007 is not None else None,
            (fr007 - fdr007) * 100 if fr007 is not None and fdr007 is not None else None,
            (bank_yield - cgb_yield) * 100 if bank_yield is not None and cgb_yield is not None else None,
        )
        percentiles = tuple(
            midrank_percentile(window, value)
            for window, value in zip(factor_windows, factors)
        )
        for field, value in zip(CORE_FACTOR_FIELDS, factors):
            row[field] = value
        for field, value in zip(PERCENTILE_FIELDS, percentiles):
            row[field] = value
        if all(value is not None for value in percentiles):
            score = sum(value * weight for value, weight in zip(percentiles, FACTOR_WEIGHTS))
        else:
            score = None
        score_change = None
        if score is not None and len(prior_scores) == 5:
            score_change = score - prior_scores[0]
        trend = None
        if score_change is not None:
            if score_change >= 10:
                trend = "收紧"
            elif score_change <= -10:
                trend = "转松"
            else:
                trend = "平稳"
        row["liquidity_tightness_score"] = score
        row["liquidity_state"] = liquidity_state(score)
        row["score_change_5d"] = score_change
        row["liquidity_trend"] = trend
        row["components_json"] = {
            "method": "prior_midrank_percentile_v1",
            "lookback_max": MAX_PERCENTILE_SAMPLES,
            "minimum_samples": MIN_PERCENTILE_SAMPLES,
            "factors": {
                factor_field: {
                    "value_bp": factor_value,
                    "percentile": percentile_value,
                    "weight": weight,
                    "prior_sample_count": len(window),
                }
                for factor_field, factor_value, percentile_value, weight, window in zip(
                    CORE_FACTOR_FIELDS,
                    factors,
                    percentiles,
                    FACTOR_WEIGHTS,
                    factor_windows,
                )
            },
        }
        row["sources_json"] = {
            "frr": {
                "source_date": str(row.get("frr_source_date") or "") or None,
                "available_at": str(row.get("frr_available_at") or "") or None,
                "url": row.get("source_url_frr"),
            },
            "closing_repo": {
                "source_date": str(row.get("closing_repo_source_date") or "") or None,
                "available_at": str(row.get("closing_repo_available_at") or "") or None,
                "url": row.get("source_url_closing_repo"),
            },
            "chinabond": {
                "source_date": str(row.get("chinabond_source_date") or "") or None,
                "available_at": str(row.get("chinabond_available_at") or "") or None,
                "url": row.get("source_url_chinabond"),
            },
            "pbc": {
                "source_date": str(row.get("pbc_source_date") or "") or None,
                "available_at": str(row.get("pbc_available_at") or "") or None,
                "url": row.get("source_url_pbc"),
            },
            "reverse_repo_7d_policy": {
                "source_date": str(row.get("reverse_repo_7d_policy_source_date") or "") or None,
                "available_at": str(row.get("reverse_repo_7d_policy_available_at") or "") or None,
                "url": row.get("source_url_reverse_repo_7d_policy"),
            },
        }
        row["fetched_at"] = datetime.now().replace(microsecond=0)
        calculated.append(row)
        for window, value in zip(factor_windows, factors):
            if value is not None:
                window.append(value)
        if score is not None:
            prior_scores.append(score)
    return calculated


def load_progress():
    if not PROGRESS_PATH.exists():
        return {"processed_omo_urls": [], "processed_monthly_urls": []}
    try:
        payload = json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"processed_omo_urls": [], "processed_monthly_urls": []}
    return payload if isinstance(payload, dict) else {"processed_omo_urls": [], "processed_monthly_urls": []}


def save_progress(payload):
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = PROGRESS_PATH.with_suffix(PROGRESS_PATH.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(PROGRESS_PATH)


def pbc_items_between_sync(start_date, end_date, max_pages=None):
    start = parse_date(start_date)
    end = parse_date(end_date)
    selected = []
    page = 1
    total_pages = 1
    while page <= total_pages and (max_pages is None or page <= max_pages):
        listing = fetch_pbc_listing_page_sync("omo", page)
        total_pages = listing["total_pages"]
        page_items = listing["items"]
        selected.extend(
            item for item in page_items
            if item.get("source_date") and start <= parse_date(item["source_date"]) <= end
        )
        dated_items = [parse_date(item["source_date"]) for item in page_items if item.get("source_date")]
        if dated_items and min(dated_items) < start:
            break
        page += 1
    deduped = {item["source_url"]: item for item in selected}
    return [deduped[key] for key in sorted(deduped, key=lambda url: deduped[url].get("source_date") or "")]


async def sync_pbc_operations(db, start_date, end_date, progress=None):
    items = await asyncio.to_thread(pbc_items_between_sync, start_date, end_date)
    existing = await db.get_cn_pbc_open_market_operations(start_date, end_date)
    existing_urls = {str(row.get("source_url") or "") for row in existing}
    processed_urls = set((progress or {}).get("processed_omo_urls") or [])
    inserted = 0
    pending_items = []
    for item in items:
        if item["source_url"] in existing_urls or item["source_url"] in processed_urls:
            processed_urls.add(item["source_url"])
            continue
        pending_items.append(item)

    request_gate = asyncio.Lock()
    in_flight_limit = asyncio.Semaphore(3)
    last_request_started = 0.0

    async def fetch_with_rate_limit(item):
        nonlocal last_request_started
        last_error = None
        for attempt in range(1, 5):
            async with in_flight_limit:
                async with request_gate:
                    wait_seconds = max(0.0, 2.0 - (time.monotonic() - last_request_started))
                    if wait_seconds:
                        await asyncio.sleep(wait_seconds)
                    last_request_started = time.monotonic()
                try:
                    rows = await asyncio.to_thread(fetch_pbc_omo_article_sync, item)
                    return item, rows
                except requests.RequestException as exc:
                    last_error = exc
            if attempt < 4:
                retry_delay = 2 ** attempt
                print(
                    f"央行公告网络请求失败，第 {attempt}/4 次，"
                    f"{retry_delay} 秒后重试：{item['source_url']}"
                )
                await asyncio.sleep(retry_delay)
        assert last_error is not None
        raise last_error

    tasks = [asyncio.create_task(fetch_with_rate_limit(item)) for item in pending_items]
    completed_articles = 0
    try:
        for future in asyncio.as_completed(tasks):
            item, rows = await future
            if rows:
                inserted += await db.upsert_cn_pbc_open_market_operations(rows)
            completed_articles += 1
            processed_urls.add(item["source_url"])
            if progress is not None:
                progress["processed_omo_urls"] = sorted(processed_urls)
                progress["last_omo_date"] = item.get("source_date")
                progress["completed_omo_articles"] = len(processed_urls)
                save_progress(progress)
            if completed_articles % 25 == 0:
                print(
                    f"央行公开市场公告回补新增 {completed_articles}/{len(pending_items)} 篇，"
                    f"当前来源日 {item.get('source_date') or '-'}"
                )
    except Exception:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return inserted


async def sync_pbc_monthly(db, progress=None):
    listing = await asyncio.to_thread(fetch_pbc_listing_page_sync, "monthly", 1)
    existing = await db.get_cn_pbc_liquidity_tool_monthly_rows(date(2025, 1, 1), date.today())
    existing_urls = {str(row.get("source_url") or "") for row in existing}
    processed_urls = set((progress or {}).get("processed_monthly_urls") or [])
    inserted = 0
    for item in reversed(listing["items"]):
        if item["source_url"] in existing_urls:
            processed_urls.add(item["source_url"])
            continue
        rows = await asyncio.to_thread(fetch_pbc_monthly_article_sync, item)
        if not rows:
            raise RuntimeError(f"央行月度工具公告未解析到分项：{item['source_url']}")
        inserted += await db.upsert_cn_pbc_liquidity_tool_monthly_rows(rows)
        processed_urls.add(item["source_url"])
        if progress is not None:
            progress["processed_monthly_urls"] = sorted(processed_urls)
            save_progress(progress)
    return inserted


REQUIRED_DAILY_FIELDS = {
    "fr001_pct": "FR001定盘利率",
    "fr007_pct": "FR007定盘利率",
    "fdr001_pct": "FDR001定盘利率",
    "fdr007_pct": "FDR007定盘利率",
    "reverse_repo_7d_policy_rate_pct": "7天逆回购政策利率",
    "reverse_repo_7d_policy_source_date": "7天逆回购政策利率来源",
    "bank_bond_aaa_1y_yield_pct": "1年AAA银行普通债收益率",
    "cgb_1y_yield_pct": "1年国债收益率",
    "dr001_weighted_pct": "DR001日终加权利率",
    "dr007_weighted_pct": "DR007日终加权利率",
    "r001_weighted_pct": "R001日终加权利率",
    "r007_weighted_pct": "R007日终加权利率",
    "liquidity_tightness_score": "银行流动性紧张度",
}


async def refresh_calculated_rows(db, start_date=HISTORY_START, end_date=None):
    start = parse_date(start_date)
    end = parse_date(end_date or date.today())
    existing = await db.get_cn_bank_liquidity_daily_rows(start, end)
    operations = await db.get_cn_pbc_open_market_operations(
        start - timedelta(days=400),
        end + timedelta(days=400),
    )
    official_market_dates = [
        row.get("trade_date")
        for row in existing
        if row.get("fdr001_pct") is not None or row.get("fdr007_pct") is not None
    ]
    operations = resolve_operation_maturity_dates(operations, official_market_dates)
    await db.upsert_cn_pbc_open_market_operations(operations)
    with_omo = append_open_market_fields(existing, operations)
    calculated = compute_liquidity_scores(with_omo)
    written = await db.upsert_cn_bank_liquidity_daily_rows(calculated)
    return calculated, written


async def _fetch_and_upsert_recent_official_data(db, target):
    recent_start = max(HISTORY_START, target - timedelta(days=14))
    frr_rows, bond_rows, closing_row = await asyncio.gather(
        asyncio.to_thread(fetch_chinamoney_frr_rows_sync, recent_start, target),
        asyncio.to_thread(fetch_chinabond_rows_sync, recent_start, target),
        asyncio.to_thread(fetch_closing_repo_row_sync, target),
    )
    merged = merge_rows_by_date(frr_rows, bond_rows, [closing_row] if closing_row else [])
    base_written = await db.upsert_cn_bank_liquidity_daily_rows(merged)
    progress = load_progress()
    omo_written = await sync_pbc_operations(
        db,
        target - timedelta(days=45),
        target,
        progress=progress,
    )
    monthly_written = await sync_pbc_monthly(db, progress=progress)
    return {
        "frr_rows": len(frr_rows),
        "bond_rows": len(bond_rows),
        "closing_repo_source_date": closing_row.get("trade_date") if closing_row else None,
        "base_written": base_written,
        "omo_written": omo_written,
        "monthly_written": monthly_written,
    }


async def sync_daily(target_date=None):
    target = parse_date(target_date or date.today())
    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_cn_bank_liquidity_tables()
        collection = await _fetch_and_upsert_recent_official_data(db, target)
        calculated, calculated_written = await refresh_calculated_rows(db, HISTORY_START, target)
        target_text = target.isoformat()
        target_row = next(
            (row for row in calculated if str(row.get("trade_date")) == target_text),
            None,
        )
        missing = [
            label
            for field, label in REQUIRED_DAILY_FIELDS.items()
            if target_row is None or target_row.get(field) is None
        ]
        if missing:
            latest_complete = next(
                (
                    str(row.get("trade_date"))
                    for row in reversed(calculated)
                    if all(row.get(field) is not None for field in REQUIRED_DAILY_FIELDS)
                ),
                None,
            )
            return {
                "status": "SOURCE_NOT_READY",
                "target_date": target_text,
                "missing": missing,
                "latest_complete_date": latest_complete,
                "closing_repo_source_date": collection["closing_repo_source_date"],
            }
        return {
            "status": "SUCCESS",
            "target_date": target_text,
            "score": target_row.get("liquidity_tightness_score"),
            "state": target_row.get("liquidity_state"),
            "trend": target_row.get("liquidity_trend"),
            "rows": calculated_written,
            "official_collection": collection,
            "source_dates": {
                "frr": str(target_row.get("frr_source_date") or "") or None,
                "closing_repo": str(target_row.get("closing_repo_source_date") or "") or None,
                "chinabond": str(target_row.get("chinabond_source_date") or "") or None,
                "pbc": str(target_row.get("pbc_source_date") or "") or None,
                "policy_rate": (
                    str(target_row.get("reverse_repo_7d_policy_source_date") or "") or None
                ),
            },
        }
    finally:
        await db.close()


def _year_ranges(start, end):
    year = start.year
    while year <= end.year:
        yield max(start, date(year, 1, 1)), min(end, date(year, 12, 31))
        year += 1


async def _backfill_market_rates(db, start, end, progress):
    total = 0
    completed_ranges = set(progress.get("completed_market_rate_ranges") or [])
    for range_start, range_end in _year_ranges(start, end):
        key = f"{range_start.isoformat()}:{range_end.isoformat()}"
        if key in completed_ranges:
            continue
        frr_rows, bond_rows = await asyncio.gather(
            asyncio.to_thread(fetch_chinamoney_frr_rows_sync, range_start, range_end),
            asyncio.to_thread(fetch_chinabond_rows_sync, range_start, range_end),
        )
        merged = merge_rows_by_date(frr_rows, bond_rows)
        total += await db.upsert_cn_bank_liquidity_daily_rows(merged)
        completed_ranges.add(key)
        progress["completed_market_rate_ranges"] = sorted(completed_ranges)
        progress["last_market_rate_range"] = key
        save_progress(progress)
        print(
            f"银行流动性官方利率回补 {range_start.isoformat()} -> {range_end.isoformat()}，"
            f"FDR/FR={len(frr_rows)}，中债={len(bond_rows)}"
        )
    return total


async def _backfill_closing_repo_window(db, start, end):
    public_start = max(start, date.today() - timedelta(days=20))
    public_end = min(end, date.today())
    if public_start > public_end:
        return 0
    rows_by_date = {}
    cursor = public_start
    while cursor <= public_end:
        row = await asyncio.to_thread(fetch_closing_repo_row_sync, cursor)
        if row and public_start <= parse_date(row["trade_date"]) <= public_end:
            rows_by_date[row["trade_date"]] = row
        cursor += timedelta(days=1)
    return await db.upsert_cn_bank_liquidity_daily_rows(
        [rows_by_date[key] for key in sorted(rows_by_date)]
    )


async def backfill(start_date=HISTORY_START, end_date=None):
    start = max(HISTORY_START, parse_date(start_date))
    end = parse_date(end_date or date.today())
    if start > end:
        raise ValueError("start_date must not be after end_date")
    progress = load_progress()
    progress.update(
        {
            "status": "running",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "started_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    save_progress(progress)
    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_cn_bank_liquidity_tables()
        base_written = await _backfill_market_rates(db, start, end, progress)
        omo_written = await sync_pbc_operations(
            db,
            start - timedelta(days=400),
            end,
            progress=progress,
        )
        monthly_written = await sync_pbc_monthly(db, progress=progress)
        closing_written = await _backfill_closing_repo_window(db, start, end)
        calculated, calculated_written = await refresh_calculated_rows(db, start, end)
        complete_scores = sum(
            1 for row in calculated if row.get("liquidity_tightness_score") is not None
        )
        stored_operations = await db.get_cn_pbc_open_market_operations(
            start - timedelta(days=400),
            end,
        )
        stored_monthly = await db.get_cn_pbc_liquidity_tool_monthly_rows(
            date(2025, 1, 1),
            end,
        )
        if not stored_operations:
            raise RuntimeError("央行公开市场历史公告未入库，回补不能标记为成功")
        if end >= date(2025, 5, 31) and not stored_monthly:
            raise RuntimeError("央行月度货币工具未入库，回补不能标记为成功")
        if complete_scores <= 0:
            raise RuntimeError("银行流动性紧张度没有有效历史得分，回补不能标记为成功")
        progress.update(
            {
                "status": "complete",
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "completed_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
        save_progress(progress)
        return {
            "status": "SUCCESS",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "base_written": base_written,
            "omo_written": omo_written,
            "monthly_written": monthly_written,
            "closing_repo_written": closing_written,
            "calculated_written": calculated_written,
            "score_rows": complete_scores,
            "progress_path": str(PROGRESS_PATH),
        }
    except Exception as exc:
        progress.update(
            {
                "status": "failed",
                "failed_at": datetime.now().isoformat(timespec="seconds"),
                "error": str(exc),
            }
        )
        save_progress(progress)
        raise
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
                start_date=args[0] if args else HISTORY_START,
                end_date=args[1] if len(args) > 1 else None,
            )
        )
        return
    raise ValueError("bank-liquidity supports: daily [date] | backfill [start] [end]")


if __name__ == "__main__":
    asyncio.run(main())
