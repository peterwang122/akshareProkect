"""Official release parsing. Periods and publication times are intentionally separate."""

import calendar
import hashlib
import json
import re
import unicodedata
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from urllib.parse import urljoin

from bs4 import BeautifulSoup


PARSER_VERSION = "macro-cycle-v5"
NUMBER = r"[-+]?\d[\d,]*(?:\.\d+)?"
AMOUNT = rf"(?P<value>{NUMBER})(?P<unit>万亿元|亿元)"
CHANGE = rf"(?P<direction>增长|上涨|上升|下降|降低|减少|增加)(?P<value>{NUMBER})%"
TSF_PARTS = {
    "rmb_loan": r"对实体经济发放的人民币贷款",
    "fx_loan": r"对实体经济发放的外币贷款(?:折合人民币)?",
    "entrusted_loan": r"委托贷款",
    "trust_loan": r"信托贷款",
    "bank_acceptance": r"未贴现的银行承兑汇票",
    "corporate_bond": r"企业债券",
    "government_bond": r"政府债券",
    "equity": r"非金融企业境内股票",
}


def compact(value):
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(value or "")))


def classify_title(title):
    title = compact(title)
    if "解读" in title and "CPI" in title and "PPI" in title and re.search(r"20\d{2}年\d+月", title):
        return "core_cpi"
    if "地区" in title or "解读" in title or "答记者问" in title:
        return None
    if "金融统计数据报告" in title or "金融运行" in title:
        return "finance"
    if "社会融资规模存量" in title and "报告" in title:
        return "tsf_stock"
    if "社会融资规模增量" in title and "报告" in title:
        return "tsf_flow"
    if "采购经理指数" in title and ("运行情况" in title or "指数为" in title):
        return "pmi"
    if "居民消费价格" in title and "城市" not in title:
        return "cpi"
    if "工业生产者出厂价格" in title or "工业生产者价格" in title:
        return "ppi"
    if "工业企业利润" in title:
        return "profit"
    return None


def period_from_title(title):
    title = compact(title)
    year_match = re.search(r"((?:19|20)\d{2})年", title)
    if not year_match:
        raise ValueError(f"Missing release period: {title}")
    year = int(year_match[1])
    remainder = title[year_match.end():]
    match = re.search(r"(?:1[—–－-])?(\d{1,2})月份?", remainder)
    if match:
        month = int(match[1])
    elif "上半年" in remainder:
        month = 6
    elif "前三季度" in remainder:
        month = 9
    elif "一季度" in remainder:
        month = 3
    elif "全年" in remainder or any(label in remainder for label in ("金融统计数据报告", "社会融资规模存量统计数据报告", "社会融资规模增量统计数据报告")):
        month = 12
    else:
        raise ValueError(f"Unknown release period: {title}")
    return date(year, month, calendar.monthrange(year, month)[1])


def publication_time(soup):
    # Prefer the visible original publication time over CMS migration/createDate.
    text = soup.get_text(" ", strip=True)
    match = re.search(r"(?:文章来源[:：]?|发布时间[:：]?)\s*((?:19|20)\d{2}[-/]\d{1,2}[-/]\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)", text)
    raw = match[1] if match else None
    if not raw:
        meta = soup.find("meta", attrs={"name": re.compile(r"^PubDate$", re.I)})
        raw = meta.get("content") if meta else None
    if not raw:
        return None, None, "unknown"
    raw = raw.strip().replace("/", "-")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None, None, "unknown"
    precision = "minute" if ":" in raw else "day"
    available = parsed if precision == "minute" else datetime.combine(parsed.date() + timedelta(days=1), time.min)
    return parsed, available, precision


def article_body(soup):
    for selector in ("#zoom", "#zoom2", ".TRS_Editor", ".trs_editor_view", ".contentcon"):
        node = soup.select_one(selector)
        if node and len(node.get_text(strip=True)) > 80:
            return node
    raise ValueError("Official article body was not found; refusing to parse navigation")


def parse_release(html, source_url, fetched_at=None):
    fetched_at = fetched_at or datetime.now()
    raw_response = html
    attachments = []
    if html.lstrip().startswith('{"official_html":'):
        bundle = json.loads(html)
        html, attachments = bundle["official_html"], bundle["attachments"]
    soup = BeautifulSoup(html, "lxml")
    meta = soup.find("meta", attrs={"name": "ArticleTitle"})
    title = meta.get("content", "") if meta else (soup.title.get_text(strip=True) if soup.title else "")
    category = classify_title(title)
    if not category:
        raise ValueError(f"Unsupported release: {title}")
    period = period_from_title(title)
    if attachments:
        text = compact("\n".join(a["text"] for a in attachments))
    else:
        body = article_body(soup)
        text = compact(body.get_text(" ", strip=True))
    published, available, precision = publication_time(soup)
    eligible = available is not None and published.date() >= period.replace(day=1) and published <= fetched_at
    # Rehosted historical articles are displayable, never backdated using their URL.
    if published and (published.date() - period).days > 120:
        eligible = False
    content_hash = hashlib.sha256((compact(title) + "\n" + text).encode()).hexdigest()
    source_key = hashlib.sha256(source_url.encode()).hexdigest()
    release_id = hashlib.sha256((source_key + content_hash).encode()).hexdigest()
    release = {
        "release_id": release_id, "source_key": source_key, "content_hash": content_hash,
        "title": title, "category": category, "source_url": source_url,
        "published_at": published, "available_at": available or fetched_at,
        "publication_precision": precision, "replay_eligible": bool(eligible),
        "vintage_status": "dated_release" if eligible else "display_only",
        "fetched_at": fetched_at, "raw_response": raw_response, "parser_version": PARSER_VERSION,
    }
    observations = {}

    def add(key, match, *, kind="monthly", unit=None, raw=None, at=period, basis=None):
        if match is None:
            return
        if isinstance(match, re.Match):
            groups = match.groupdict()
            value = Decimal(groups["value"].replace(",", ""))
            raw_unit = groups.get("unit") or unit or "%"
            if groups.get("direction") in ("下降", "降低", "减少"):
                value = -value
            raw_value = match.group(0)
        else:
            value = Decimal(str(match))
            raw_unit = unit or "%"
            raw_value = raw or str(match)
        if not value.is_finite():
            return
        if raw_unit in ("亿元", "万亿元"):
            value *= Decimal("100000000" if raw_unit == "亿元" else "1000000000000")
            normalized_unit = "CNY"
        else:
            normalized_unit = raw_unit
        if basis is None:
            basis = ("m1_2025" if at >= date(2025, 1, 1) else "m1_legacy") if key.startswith("m1_") else "official"
        identity = (key, at.isoformat(), kind, basis)
        row = {"series_key": key, "period_end": at.isoformat(), "period_kind": kind,
               "value": str(value), "unit": normalized_unit, "raw_value": raw_value,
               "raw_unit": raw_unit, "basis_version": basis, "release_id": release_id}
        previous = observations.get(identity)
        if previous and previous["value"] != row["value"]:
            raise ValueError(f"Conflicting values for {identity}")
        observations[identity] = row

    if category in ("finance", "tsf_stock", "tsf_flow"):
        text = re.split(r"注[1-9一二三四五六七八九]?[:：]", text, maxsplit=1)[0]
        for code, name in (("m1", "狭义"), ("m2", "广义")):
            match = re.search(rf"{name}货币(?:供应量)?\(?{code.upper()}\)?余额(?:为)?{AMOUNT}", text)
            add(f"{code}_balance", match, kind="stock")
            if match:
                add(f"{code}_yoy", re.search(rf"同比{CHANGE}", text[match.end():match.end() + 50]))
        stock = re.search(rf"社会融资规模存量(?:为|达到){AMOUNT}", text)
        add("tsf_stock", stock, kind="stock")
        if stock:
            add("tsf_stock_yoy", re.search(rf"同比{CHANGE}", text[stock.end():stock.end() + 50]))
        for key, label in TSF_PARTS.items():
            add(f"tsf_{key}_stock", re.search(rf"{label}余额(?:为)?{AMOUNT}", text), kind="stock")
        # Anchor the flow period to its own paragraph, not the report title.
        flow = re.search(rf"(?P<prefix>[^。；]{{0,80}})社会融资规模增量(?P<cumulative>累计)?(?:为|是){AMOUNT}", text)
        if flow:
            kind = "ytd" if flow["cumulative"] or re.search(r"前|上半年|季度|全年|1[-—]", flow["prefix"]) else "monthly"
            add("tsf_flow", flow, kind=kind)
            end = text.find("三、", flow.end())
            section = text[flow.end():end if end >= 0 else flow.end() + 1400]
            for key, label in TSF_PARTS.items():
                add(f"tsf_{key}_flow", re.search(rf"{label}(?:净融资|融资)?(?P<direction>增加|减少)?{AMOUNT}", section), kind=kind)
        for loan_start in re.finditer(r"(?:分部门看|从分部门情况看)[,，:]?(?:住户|居民)(?:部门|户)?贷款", text):
            section = text[loan_start.start():loan_start.start() + 1000]
            prefix = text[max(0, loan_start.start() - 120):loan_start.start()]
            kind = "ytd" if "当月" not in prefix and re.search(r"前[^。]{0,8}月|上半年|季度|全年|1[-—]", prefix) else "monthly"
            for name, pattern in (("household", r"(?:住户|居民)(?:部门|户)?贷款(?P<part>.*?)(?:企\(?事\)?业|非金融企业|非金融性?公司)"),
                                  ("corporate", r"(?:企\(?事\)?业单位|非金融企业(?:及机关团体)?|非金融性?公司(?:及其他部门)?)贷款(?P<part>.*?)(?:非银行业|[。])")):
                segment = re.search(pattern, section)
                if segment:
                    for field, label in (("mlt", "中长期贷款"), ("short", "短期贷款"), ("bills", "票据融资")):
                        add(f"loan_{name}_{field}", re.search(rf"{label}(?P<direction>增加|减少){AMOUNT}", segment["part"]), kind=kind)
    elif category == "pmi":
        manufacturing = text.split("二、中国非制造业")[0]
        for key, pattern in {
            "pmi_manufacturing": rf"制造业采购经理指数\(PMI\)为(?P<value>{NUMBER})%",
            "pmi_production": rf"生产指数为(?P<value>{NUMBER})%",
            "pmi_new_orders": rf"新订单指数为(?P<value>{NUMBER})%",
        }.items():
            add(key, re.search(pattern, manufacturing), unit="points")
        add("pmi_nonmanufacturing", re.search(rf"非制造业商务活动指数为(?P<value>{NUMBER})%", text), unit="points")
    elif category in ("cpi", "ppi", "core_cpi"):
        label = r"(?:全国)?居民消费价格(?:指数\(CPI\))?" if category == "cpi" else r"(?:全国)?工业生产者出厂价格"
        for measure, word in (() if category == "core_cpi" else (("yoy", "同比"), ("mom", "环比"))):
            add(f"{category}_{measure}", re.search(rf"{label}{word}{CHANGE}", text[:1800]))
            if not any(k[0] == f"{category}_{measure}" for k in observations):
                first = re.search(rf"{label}([^。]{{0,100}})", text[:1800])
                if first:
                    add(f"{category}_{measure}", re.search(rf"{word}(?:均)?{CHANGE}", first[1]))
            if re.search(rf"{label}{word}(?:持平|由[^。；]*转为持平)", text[:1800]):
                add(f"{category}_{measure}", 0, raw=f"{word}持平")
        if category in ("cpi", "core_cpi"):
            add("core_cpi_yoy", re.search(rf"核心CPI[^。；]{{0,15}}?同比(?:涨幅(?:扩大|回升|回落|收窄)?至)?{CHANGE}", text))
            if not any(k[0] == "core_cpi_yoy" for k in observations):
                add("core_cpi_yoy", re.search(rf"核心CPI同比(?:涨幅(?:扩大|回升|回落|收窄)?至|上涨)(?P<value>{NUMBER})%", text))
            if not any(k[0] == "core_cpi_yoy" for k in observations) and re.search(r"核心CPI同比(?:持平|由[^。;；]{0,25}转为持平)", text):
                add("core_cpi_yoy", 0, raw="核心CPI同比持平")
            if not any(k[0] == "core_cpi_yoy" for k in observations):
                # Some releases state month-on-month first; the same sentence then
                # gives year-on-year after a semicolon. Never scan into another CPI.
                clause = re.search(r"核心CPI((?:(?!核心CPI|CPI|PPI|。).){0,160})", text)
                if clause:
                    add("core_cpi_yoy", re.search(rf"同比(?:由[^，；]{{0,20}}转为)?{CHANGE}", clause[1]))
    elif category == "profit":
        for key, label in (("industrial_profit", "利润总额"), ("industrial_revenue", "营业收入")):
            match = re.search(rf"(?:实现)?{label}{AMOUNT}", text)
            add(f"{key}_ytd", match, kind="ytd")
            if match:
                add(f"{key}_ytd_yoy", re.search(rf"(?:同比)?{CHANGE}", text[match.end():match.end() + 70]), kind="ytd")
        add("industrial_profit_month_yoy", re.search(rf"{period.month}月份?[,，]全国规模以上工业企业(?:实现)?利润(?:总额)?同比{CHANGE}", text))
        if not any(k[0] == "industrial_profit_month_yoy" for k in observations):
            add("industrial_profit_month_yoy", re.search(rf"{period.month}月份?[,，](?:全国)?(?:规模以上工业企业)?利润同比{CHANGE}", text))
    if not observations and not (category == "core_cpi" and "核心CPI" not in text):
        raise ValueError(f"No supported observations parsed from {title}")
    return release, list(observations.values())


def parse_tsf_table(html, source_url, fetched_at=None):
    """Read the official monthly flow table, never difference cumulative reports."""
    from io import StringIO
    import pandas as pd

    fetched_at = fetched_at or datetime.now()
    soup = BeautifulSoup(html, "lxml")
    text = compact(soup.get_text(" ", strip=True))
    if "社会融资规模增量统计表" not in text or "单位:亿元人民币" not in text:
        raise ValueError("Unsupported TSF table or unit")
    digest = hashlib.sha256(text.encode()).hexdigest()
    source_key = hashlib.sha256(source_url.encode()).hexdigest()
    release_id = hashlib.sha256((source_key + digest).encode()).hexdigest()
    release = dict(release_id=release_id, source_key=source_key, content_hash=digest,
                   title="社会融资规模增量统计表", category="tsf_monthly_table", source_url=source_url,
                   published_at=None, available_at=fetched_at, publication_precision="unknown",
                   replay_eligible=True, vintage_status="first_observed_only", fetched_at=fetched_at,
                   raw_response=html, parser_version=PARSER_VERSION)
    labels = {"社会融资规模增量": "tsf_flow", "人民币贷款": "tsf_rmb_loan_flow",
              "外币贷款(折合人民币)": "tsf_fx_loan_flow", "委托贷款": "tsf_entrusted_loan_flow",
              "信托贷款": "tsf_trust_loan_flow", "未贴现银行承兑汇票": "tsf_bank_acceptance_flow",
              "企业债券": "tsf_corporate_bond_flow", "政府债券": "tsf_government_bond_flow",
              "非金融企业境内股票融资": "tsf_equity_flow"}
    rows = []
    for frame in pd.read_html(StringIO(html), header=None, converters={0: str}):
        columns = {}
        amount_section = True
        for values in frame.itertuples(index=False, name=None):
            units = [compact(value) for value in values if compact(value).startswith('单位:')]
            if units:
                amount_section = all(unit == '单位:亿元人民币' for unit in units)
                columns = {}
                continue
            if not amount_section:
                continue
            if any(compact(value) == "社会融资规模增量" for value in values):
                columns = {i: labels[compact(v)] for i, v in enumerate(values) if compact(v) in labels}
                continue
            match = re.fullmatch(r"((?:19|20)\d{2})[.年](\d{1,2})(?:月)?", compact(values[0]))
            if not match or not columns:
                continue
            year, month = map(int, match.groups())
            period = date(year, month, calendar.monthrange(year, month)[1]).isoformat()
            for i, key in columns.items():
                raw = compact(values[i])
                if raw in ("nan", "", "--", "..."):
                    continue
                if not re.fullmatch(NUMBER, raw):
                    raise ValueError(f"Invalid official TSF value: {raw}")
                rows.append(dict(release_id=release_id, series_key=key, period_end=period,
                                 period_kind="monthly", value=str(Decimal(raw.replace(',', '')) * 100_000_000),
                                 unit="CNY", raw_value=raw, raw_unit="亿元", basis_version="official"))
    if not rows:
        raise ValueError("No monthly observations in official TSF table")
    unique = {}
    for row in rows:
        key = (row["series_key"], row["period_end"], row["period_kind"], row["basis_version"])
        if key in unique and unique[key]["value"] != row["value"]:
            raise ValueError(f"Conflicting official table cells: {key}")
        unique[key] = row
    return release, list(unique.values())


def parse_money_supply_table(html, source_url, fetched_at=None):
    """Read PBC annual money-supply tables, retaining the 2025 M1 basis split."""
    from io import StringIO
    import pandas as pd

    fetched_at = fetched_at or datetime.now()
    soup = BeautifulSoup(html, "lxml")
    text = compact(soup.get_text(" ", strip=True))
    if "货币供应量" not in text or not re.search(r"单位[:：](?:人民币)?亿元", text):
        raise ValueError("Unsupported official money-supply table or unit")
    digest = hashlib.sha256(text.encode()).hexdigest()
    source_key = hashlib.sha256(source_url.encode()).hexdigest()
    release_id = hashlib.sha256((source_key + digest).encode()).hexdigest()
    release = dict(release_id=release_id, source_key=source_key, content_hash=digest,
                   title="货币供应量统计表", category="money_supply_table", source_url=source_url,
                   published_at=None, available_at=fetched_at, publication_precision="unknown",
                   replay_eligible=True, vintage_status="first_observed_only", fetched_at=fetched_at,
                   raw_response=html, parser_version=PARSER_VERSION)
    rows = []

    def append(key, period_text, value, *, basis):
        match = re.fullmatch(r"((?:19|20)\d{2})[.年](\d{1,2})(?:月)?", compact(period_text))
        raw = compact(value).replace("%", "")
        if not match or not re.fullmatch(NUMBER, raw):
            return
        year, month = map(int, match.groups())
        period = date(year, month, calendar.monthrange(year, month)[1]).isoformat()
        is_yoy = key.endswith("_yoy")
        amount = Decimal(raw) if is_yoy else Decimal(raw) * 100_000_000
        rows.append(dict(release_id=release_id, series_key=key, period_end=period,
                         period_kind="monthly" if is_yoy else "stock", value=str(amount),
                         unit="%" if is_yoy else "CNY", raw_value=compact(value),
                         raw_unit="%" if is_yoy else "亿元", basis_version=basis))

    for frame in pd.read_html(StringIO(html), header=None):
        headers = {}
        for row_index, values in frame.iterrows():
            compacted = [compact(value) for value in values]
            date_columns = {column: value for column, value in enumerate(compacted)
                            if re.fullmatch(r"(?:19|20)\d{2}\.\d{1,2}", value)}
            if len(date_columns) >= 2:
                headers[row_index] = date_columns
        header_indexes = sorted(headers)
        for header_position, row_index in enumerate(header_indexes):
            date_columns = headers[row_index]
            next_header = header_indexes[header_position + 1] if header_position + 1 < len(header_indexes) else len(frame)
            for value_index in range(row_index + 1, min(next_header, row_index + 10)):
                values = [compact(value) for value in frame.iloc[value_index]]
                label = "".join(values[:min(date_columns)])
                if "货币和准货币(M2)" in label:
                    key, basis = "m2_balance", "official"
                elif "货币(M1)" in label and "流通中" not in label:
                    key = "m1_balance"
                    basis = "m1_2025" if min(date_columns.values()) >= "2025.01" else "m1_legacy"
                elif "余额(亿元)" in label and "新修订" in text:
                    key, basis = "m1_balance", "m1_2025"
                elif "同比增速" in label and "新修订" in text:
                    key, basis = "m1_yoy", "m1_2025"
                else:
                    continue
                for column, period in date_columns.items():
                    append(key, period, frame.iloc[value_index, column], basis=basis)
    if not rows:
        raise ValueError("No money-supply observations in official table")
    unique = {}
    for row in rows:
        identity = (row["series_key"], row["period_end"], row["period_kind"], row["basis_version"])
        if identity in unique and unique[identity]["value"] != row["value"]:
            raise ValueError(f"Conflicting official money-supply cells: {identity}")
        unique[identity] = row
    return release, list(unique.values())


def discover_page(html, page_url):
    soup = BeautifulSoup(html, "lxml")
    releases = {}
    for link in soup.find_all("a", href=True):
        title = compact(link.get("title") or link.get_text(" ", strip=True))
        if classify_title(title):
            try:
                period = period_from_title(title)
            except ValueError:
                continue
            url = urljoin(page_url, link["href"])
            if url.startswith("https://www.pbc.gov.cn/") or url.startswith("https://www.stats.gov.cn/"):
                releases[url] = {"url": url, "title": title, "period_end": period.isoformat()}
    pages = []
    for link in soup.find_all("a", attrs={"tagname": True}):
        if link["tagname"].startswith("/") and "下一页" in link.get_text():
            pages.append(urljoin(page_url, link["tagname"]))
    match = re.search(r"createPageHTML\((\d+),\s*(\d+),\s*[\"']index[\"']", html)
    if match and int(match[2]) + 1 < int(match[1]):
        pages.append(urljoin(page_url, f"index_{int(match[2]) + 1}.html"))
    return list(releases.values()), pages
