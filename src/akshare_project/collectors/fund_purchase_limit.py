import asyncio
import hashlib
import json
import math
import re
import sys
import time
from datetime import date, datetime

import akshare as ak
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.db.db_tool import DbTools


LOGGER = get_logger("fund_purchase_limit")
DATA_SOURCE = "eastmoney_open_fund_daily"
HISTORY_DATA_SOURCE = "eastmoney_f10_lsjz"
HISTORY_URL = "https://api.fund.eastmoney.com/f10/lsjz"
HISTORY_PAGE_SIZE = 20
HISTORY_DEFAULT_START_DATE = date(2001, 1, 1)
HISTORY_DEFAULT_CONCURRENCY = 4
HISTORY_PROGRESS_STORE = ProgressStore("fund_purchase_limit_history")
MARKET_WIDE_PAUSE_RATIO = 0.08
MARKET_WIDE_PAUSE_MIN_PRODUCTS = 100
ELIGIBLE_FUND_TYPES = {
    "股票型",
    "混合型-偏股",
    "混合型-灵活",
    "混合型-平衡",
    "指数型-股票",
}
EXCHANGE_TRADED_FUND_CODE_PREFIXES = ("159", "51", "520", "56", "588", "589")
EXCLUDED_NAME_PATTERN = re.compile(
    r"QDII|港股|恒生|香港|沪港深|海外|全球|纳斯达克|标普|日经|德国|法国|印度|越南|"
    r"美国|日本|英国|欧洲|东南亚|亚洲(?:除日本)|金砖|大中华",
    re.IGNORECASE,
)
SHARE_CLASS_SUFFIX_PATTERN = re.compile(
    r"(?:\s*[-－_/]?\s*(?:A|B|C|D|E|H|I|R|Y|人民币(?:A|B|C)?|美元(?:现汇|现钞)?|前端|后端))$",
    re.IGNORECASE,
)
DATE_COLUMN_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})-(?:单位净值|累计净值)$")


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


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


def normalize_fund_product_name(value):
    name = re.sub(r"\s+", "", str(value or "").strip())
    if not name:
        return ""
    normalized = name
    while True:
        stripped = SHARE_CLASS_SUFFIX_PATTERN.sub("", normalized).strip("-－_/ ")
        if not stripped or stripped == normalized:
            break
        normalized = stripped
    return normalized or name


def build_product_key(product_name):
    return hashlib.sha1(str(product_name or "").encode("utf-8")).hexdigest()


def is_exchange_traded_fund_code(value):
    fund_code = str(value or "").strip()
    return len(fund_code) == 6 and fund_code.startswith(
        EXCHANGE_TRADED_FUND_CODE_PREFIXES
    )


def is_eligible_a_share_equity_fund(
    fund_type,
    fund_name,
    purchase_status=None,
    fund_code=None,
):
    normalized_type = str(fund_type or "").strip()
    normalized_name = str(fund_name or "").strip()
    normalized_status = str(purchase_status or "").strip()
    if normalized_type not in ELIGIBLE_FUND_TYPES:
        return False
    if not normalized_name or EXCLUDED_NAME_PATTERN.search(normalized_name):
        return False
    if "场内" in normalized_status or is_exchange_traded_fund_code(fund_code):
        return False
    return True


def classify_purchase_status(value):
    status = re.sub(r"\s+", "", str(value or "").strip())
    suspended_purchase = status in {"暂停申购", "停止申购"}
    limited_large = "大额" in status and any(
        marker in status for marker in ("限", "暂停", "停止")
    )
    return {
        # Ordinary purchase suspension often appears market-wide around holidays.
        # Keep it as raw status, but do not treat it as manager-driven large limits.
        "limited_flag": 1 if limited_large else 0,
        "limited_large_flag": 1 if limited_large else 0,
        "suspended_purchase_flag": 1 if suspended_purchase else 0,
    }


def detect_market_wide_purchase_pause(
    rows,
    pause_ratio=MARKET_WIDE_PAUSE_RATIO,
    min_products=MARKET_WIDE_PAUSE_MIN_PRODUCTS,
):
    product_suspended = {}
    for row in rows or []:
        product_key = str(row.get("product_key") or "").strip()
        if not product_key:
            continue
        product_suspended[product_key] = max(
            product_suspended.get(product_key, 0),
            1 if row.get("suspended_purchase_flag") else 0,
        )
    total_products = len(product_suspended)
    suspended_products = sum(product_suspended.values())
    detected = (
        total_products >= int(min_products)
        and suspended_products / total_products >= float(pause_ratio)
    )
    return {
        "detected": detected,
        "total_products": total_products,
        "suspended_products": suspended_products,
        "suspended_pct": (
            suspended_products / total_products * 100 if total_products else 0.0
        ),
    }


def carry_forward_market_wide_pause_flags(
    rows,
    previous_flags,
    pause_ratio=MARKET_WIDE_PAUSE_RATIO,
    min_products=MARKET_WIDE_PAUSE_MIN_PRODUCTS,
):
    detection = detect_market_wide_purchase_pause(
        rows,
        pause_ratio=pause_ratio,
        min_products=min_products,
    )
    carried_rows = 0
    if detection["detected"]:
        for row in rows:
            fund_code = str(row.get("fund_code") or "").strip()
            if (
                row.get("suspended_purchase_flag")
                and not row.get("limited_large_flag")
                and previous_flags.get(fund_code)
            ):
                row["limited_flag"] = 1
                carried_rows += 1
    return {**detection, "carried_rows": carried_rows}


def detect_source_date(columns):
    dates = []
    for column in columns:
        match = DATE_COLUMN_PATTERN.match(str(column).strip())
        if match:
            dates.append(parse_date(match.group(1)))
    if not dates:
        raise RuntimeError("open fund snapshot does not expose a dated NAV column")
    return max(dates)


def build_fund_purchase_limit_rows(snapshot_df, fund_name_df):
    source_date = detect_source_date(snapshot_df.columns)
    type_by_code = {
        str(item.get("基金代码") or "").strip(): str(item.get("基金类型") or "").strip()
        for item in fund_name_df.to_dict("records")
    }
    rows = []
    for record in snapshot_df.to_dict("records"):
        raw_record = {
            str(key): None if pd.isna(value) else value
            for key, value in record.items()
        }
        fund_code = str(record.get("基金代码") or "").strip()
        fund_name = str(record.get("基金简称") or "").strip()
        fund_type = type_by_code.get(fund_code, "")
        purchase_status = str(record.get("申购状态") or "").strip()
        redemption_status = str(record.get("赎回状态") or "").strip()
        if not fund_code or not is_eligible_a_share_equity_fund(
            fund_type,
            fund_name,
            purchase_status,
            fund_code,
        ):
            continue
        product_name = normalize_fund_product_name(fund_name)
        status_flags = classify_purchase_status(purchase_status)
        rows.append(
            {
                "trade_date": source_date.isoformat(),
                "fund_code": fund_code,
                "fund_name": fund_name,
                "product_name": product_name,
                "product_key": build_product_key(product_name),
                "fund_type": fund_type,
                "purchase_status": purchase_status or None,
                "redemption_status": redemption_status or None,
                **status_flags,
                "data_source": DATA_SOURCE,
                "raw_json": json.dumps(raw_record, ensure_ascii=False, default=str),
            }
        )
    return source_date, rows


def build_historical_fund_rows(fund_info, history_records):
    fund_code = str(fund_info.get("fund_code") or "").strip()
    fund_name = str(fund_info.get("fund_name") or "").strip()
    fund_type = str(fund_info.get("fund_type") or "").strip()
    product_name = normalize_fund_product_name(fund_name)
    product_key = build_product_key(product_name)
    rows = []
    for record in history_records or []:
        trade_date = str(record.get("FSRQ") or "").strip()
        purchase_status = str(record.get("SGZT") or "").strip()
        redemption_status = str(record.get("SHZT") or "").strip()
        if not trade_date or not is_eligible_a_share_equity_fund(
            fund_type,
            fund_name,
            purchase_status,
            fund_code,
        ):
            continue
        rows.append(
            {
                "trade_date": parse_date(trade_date).isoformat(),
                "fund_code": fund_code,
                "fund_name": fund_name,
                "product_name": product_name,
                "product_key": product_key,
                "fund_type": fund_type,
                "purchase_status": purchase_status or None,
                "redemption_status": redemption_status or None,
                **classify_purchase_status(purchase_status),
                "data_source": HISTORY_DATA_SOURCE,
                "raw_json": json.dumps(record, ensure_ascii=False, default=str),
            }
        )
    return rows


def build_history_session():
    session = requests.Session()
    session.trust_env = False
    retries = Retry(
        total=6,
        connect=6,
        read=6,
        status=6,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=1, pool_maxsize=1)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Referer": "https://fundf10.eastmoney.com/",
        }
    )
    return session


def fetch_history_page(session, fund_code, page_index, start_date, end_date):
    response = session.get(
        HISTORY_URL,
        params={
            "fundCode": fund_code,
            "pageIndex": page_index,
            "pageSize": HISTORY_PAGE_SIZE,
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        },
        timeout=(10, 45),
    )
    response.raise_for_status()
    payload = response.json()
    if int(payload.get("ErrCode") or 0) != 0:
        raise RuntimeError(
            f"history source error for {fund_code} page {page_index}: "
            f"{payload.get('ErrMsg') or payload.get('ErrCode')}"
        )
    data = payload.get("Data") or {}
    return payload, list(data.get("LSJZList") or [])


def fetch_fund_history_sync(fund_code, start_date, end_date):
    session = build_history_session()
    try:
        first_payload, first_rows = fetch_history_page(
            session,
            fund_code,
            1,
            start_date,
            end_date,
        )
        total_count = int(first_payload.get("TotalCount") or len(first_rows))
        total_pages = max(1, math.ceil(total_count / HISTORY_PAGE_SIZE))
        records = list(first_rows)
        for page_index in range(2, total_pages + 1):
            _, page_rows = fetch_history_page(
                session,
                fund_code,
                page_index,
                start_date,
                end_date,
            )
            if not page_rows:
                raise RuntimeError(
                    f"history source returned empty page for {fund_code}: "
                    f"{page_index}/{total_pages}"
                )
            records.extend(page_rows)
        unique_records = {
            str(record.get("FSRQ") or "").strip(): record
            for record in records
            if str(record.get("FSRQ") or "").strip()
        }
        return [unique_records[key] for key in sorted(unique_records)]
    finally:
        session.close()


def fetch_history_universe_sync():
    fund_name_df = ak.fund_name_em()
    universe = []
    for record in fund_name_df.to_dict("records"):
        fund_code = str(record.get("基金代码") or "").strip()
        fund_name = str(record.get("基金简称") or "").strip()
        fund_type = str(record.get("基金类型") or "").strip()
        if fund_code and is_eligible_a_share_equity_fund(
            fund_type,
            fund_name,
            fund_code=fund_code,
        ):
            universe.append(
                {
                    "fund_code": fund_code,
                    "fund_name": fund_name,
                    "fund_type": fund_type,
                }
            )
    return universe


def history_progress_key(start_date, end_date, fund_code):
    return f"{start_date.isoformat()},{end_date.isoformat()},{fund_code}"


def fetch_rows_sync():
    snapshot_df = ak.fund_open_fund_daily_em()
    fund_name_df = ak.fund_name_em()
    return build_fund_purchase_limit_rows(snapshot_df, fund_name_df)


async def sync_daily(target_date=None):
    expected_date = parse_date(target_date or date.today())
    source_date, rows = await asyncio.to_thread(fetch_rows_sync)
    if source_date != expected_date:
        raise RuntimeError(
            f"fund purchase status source is not ready for {expected_date.isoformat()}: "
            f"latest source date is {source_date.isoformat()}"
        )
    if len(rows) < 1000:
        raise RuntimeError(
            f"eligible A-share equity fund sample is unexpectedly small: {len(rows)}"
        )

    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_fund_purchase_limit_daily_table()
        pause_detection = detect_market_wide_purchase_pause(rows)
        if pause_detection["detected"]:
            previous_flags = await db.get_previous_fund_purchase_limit_flags(
                source_date
            )
            pause_detection = carry_forward_market_wide_pause_flags(
                rows,
                previous_flags,
            )
        written = await db.replace_fund_purchase_limit_daily_rows(source_date, rows)
        summary = await db.get_fund_purchase_limit_daily_summary(
            source_date,
            source_date,
        )
        item = summary[0] if summary else None
        if not item or int(item.get("total_fund_count") or 0) <= 0:
            raise RuntimeError("fund purchase limit rows were written but product summary is empty")
        return {
            "status": "SUCCESS",
            "target_date": source_date.isoformat(),
            "share_class_rows": written,
            "total_fund_count": int(item["total_fund_count"]),
            "limited_fund_count": int(item["limited_fund_count"]),
            "limited_fund_pct": float(item["limited_fund_pct"]),
            "market_wide_pause": bool(pause_detection["detected"]),
            "suspended_product_pct": round(
                float(pause_detection["suspended_pct"]),
                4,
            ),
            "carried_limit_rows": int(pause_detection.get("carried_rows") or 0),
            "data_source": DATA_SOURCE,
        }
    finally:
        await db.close()


async def backfill_history(start_date=None, end_date=None, concurrency=None):
    normalized_start = parse_date(start_date or HISTORY_DEFAULT_START_DATE)
    normalized_end = parse_date(end_date or date.today())
    if normalized_start > normalized_end:
        raise ValueError("history start date must not be later than end date")
    normalized_concurrency = max(
        1,
        min(12, int(concurrency or HISTORY_DEFAULT_CONCURRENCY)),
    )
    universe = await asyncio.to_thread(fetch_history_universe_sync)
    processed = HISTORY_PROGRESS_STORE.load()
    pending = [
        item
        for item in universe
        if history_progress_key(
            normalized_start,
            normalized_end,
            item["fund_code"],
        )
        not in processed
    ]
    print(
        "fund purchase limit history backfill: "
        f"range={normalized_start.isoformat()}..{normalized_end.isoformat()}, "
        f"eligible_share_classes={len(universe)}, pending={len(pending)}, "
        f"concurrency={normalized_concurrency}"
    )

    db = DbTools()
    await db.init_pool()
    semaphore = asyncio.Semaphore(normalized_concurrency)
    progress_lock = asyncio.Lock()
    counters = {"completed": 0, "rows": 0, "failed": 0}
    failures = []
    started_at = time.monotonic()

    async def process_fund(fund_info):
        fund_code = fund_info["fund_code"]
        async with semaphore:
            try:
                history_records = await asyncio.to_thread(
                    fetch_fund_history_sync,
                    fund_code,
                    normalized_start,
                    normalized_end,
                )
                rows = build_historical_fund_rows(fund_info, history_records)
                written = await db.upsert_fund_purchase_limit_daily_rows(rows)
                key = history_progress_key(normalized_start, normalized_end, fund_code)
                async with progress_lock:
                    await asyncio.to_thread(HISTORY_PROGRESS_STORE.append, key)
                    processed.add(key)
                    counters["completed"] += 1
                    counters["rows"] += written
            except Exception as exc:
                LOGGER.exception("fund history backfill failed for %s", fund_code)
                async with progress_lock:
                    counters["failed"] += 1
                    failures.append(f"{fund_code}: {exc}")
            finally:
                finished = counters["completed"] + counters["failed"]
                if finished and (finished % 25 == 0 or finished == len(pending)):
                    elapsed = max(0.001, time.monotonic() - started_at)
                    rate = finished / elapsed
                    remaining = max(0, len(pending) - finished)
                    eta_minutes = remaining / rate / 60 if rate > 0 else 0
                    print(
                        "fund purchase limit history progress: "
                        f"{finished}/{len(pending)}, completed={counters['completed']}, "
                        f"failed={counters['failed']}, rows={counters['rows']}, "
                        f"eta={eta_minutes:.1f}m"
                    )

    try:
        await asyncio.gather(*(process_fund(item) for item in pending))
        if failures:
            raise RuntimeError(
                f"fund purchase limit history has {len(failures)} failed funds; "
                f"rerun resumes automatically. samples: {' | '.join(failures[:10])}"
            )
        normalization = (
            await db.normalize_fund_purchase_limit_indicator_flags(
                normalized_start,
                normalized_end,
                pause_ratio=MARKET_WIDE_PAUSE_RATIO,
                min_products=MARKET_WIDE_PAUSE_MIN_PRODUCTS,
            )
        )
        return {
            "status": "SUCCESS",
            "start_date": normalized_start.isoformat(),
            "end_date": normalized_end.isoformat(),
            "eligible_share_classes": len(universe),
            "completed_share_classes": counters["completed"],
            "written_rows": counters["rows"],
            "market_wide_pause_dates": normalization["market_wide_pause_dates"],
            "carried_limit_rows": normalization["carried_limit_rows"],
        }
    finally:
        await db.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "daily"
    if command == "daily":
        print(await sync_daily(target_date=sys.argv[2] if len(sys.argv) > 2 else None))
        return
    if command == "backfill":
        print(
            await backfill_history(
                start_date=sys.argv[2] if len(sys.argv) > 2 else None,
                end_date=sys.argv[3] if len(sys.argv) > 3 else None,
                concurrency=sys.argv[4] if len(sys.argv) > 4 else None,
            )
        )
        return
    raise ValueError(
        "fund-purchase-limit supports: daily [date] | "
        "backfill [start_date] [end_date] [concurrency]"
    )


if __name__ == "__main__":
    asyncio.run(main())
