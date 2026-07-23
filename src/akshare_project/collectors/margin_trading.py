import asyncio
import json
import re
import sys
import time
from datetime import date, datetime, timedelta

import requests

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.db.db_tool import DbTools


LOGGER = get_logger("margin_trading")
SSE_URL = "https://query.sse.com.cn/marketdata/tradedata/queryMargin.do"
SZSE_URL = "http://www.szse.cn/api/report/ShowReport/data"
BSE_HOSTS = ("https://www.bse.cn", "https://www.bseinfo.net")
BSE_PAGE_PATH = "/disclosure/rzrq_trans_list.html"
BSE_API_PATH = "/rzrqjyyexxController/summaryInfoResult.do"
SSE_START_DATE = date(2010, 3, 31)
SZSE_START_DATE = date(2010, 3, 31)
BSE_START_DATE = date(2023, 2, 13)
RECENT_REPAIR_CALENDAR_DAYS = 18
OFFICIAL_REQUEST_INTERVAL_SECONDS = 2.0
HISTORY_PROGRESS = ProgressStore("margin_trading_history")


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


def parse_number(value):
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text.lower() in {"none", "nan", "--", "-"}:
        return None
    try:
        result = float(text)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def direct_session():
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/138.0 Safari/537.36"
            )
        }
    )
    return session


def parse_sse_payload(payload):
    rows = []
    for item in (payload or {}).get("result") or []:
        trade_date = str(item.get("opDate") or "").strip()
        if len(trade_date) != 8:
            continue
        rows.append(
            {
                "trade_date": parse_date(trade_date).isoformat(),
                "exchange": "SSE",
                "financing_balance": parse_number(item.get("rzye")),
                "financing_buy_amount": parse_number(item.get("rzmre")),
                "financing_repayment_amount": parse_number(item.get("rzche")),
                "securities_lending_balance": parse_number(item.get("rqylje")),
                "margin_balance": parse_number(item.get("rzrqjyzl")),
                "securities_lending_sell_volume": parse_number(item.get("rqmcl")),
                "securities_lending_remaining_volume": parse_number(item.get("rqyl")),
                "data_source": "sse_official_margin_summary",
                "source_url": SSE_URL,
                "raw_json": item,
            }
        )
    return rows


def fetch_sse_rows_sync(start_date, end_date):
    start = max(parse_date(start_date), SSE_START_DATE)
    end = parse_date(end_date)
    if start > end:
        return []
    session = direct_session()
    rows_by_date = {}
    page_no = 1
    page_count = 1
    previous_dates = None
    while page_no <= page_count:
        response = session.get(
            SSE_URL,
            params={
                "isPagination": "true",
                "beginDate": start.strftime("%Y%m%d"),
                "endDate": end.strftime("%Y%m%d"),
                "tabType": "",
                "stockCode": "",
                "pageHelp.pageSize": "2000",
                "pageHelp.pageNo": str(page_no),
                "pageHelp.beginPage": str(page_no),
                "pageHelp.cacheSize": "1",
                "pageHelp.endPage": str(page_no),
            },
            headers={"Referer": "https://www.sse.com.cn/"},
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        page_rows = parse_sse_payload(payload)
        page_dates = tuple(row["trade_date"] for row in page_rows)
        if page_no > 1 and page_dates == previous_dates:
            raise RuntimeError(f"上交所融资融券接口第 {page_no} 页重复返回上一页")
        for row in page_rows:
            rows_by_date[row["trade_date"]] = row
        page_help = payload.get("pageHelp") or {}
        page_count = max(int(page_help.get("pageCount") or 1), 1)
        previous_dates = page_dates
        page_no += 1
    return sorted(rows_by_date.values(), key=lambda row: row["trade_date"])


def parse_szse_payload(payload, requested_date):
    sections = payload if isinstance(payload, list) else []
    summary_section = next(
        (
            item
            for item in sections
            if str((item.get("metadata") or {}).get("tabkey") or "") == "tab1"
        ),
        None,
    )
    records = (summary_section or {}).get("data") or []
    metadata = (summary_section or {}).get("metadata") or {}
    source_date = str(metadata.get("subname") or "").strip()
    if not records or source_date != parse_date(requested_date).isoformat():
        return None
    item = records[0]
    amount_scale = 100_000_000
    return {
        "trade_date": source_date,
        "exchange": "SZSE",
        "financing_balance": (parse_number(item.get("jrrzye")) or 0) * amount_scale,
        "financing_buy_amount": (parse_number(item.get("jrrzmr")) or 0) * amount_scale,
        "financing_repayment_amount": None,
        "securities_lending_balance": (parse_number(item.get("jrrjye")) or 0) * amount_scale,
        "margin_balance": (parse_number(item.get("jrrzrjye")) or 0) * amount_scale,
        "securities_lending_sell_volume": (parse_number(item.get("jrrjmc")) or 0) * amount_scale,
        "securities_lending_remaining_volume": (parse_number(item.get("jrrjyl")) or 0) * amount_scale,
        "data_source": "szse_official_margin_summary",
        "source_url": SZSE_URL,
        "raw_json": {"metadata": metadata, "data": item},
    }


def fetch_szse_row_sync(trade_date):
    target = parse_date(trade_date)
    if target < SZSE_START_DATE:
        return None
    response = direct_session().get(
        SZSE_URL,
        params={
            "SHOWTYPE": "JSON",
            "CATALOGID": "1837_xxpl",
            "txtDate": target.isoformat(),
            "tab1PAGENO": "1",
            "random": str(time.time()),
        },
        headers={
            "Referer": "http://www.szse.cn/disclosure/margin/object/index.html",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
        },
        timeout=60,
    )
    response.raise_for_status()
    return parse_szse_payload(response.json(), target)


def parse_jsonp(text):
    payload = str(text or "").strip()
    start = payload.find("(")
    end = payload.rfind(")")
    if start >= 0 and end > start:
        payload = payload[start + 1 : end]
    payload = re.sub(
        r"'(\d{4}-\d{2}-\d{2})'",
        r'"\1"',
        payload,
    )
    return json.loads(payload)


def parse_bse_payload(payload, requested_date):
    if not isinstance(payload, list) or len(payload) < 2:
        return None
    records = payload[0] or []
    source_date = str(payload[1] or "").strip()
    target_text = parse_date(requested_date).isoformat()
    if not records or source_date != target_text:
        return None
    item = records[0]
    return {
        "trade_date": source_date,
        "exchange": "BSE",
        "financing_balance": parse_number(item.get("rzye")),
        "financing_buy_amount": parse_number(item.get("rzmre")),
        "financing_repayment_amount": None,
        "securities_lending_balance": parse_number(item.get("rqye")),
        "margin_balance": parse_number(item.get("rzrqye")),
        "securities_lending_sell_volume": parse_number(item.get("rqmcl")),
        "securities_lending_remaining_volume": parse_number(item.get("rqyl")),
        "data_source": "bse_official_margin_summary",
        "source_url": f"{BSE_HOSTS[0]}{BSE_PAGE_PATH}",
        "raw_json": item,
    }


def fetch_bse_row_sync(trade_date):
    target = parse_date(trade_date)
    if target < BSE_START_DATE:
        return None
    last_error = None
    for host in BSE_HOSTS:
        page_url = f"{host}{BSE_PAGE_PATH}"
        api_url = f"{host}{BSE_API_PATH}"
        try:
            session = direct_session()
            page_response = session.get(
                page_url,
                timeout=60,
                allow_redirects=True,
            )
            page_response.raise_for_status()
            callback = f"marginCallback{int(time.time() * 1000)}"
            response = session.get(
                api_url,
                params={
                    "transDate": target.isoformat(),
                    "page": "0",
                    "callback": callback,
                },
                headers={"Referer": page_url},
                timeout=60,
            )
            response.raise_for_status()
            row = parse_bse_payload(parse_jsonp(response.text), target)
            if row:
                row["source_url"] = page_url
            return row
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise RuntimeError(f"BSE official endpoints unavailable: {last_error}")


async def fetch_with_retry(fetcher, trade_date, attempts=3, wait_for_slot=None):
    last_error = None
    for attempt in range(attempts):
        try:
            if wait_for_slot is not None:
                await wait_for_slot()
            return await asyncio.to_thread(fetcher, trade_date)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(1.0 + attempt)
    raise RuntimeError(
        f"{fetcher.__name__} failed for {parse_date(trade_date).isoformat()}: {last_error}"
    )


def expected_exchanges(trade_date):
    target = parse_date(trade_date)
    exchanges = {"SSE", "SZSE"}
    if target >= BSE_START_DATE:
        exchanges.add("BSE")
    return exchanges


async def _fetch_exchange_dates(exchange, trade_dates, concurrency=3):
    fetcher = fetch_szse_row_sync if exchange == "SZSE" else fetch_bse_row_sync
    semaphore = asyncio.Semaphore(max(1, int(concurrency)))
    rate_lock = asyncio.Lock()
    next_allowed_at = 0.0

    async def wait_for_slot():
        nonlocal next_allowed_at
        async with rate_lock:
            now = time.monotonic()
            sleep_seconds = next_allowed_at - now
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)
            next_allowed_at = time.monotonic() + OFFICIAL_REQUEST_INTERVAL_SECONDS

    async def run_one(trade_date):
        async with semaphore:
            return await fetch_with_retry(
                fetcher,
                trade_date,
                wait_for_slot=wait_for_slot,
            )

    results = await asyncio.gather(
        *(run_one(trade_date) for trade_date in trade_dates),
        return_exceptions=True,
    )
    rows = []
    failures = []
    for trade_date, result in zip(trade_dates, results):
        if isinstance(result, Exception):
            failures.append(
                {
                    "exchange": exchange,
                    "trade_date": parse_date(trade_date).isoformat(),
                    "error": str(result),
                }
            )
        elif result:
            rows.append(result)
    return rows, failures


async def _load_trade_dates(db, start_date, end_date):
    return await db.get_quant_index_dashboard_trade_dates(
        ["上证指数"],
        start_date=parse_date(start_date).isoformat(),
        end_date=parse_date(end_date).isoformat(),
    )


async def backfill(start_date="2010-03-31", end_date=None, concurrency=3):
    start = max(parse_date(start_date), SSE_START_DATE)
    end = parse_date(end_date or date.today())
    if start > end:
        raise ValueError("start_date must not be after end_date")

    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_margin_trading_daily_table()
        trade_dates = await _load_trade_dates(db, start, end)
        existing_keys = await db.get_margin_trading_existing_keys(start, end)

        sse_rows = await asyncio.to_thread(fetch_sse_rows_sync, start, end)
        sse_rows = [
            row
            for row in sse_rows
            if ("SSE", row["trade_date"]) not in existing_keys
        ]
        if sse_rows:
            await db.upsert_margin_trading_daily_rows(sse_rows)
            HISTORY_PROGRESS.append_lines(
                f"SSE:{row['trade_date']}" for row in sse_rows
            )

        all_failures = []
        inserted = len(sse_rows)
        for exchange, exchange_start in (
            ("SZSE", SZSE_START_DATE),
            ("BSE", BSE_START_DATE),
        ):
            missing_dates = [
                item
                for item in trade_dates
                if parse_date(item) >= exchange_start
                and (exchange, parse_date(item).isoformat()) not in existing_keys
            ]
            for offset in range(0, len(missing_dates), 120):
                chunk = missing_dates[offset : offset + 120]
                rows, failures = await _fetch_exchange_dates(
                    exchange,
                    chunk,
                    concurrency=concurrency,
                )
                if rows:
                    inserted += await db.upsert_margin_trading_daily_rows(rows)
                    HISTORY_PROGRESS.append_lines(
                        f"{exchange}:{row['trade_date']}" for row in rows
                    )
                all_failures.extend(failures)
                print(
                    "margin history: "
                    f"exchange={exchange}, completed={min(offset + len(chunk), len(missing_dates))}"
                    f"/{len(missing_dates)}, inserted={len(rows)}, failures={len(failures)}"
                )

        await db.recompute_margin_trading_net_buy(start, end)
        summary = await db.get_margin_trading_coverage_summary(start, end)
        return {
            "status": "SUCCESS" if not all_failures else "PARTIAL",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "inserted": inserted,
            "coverage": summary,
            "failure_count": len(all_failures),
            "failure_samples": all_failures[:20],
        }
    finally:
        await db.close()


async def sync_daily(target_date=None):
    target = parse_date(target_date or date.today())
    start = max(SSE_START_DATE, target - timedelta(days=RECENT_REPAIR_CALENDAR_DAYS))
    db = DbTools()
    await db.init_pool()
    try:
        await db.ensure_margin_trading_daily_table()
        trade_dates = await _load_trade_dates(db, start, target)
        if not trade_dates:
            raise RuntimeError(f"{target.isoformat()} 前没有可用 A 股交易日")

        inserted = await db.upsert_margin_trading_daily_rows(
            await asyncio.to_thread(fetch_sse_rows_sync, start, target)
        )
        failures = []
        for exchange, exchange_start in (
            ("SZSE", SZSE_START_DATE),
            ("BSE", BSE_START_DATE),
        ):
            exchange_dates = [
                item for item in trade_dates if parse_date(item) >= exchange_start
            ]
            rows, exchange_failures = await _fetch_exchange_dates(
                exchange,
                exchange_dates,
                concurrency=3,
            )
            if rows:
                inserted += await db.upsert_margin_trading_daily_rows(rows)
            failures.extend(exchange_failures)

        await db.recompute_margin_trading_net_buy(start, target)
        target_text = target.isoformat()
        coverage = await db.get_margin_trading_coverage_summary(start, target)
        coverage_by_date = {
            str(item.get("trade_date")): set(
                str(item.get("exchanges") or "").split(",")
            )
            for item in coverage
        }
        complete_dates = [
            parse_date(item)
            for item, exchanges in coverage_by_date.items()
            if expected_exchanges(item).issubset(exchanges)
        ]
        latest_complete = max(complete_dates) if complete_dates else None
        target_complete = expected_exchanges(target).issubset(
            coverage_by_date.get(target_text, set())
        )
        dashboard_affected = 0
        if target_complete:
            from akshare_project.collectors import quant_index

            dashboard_affected = await quant_index.refresh_trade_dates(db, [target])
        result = {
            "status": "SUCCESS" if target_complete else "SOURCE_NOT_READY",
            "target_date": target_text,
            "latest_complete_date": (
                latest_complete.isoformat() if latest_complete else None
            ),
            "expected_exchanges": sorted(expected_exchanges(target)),
            "available_exchanges": sorted(coverage_by_date.get(target_text, set())),
            "inserted": inserted,
            "dashboard_affected": dashboard_affected,
            "failure_count": len(failures),
            "failure_samples": failures[:10],
        }
        if not target_complete:
            result["message"] = (
                f"{target_text} 官网数据尚未齐全；"
                f"当前已有 {','.join(result['available_exchanges']) or '-'}，"
                f"最近完整日期 {result['latest_complete_date'] or '-'}"
            )
        return result
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
                start_date=args[0] if args else "2010-03-31",
                end_date=args[1] if len(args) > 1 else None,
                concurrency=int(args[2]) if len(args) > 2 else 3,
            )
        )
        return
    raise ValueError(
        "margin-trading supports: daily [date] | backfill [start] [end] [concurrency]"
    )


if __name__ == "__main__":
    asyncio.run(main())
