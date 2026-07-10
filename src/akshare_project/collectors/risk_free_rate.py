import asyncio
import sys
from datetime import date, datetime, timedelta

import requests

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools


LOGGER = get_logger("risk_free_rate")
CHINAMONEY_SHIBOR_URL = (
    "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-shibor/ShiborHis"
)
CHINAMONEY_HEADERS = {
    "Referer": "https://www.chinamoney.com.cn/chinese/bkshibor/",
    "User-Agent": "Mozilla/5.0",
}
SHIBOR_TENORS = {
    "ON": 1,
    "1W": 7,
    "2W": 14,
    "1M": 30,
    "3M": 90,
    "6M": 180,
    "9M": 270,
    "1Y": 360,
}


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


def direct_session():
    session = requests.Session()
    session.trust_env = False
    return session


def parse_rate(value):
    try:
        result = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def parse_chinamoney_shibor_payload(payload):
    rows = []
    for record in (payload or {}).get("records") or []:
        trade_date = str(record.get("showDateCN") or "").strip()
        try:
            trade_date = parse_date(trade_date).isoformat()
        except ValueError:
            continue
        for tenor_code, tenor_days in SHIBOR_TENORS.items():
            rate_pct = parse_rate(record.get(tenor_code))
            if rate_pct is None:
                continue
            rows.append(
                {
                    "trade_date": trade_date,
                    "tenor_code": tenor_code,
                    "tenor_days": tenor_days,
                    "rate_pct": rate_pct,
                    "rate_decimal": rate_pct / 100,
                    "data_source": "chinamoney_shibor",
                    "source_url": CHINAMONEY_SHIBOR_URL,
                    "raw_json": record,
                }
            )
    return rows


def fetch_shibor_rows_sync(start_date, end_date):
    start = parse_date(start_date)
    end = parse_date(end_date)
    if start > end:
        raise ValueError("start_date must not be after end_date")
    response = direct_session().get(
        CHINAMONEY_SHIBOR_URL,
        params={
            "lang": "CN",
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
        },
        headers=CHINAMONEY_HEADERS,
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    rep_code = str(((payload or {}).get("head") or {}).get("rep_code") or "")
    if rep_code and rep_code != "200":
        raise RuntimeError(
            ((payload or {}).get("head") or {}).get("rep_message")
            or f"unexpected ChinaMoney response code {rep_code}"
        )
    return parse_chinamoney_shibor_payload(payload)


async def backfill(start_date="2015-02-09", end_date=None):
    start = parse_date(start_date)
    end = parse_date(end_date or date.today())
    db = DbTools()
    await db.init_pool()
    inserted = 0
    try:
        await db.ensure_cn_risk_free_rate_table()
        year = start.year
        while year <= end.year:
            range_start = max(start, date(year, 1, 1))
            range_end = min(end, date(year, 12, 31))
            rows = await asyncio.to_thread(
                fetch_shibor_rows_sync,
                range_start,
                range_end,
            )
            inserted += await db.upsert_cn_risk_free_rate_daily(rows)
            print(
                "ChinaMoney SHIBOR backfill: "
                f"{range_start.isoformat()} -> {range_end.isoformat()}, "
                f"rows={len(rows)}"
            )
            year += 1
        return {
            "status": "SUCCESS",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "rows": inserted,
        }
    finally:
        await db.close()


async def sync_daily(target_date=None):
    target = parse_date(target_date or date.today())
    start = target - timedelta(days=10)
    rows = await asyncio.to_thread(fetch_shibor_rows_sync, start, target)
    target_text = target.isoformat()
    target_rows = [row for row in rows if row["trade_date"] == target_text]
    if len(target_rows) != len(SHIBOR_TENORS):
        raise RuntimeError(
            f"ChinaMoney SHIBOR {target_text} expected "
            f"{len(SHIBOR_TENORS)} tenors, got {len(target_rows)}"
        )
    db = DbTools()
    await db.init_pool()
    try:
        inserted = await db.upsert_cn_risk_free_rate_daily(target_rows)
        return {
            "status": "SUCCESS",
            "target_date": target_text,
            "rows": inserted,
            "tenors": sorted(SHIBOR_TENORS),
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
                start_date=args[0] if args else "2015-02-09",
                end_date=args[1] if len(args) > 1 else None,
            )
        )
        return
    raise ValueError("risk-free-rate supports: daily [date] | backfill [start] [end]")


if __name__ == "__main__":
    asyncio.run(main())
