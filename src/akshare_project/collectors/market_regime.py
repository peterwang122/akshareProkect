"""Refresh four official index series independently of individual stock collection."""

import asyncio
from datetime import date, timedelta

import aiomysql

from akshare_project.collectors.global_risk import build_csi_tech_index_rows, fetch_csi_index_perf
from akshare_project.db.db_tool import DbTools

INDEX_CODES = ("sh000985", "sh000300", "sh000905", "sh000852")


async def sync_daily(target_date=None):
    target = date.fromisoformat(str(target_date)) if target_date else date.today()
    db = DbTools()
    written, coverage = 0, {}
    try:
        await db.init_pool()
        for code in INDEX_CODES:
            async with db.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute("SELECT MAX(trade_date) AS latest FROM index_daily_data WHERE index_code=%s AND trade_date<=%s", (code, target))
                    latest = (await cursor.fetchone())["latest"]
            start = max(date(2005, 1, 1), latest - timedelta(days=10)) if latest else date(2005, 1, 1)
            while start <= target:
                end = min(target, start + timedelta(days=365))
                raw = await asyncio.to_thread(fetch_csi_index_perf, code[2:], start, end)
                rows = build_csi_tech_index_rows(raw, code[2:], code)
                rows = [r for r in rows if start.isoformat() <= r["trade_date"] <= end.isoformat()]
                written += await db.upsert_index_daily_data(rows)
                start = end + timedelta(days=1)
                await asyncio.sleep(1)
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT COUNT(*) FROM index_daily_data WHERE index_code=%s AND trade_date=%s AND close_price>0", (code, target))
                    coverage[code] = int((await cursor.fetchone())[0])
        if not all(coverage.values()):
            raise RuntimeError(f"Market regime required index date {target} incomplete: {coverage}")
        return {"status": "ok", "target_date": target.isoformat(), "upserted": written, "index_coverage": coverage}
    finally:
        await db.close()
