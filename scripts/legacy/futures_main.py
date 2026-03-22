import asyncio
import os
import sys
import time
from datetime import date, datetime, timedelta

import akshare as ak

from util.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MARKET = 'CFFEX'
BACKFILL_START_DATE = date(2010, 4, 16)
CHUNK_DAYS = 90
FUTURES_PROGRESS_LOG = 'futures_progress.log'
FUTURES_ERROR_LOG = 'futures_error.log'


def log_progress(start_date, end_date, inserted_rows):
    with open(FUTURES_PROGRESS_LOG, 'a', encoding='utf-8') as f:
        f.write(f'{start_date},{end_date},{inserted_rows}\n')


def log_error(start_date, end_date, error_message):
    with open(FUTURES_ERROR_LOG, 'a', encoding='utf-8') as f:
        f.write(f'{start_date},{end_date},{error_message}\n')


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            if attempt < retries:
                print(f'{func.__name__} attempt {attempt}/{retries} failed: {e}')
                time.sleep(sleep_seconds)
    raise last_error


def get_futures_daily_range(start_date, end_date, market=MARKET):
    return fetch_with_retry(
        ak.get_futures_daily,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d'),
        market=market,
    )


def normalize_trade_date(value):
    if value is None:
        return ''
    text = str(value).split(' ')[0]
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, '%Y%m%d').strftime('%Y-%m-%d')
    return text


def build_futures_rows(df, market=MARKET):
    rows = []
    for _, row in df.iterrows():
        trade_date = normalize_trade_date(row.get('date'))
        symbol = str(row.get('symbol', '')).strip().upper()
        if not symbol or not trade_date:
            continue

        rows.append({
            'market': market,
            'symbol': symbol,
            'variety': str(row.get('variety', '')).strip().upper() or None,
            'trade_date': trade_date,
            'open_price': row.get('open'),
            'high_price': row.get('high'),
            'low_price': row.get('low'),
            'close_price': row.get('close'),
            'volume': row.get('volume'),
            'open_interest': row.get('open_interest'),
            'turnover': row.get('turnover'),
            'settle_price': row.get('settle'),
            'pre_settle_price': row.get('pre_settle'),
            'data_source': 'get_futures_daily',
        })

    return rows


def group_rows_by_symbol(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row['symbol'], []).append(row)
    return grouped


def build_date_ranges(start_date, end_date, chunk_days=CHUNK_DAYS):
    ranges = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        ranges.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return ranges


async def ingest_range(db_tools, start_date, end_date, market=MARKET):
    try:
        df = await asyncio.to_thread(get_futures_daily_range, start_date, end_date, market)
        if df is None or df.empty:
            print(f'{start_date} -> {end_date}: no futures data')
            log_progress(start_date, end_date, 0)
            return 0

        rows = build_futures_rows(df, market)
        grouped_rows = group_rows_by_symbol(rows)

        inserted_total = 0
        for symbol_rows in grouped_rows.values():
            inserted_total += await db_tools.batch_futures_daily_data(symbol_rows)

        log_progress(start_date, end_date, inserted_total)
        print(f'{start_date} -> {end_date}: inserted {inserted_total}')
        return inserted_total
    except Exception as e:
        error_message = f'Error processing {start_date} -> {end_date}: {e}'
        print(error_message)
        log_error(start_date, end_date, error_message)
        return 0


async def backfill_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        end_date = datetime.now().date()
        total_inserted = 0
        for start_date, range_end in build_date_ranges(BACKFILL_START_DATE, end_date):
            total_inserted += await ingest_range(db_tools, start_date, range_end, MARKET)
        print(f'futures backfill finished, inserted rows: {total_inserted}')
    finally:
        await db_tools.close()


async def sync_today():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        today = datetime.now().date()
        inserted = await ingest_range(db_tools, today, today, MARKET)
        print(f'futures daily finished, inserted rows: {inserted}')
        return inserted
    finally:
        await db_tools.close()


async def main():
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else 'backfill'
    if mode == 'backfill':
        await backfill_history()
        return
    if mode == 'daily':
        await sync_today()
        return
    raise ValueError('usage: python futures_main.py [backfill|daily]')


if __name__ == '__main__':
    asyncio.run(main())
