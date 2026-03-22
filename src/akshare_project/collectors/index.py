import asyncio
import os
import re
import sys
import time
from datetime import datetime

import akshare as ak

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 5
LOGGER = get_logger('index')
PROGRESS_STORE = ProgressStore('index')

COL_CODE = '\u4ee3\u7801'
COL_NAME = '\u540d\u79f0'
COL_DATE = '\u65e5\u671f'
COL_OPEN = '\u5f00\u76d8'
COL_CLOSE = '\u6536\u76d8'
COL_LATEST = '\u6700\u65b0\u4ef7'
COL_HIGH = '\u6700\u9ad8'
COL_LOW = '\u6700\u4f4e'
COL_PRE_CLOSE = '\u6628\u6536'
COL_SPOT_OPEN = '\u4eca\u5f00'
COL_VOLUME = '\u6210\u4ea4\u91cf'
COL_AMOUNT = '\u6210\u4ea4\u989d'
COL_AMPLITUDE = '\u632f\u5e45'
COL_CHANGE_RATE = '\u6da8\u8dcc\u5e45'
COL_CHANGE_AMOUNT = '\u6da8\u8dcc\u989d'
COL_TURNOVER_RATE = '\u6362\u624b\u7387'


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def parse_index_code(raw_code):
    code = str(raw_code or '').strip().lower()
    if not code:
        return '', '', ''

    match = re.match(r'([a-z]+)?(\d+)', code)
    if not match:
        return code, '', ''

    market = match.group(1) or ''
    simple_code = match.group(2)
    return code, simple_code, market


def save_progress_batch(progress_lines):
    PROGRESS_STORE.append_lines(progress_lines)


def load_progress():
    return PROGRESS_STORE.load()


def log_error(index_code, trade_date, error_message):
    LOGGER.error('%s,%s,%s', index_code, trade_date, error_message)


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    return shared_fetch_with_retry(
        func,
        *args,
        retries=retries,
        sleep_seconds=sleep_seconds,
        logger=LOGGER,
        **kwargs,
    )


def get_all_index_spot():
    return fetch_with_retry(ak.stock_zh_index_spot_sina)


def get_index_history(index_code, simple_code, end_date):
    last_error = None

    try:
        history_df = fetch_with_retry(
            ak.index_zh_a_hist,
            symbol=simple_code,
            period='daily',
            start_date='19700101',
            end_date=end_date,
        )
        if history_df is not None and not history_df.empty:
            return history_df, 'index_zh_a_hist'
    except Exception as exc:
        last_error = exc

    try:
        history_df = fetch_with_retry(ak.stock_zh_index_daily_em, symbol=index_code)
        if history_df is not None and not history_df.empty:
            return history_df, 'stock_zh_index_daily_em'
    except Exception as exc:
        last_error = exc

    if last_error is not None:
        raise last_error
    raise ValueError(f'No history data returned for {index_code}')


def build_index_basic_rows(spot_df):
    basic_rows = []
    for _, row in spot_df.iterrows():
        index_code, simple_code, market = parse_index_code(row.get(COL_CODE))
        if not index_code:
            continue
        basic_rows.append({
            'index_code': index_code,
            'simple_code': simple_code,
            'market': market,
            'index_name': str(row.get(COL_NAME, '')).strip(),
            'data_source': 'stock_zh_index_spot_sina',
        })
    return basic_rows


def normalize_trade_date(value):
    if value is None:
        return ''
    return str(value).split(' ')[0]


def first_value(row, candidates):
    for candidate in candidates:
        if candidate in row:
            value = row[candidate]
            if value is not None:
                return value
    return None


def calculate_amplitude(high_price, low_price, pre_close):
    try:
        if high_price is None or low_price is None or pre_close in (None, 0):
            return None
        return (float(high_price) - float(low_price)) / float(pre_close) * 100
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def build_index_daily_rows(index_code, history_df, source_name):
    daily_rows = []
    for _, row in history_df.iterrows():
        trade_date = normalize_trade_date(first_value(row, [COL_DATE, 'date']))
        if not trade_date:
            continue

        daily_rows.append({
            'index_code': index_code,
            'open_price': first_value(row, [COL_OPEN, 'open']),
            'close_price': first_value(row, [COL_CLOSE, 'close']),
            'high_price': first_value(row, [COL_HIGH, 'high']),
            'low_price': first_value(row, [COL_LOW, 'low']),
            'volume': first_value(row, [COL_VOLUME, 'volume']),
            'turnover': first_value(row, [COL_AMOUNT, 'amount']),
            'amplitude': first_value(row, [COL_AMPLITUDE]),
            'price_change_rate': first_value(row, [COL_CHANGE_RATE]),
            'price_change_amount': first_value(row, [COL_CHANGE_AMOUNT]),
            'turnover_rate': first_value(row, [COL_TURNOVER_RATE]),
            'trade_date': trade_date,
            'data_source': source_name,
        })
    return daily_rows


def build_index_spot_daily_rows(spot_df, trade_date):
    rows = []
    for _, row in spot_df.iterrows():
        index_code, _, _ = parse_index_code(row.get(COL_CODE))
        if not index_code:
            continue

        high_price = row.get(COL_HIGH)
        low_price = row.get(COL_LOW)
        pre_close = row.get(COL_PRE_CLOSE)
        rows.append({
            'index_code': index_code,
            'open_price': row.get(COL_SPOT_OPEN),
            'close_price': row.get(COL_LATEST),
            'high_price': high_price,
            'low_price': low_price,
            'volume': row.get(COL_VOLUME),
            'turnover': row.get(COL_AMOUNT),
            'amplitude': calculate_amplitude(high_price, low_price, pre_close),
            'price_change_rate': row.get(COL_CHANGE_RATE),
            'price_change_amount': row.get(COL_CHANGE_AMOUNT),
            'turnover_rate': None,
            'trade_date': trade_date,
            'data_source': 'stock_zh_index_spot_sina',
        })
    return rows


async def process_index(index_row, processed, db_tools, semaphore, progress_lock, end_date):
    index_code = index_row['index_code']
    simple_code = index_row['simple_code']

    if not simple_code:
        log_error(index_code, 'N/A', 'missing simple index code')
        return

    try:
        async with semaphore:
            history_df, source_name = await asyncio.to_thread(
                get_index_history,
                index_code,
                simple_code,
                end_date,
            )

        if history_df is None or history_df.empty:
            return

        pending_updates = []
        new_progress_lines = []

        for update in build_index_daily_rows(index_code, history_df, source_name):
            progress_key = f"{index_code},{update['trade_date']}"
            if progress_key in processed:
                continue
            pending_updates.append(update)
            new_progress_lines.append(f'{progress_key}\n')

        if not pending_updates:
            return

        inserted = await db_tools.batch_index_daily_data(pending_updates)
        if inserted <= 0:
            return

        async with progress_lock:
            await asyncio.to_thread(save_progress_batch, new_progress_lines)
            processed.update(line.strip() for line in new_progress_lines)

    except Exception as exc:
        error_message = f'Error processing {index_code}: {exc}'
        print(error_message)
        log_error(index_code, 'N/A', error_message)


async def backfill_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    processed = load_progress()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    progress_lock = asyncio.Lock()
    end_date = datetime.now().strftime('%Y%m%d')

    try:
        spot_df = await asyncio.to_thread(get_all_index_spot)
        if spot_df is None or spot_df.empty:
            print('No index spot data fetched.')
            return

        index_rows = build_index_basic_rows(spot_df)
        upserted = await db_tools.upsert_index_basic_info(index_rows)
        print(f'index_basic_info upserted: {upserted}')

        tasks = [
            process_index(index_row, processed, db_tools, semaphore, progress_lock, end_date)
            for index_row in index_rows
        ]
        await asyncio.gather(*tasks)
        print('index history backfill finished.')
    finally:
        await db_tools.close()


async def sync_daily_from_spot():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        spot_df = await asyncio.to_thread(get_all_index_spot)
        if spot_df is None or spot_df.empty:
            print('No index spot data fetched.')
            return 0

        trade_date = datetime.now().strftime('%Y-%m-%d')
        basic_rows = build_index_basic_rows(spot_df)
        daily_rows = build_index_spot_daily_rows(spot_df, trade_date)

        basic_upserted = await db_tools.upsert_index_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_index_daily_snapshots(daily_rows)
        print(
            'index daily finished, '
            f'index_basic_info upserted: {basic_upserted}, '
            f'index_daily_data upserted: {daily_upserted}'
        )
        return daily_upserted
    finally:
        await db_tools.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else 'backfill'

    if command == 'backfill':
        await backfill_history()
        return
    if command == 'daily':
        await sync_daily_from_spot()
        return

    raise ValueError('supported commands: backfill, daily')


if __name__ == '__main__':
    asyncio.run(main())
