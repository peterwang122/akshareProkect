import asyncio
import os
import re
import time
from datetime import datetime

import akshare as ak

from util.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 5
INDEX_PROGRESS_LOG = 'index_progress.log'
INDEX_ERROR_LOG = 'index_error.log'
COL_CODE = '\u4ee3\u7801'
COL_NAME = '\u540d\u79f0'
COL_DATE = '\u65e5\u671f'
COL_OPEN = '\u5f00\u76d8'
COL_CLOSE = '\u6536\u76d8'
COL_HIGH = '\u6700\u9ad8'
COL_LOW = '\u6700\u4f4e'
COL_VOLUME = '\u6210\u4ea4\u91cf'
COL_AMOUNT = '\u6210\u4ea4\u989d'
COL_AMPLITUDE = '\u632f\u5e45'
COL_CHANGE_RATE = '\u6da8\u8dcc\u5e45'
COL_CHANGE_AMOUNT = '\u6da8\u8dcc\u989d'
COL_TURNOVER_RATE = '\u6362\u624b\u7387'


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
    if not progress_lines:
        return
    with open(INDEX_PROGRESS_LOG, 'a') as f:
        f.writelines(progress_lines)


def load_progress():
    if not os.path.exists(INDEX_PROGRESS_LOG):
        return set()
    with open(INDEX_PROGRESS_LOG, 'r') as f:
        return {line.strip() for line in f if line.strip()}


def log_error(index_code, trade_date, error_message):
    with open(INDEX_ERROR_LOG, 'a') as f:
        f.write(f'{index_code},{trade_date},{error_message}\n')


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
    except Exception as e:
        last_error = e

    try:
        history_df = fetch_with_retry(ak.stock_zh_index_daily_em, symbol=index_code)
        if history_df is not None and not history_df.empty:
            return history_df, 'stock_zh_index_daily_em'
    except Exception as e:
        last_error = e

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

    except Exception as e:
        error_message = f'Error processing {index_code}: {e}'
        print(error_message)
        log_error(index_code, 'N/A', error_message)


async def run():
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
    finally:
        await db_tools.close()


if __name__ == '__main__':
    asyncio.run(run())
