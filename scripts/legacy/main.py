import asyncio
import json
import os
import random
import sys
import time
from http.client import RemoteDisconnected
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd
import requests

from util.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 8
STOCK_HISTORY_MAX_CONCURRENCY = 3
RETRY_SLEEP_CAP_SECONDS = 30
REQUEST_JITTER_MAX_SECONDS = 1.5
STOCK_BATCH_SIZE = 100
STOCK_BATCH_PAUSE_MIN_SECONDS = 20
STOCK_BATCH_PAUSE_MAX_SECONDS = 40
DEFAULT_STOCK_HISTORY_START_DATE = '19900101'

PROGRESS_LOG = 'progress.log'
ERROR_LOG = 'error.log'
STOCK_MISSING_DATE_TASK_NAME = 'stock_missing_date_backfill'

COL_CODE = '\u4ee3\u7801'
COL_NAME = '\u540d\u79f0'
COL_DATE = '\u65e5\u671f'
COL_OPEN = '\u5f00\u76d8'
COL_CLOSE = '\u6536\u76d8'
COL_LATEST = '\u6700\u65b0\u4ef7'
COL_HIGH = '\u6700\u9ad8'
COL_LOW = '\u6700\u4f4e'
COL_SPOT_OPEN = '\u4eca\u5f00'
COL_VOLUME = '\u6210\u4ea4\u91cf'
COL_AMOUNT = '\u6210\u4ea4\u989d'
COL_AMPLITUDE = '\u632f\u5e45'
COL_CHANGE_RATE = '\u6da8\u8dcc\u5e45'
COL_CHANGE_AMOUNT = '\u6da8\u8dcc\u989d'
COL_TURNOVER_RATE = '\u6362\u624b\u7387'
COL_PE_TTM = '\u5e02\u76c8\u7387-\u52a8\u6001'
COL_PB = '\u5e02\u51c0\u7387'
COL_TOTAL_MARKET_VALUE = '\u603b\u5e02\u503c'
COL_CIRCULATING_MARKET_VALUE = '\u6d41\u901a\u5e02\u503c'
LISTING_DATE_ITEM = '\u4e0a\u5e02\u65f6\u95f4'
HISTORY_COL_STOCK_CODE = '\u80a1\u7968\u4ee3\u7801'


def normalize_stock_code(stock_code):
    code = str(stock_code or '').strip()
    if '.' in code:
        code = code.split('.')[0]
    code = ''.join(ch for ch in code if ch.isdigit())
    if not code:
        return ''
    return code.zfill(6)


def save_progress_batch(progress_lines):
    if not progress_lines:
        return
    with open(PROGRESS_LOG, 'a', encoding='utf-8') as file:
        file.writelines(progress_lines)


def load_progress():
    if not os.path.exists(PROGRESS_LOG):
        return set()
    with open(PROGRESS_LOG, 'r', encoding='utf-8') as file:
        return {line.strip() for line in file if line.strip()}


def log_error(stock_code, trade_date, error_message):
    with open(ERROR_LOG, 'a', encoding='utf-8') as file:
        file.write(f'{stock_code},{trade_date},{error_message}\n')


def classify_fetch_error(exc):
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout, RemoteDisconnected)):
        return 'network'
    if isinstance(exc, ValueError):
        return 'data'
    return 'unexpected'


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                wait_seconds = min(sleep_seconds * (2 ** (attempt - 1)), RETRY_SLEEP_CAP_SECONDS)
                wait_seconds += random.uniform(0, REQUEST_JITTER_MAX_SECONDS)
                error_category = classify_fetch_error(exc)
                print(
                    f'{func.__name__} attempt {attempt}/{retries} failed '
                    f'[{error_category}]: {exc}; '
                    f'retrying in {wait_seconds:.1f}s'
                )
                time.sleep(wait_seconds)
    raise last_error


def throttle_stock_request():
    time.sleep(random.uniform(0.2, REQUEST_JITTER_MAX_SECONDS))


def get_stock_history_start_date(stock_code):
    throttle_stock_request()
    try:
        info_df = fetch_with_retry(ak.stock_individual_info_em, symbol=stock_code)
        listing_date_series = info_df.loc[info_df['item'] == LISTING_DATE_ITEM, 'value']
        if listing_date_series.empty:
            raise ValueError(f'listing date not found for {stock_code}')
        return str(listing_date_series.values[0])
    except Exception as exc:
        error_category = classify_fetch_error(exc)
        print(
            f'stock_individual_info_em fallback for {stock_code} '
            f'[{error_category}]: {exc}; using {DEFAULT_STOCK_HISTORY_START_DATE}'
        )
        return DEFAULT_STOCK_HISTORY_START_DATE


def get_stock_history(stock_code, end_date):
    listing_date = get_stock_history_start_date(stock_code)

    return fetch_with_retry(
        ak.stock_zh_a_hist,
        symbol=stock_code,
        period='daily',
        start_date=listing_date,
        end_date=end_date,
        adjust='hfq',
    )


def get_stock_spot():
    return fetch_with_retry(ak.stock_zh_a_spot_em)


def get_stock_history_by_range(stock_code, start_date, end_date):
    throttle_stock_request()
    return fetch_with_retry(
        ak.stock_zh_a_hist,
        symbol=stock_code,
        period='daily',
        start_date=start_date,
        end_date=end_date,
        adjust='hfq',
    )


def build_basic_rows_from_spot(spot_df):
    rows = []
    for _, row in spot_df.iterrows():
        stock_code = normalize_stock_code(row.get(COL_CODE))
        if not stock_code:
            continue
        rows.append({
            'stock_code': stock_code,
            'stock_name': str(row.get(COL_NAME, '')).strip(),
        })
    return rows


def build_valuation_rows_from_spot(spot_df):
    rows = []
    for _, row in spot_df.iterrows():
        stock_code = normalize_stock_code(row.get(COL_CODE))
        if not stock_code:
            continue
        rows.append({
            'stock_code': stock_code,
            'pe_ttm': row.get(COL_PE_TTM),
            'pb': row.get(COL_PB),
            'total_market_value': row.get(COL_TOTAL_MARKET_VALUE),
            'circulating_market_value': row.get(COL_CIRCULATING_MARKET_VALUE),
        })
    return rows


def build_stock_daily_rows_from_spot(spot_df, trade_date):
    rows = []
    for _, row in spot_df.iterrows():
        stock_code = normalize_stock_code(row.get(COL_CODE))
        if not stock_code:
            continue
        rows.append({
            'stock_code': stock_code,
            'open_price': row.get(COL_SPOT_OPEN),
            'close_price': row.get(COL_LATEST),
            'high_price': row.get(COL_HIGH),
            'low_price': row.get(COL_LOW),
            'volume': row.get(COL_VOLUME),
            'turnover': row.get(COL_AMOUNT),
            'amplitude': row.get(COL_AMPLITUDE),
            'price_change_rate': row.get(COL_CHANGE_RATE),
            'price_change_amount': row.get(COL_CHANGE_AMOUNT),
            'turnover_rate': row.get(COL_TURNOVER_RATE),
            'date': trade_date,
        })
    return rows


def build_stock_history_rows(history_df, fallback_stock_code=''):
    rows = []
    for _, row in history_df.iterrows():
        trade_date = str(row.get(COL_DATE))
        if not trade_date:
            continue
        rows.append({
            'stock_code': normalize_stock_code(row.get(HISTORY_COL_STOCK_CODE, fallback_stock_code)) or fallback_stock_code,
            'open_price': row.get(COL_OPEN),
            'close_price': row.get(COL_CLOSE),
            'high_price': row.get(COL_HIGH),
            'low_price': row.get(COL_LOW),
            'volume': row.get(COL_VOLUME),
            'turnover': row.get(COL_AMOUNT),
            'amplitude': row.get(COL_AMPLITUDE),
            'price_change_rate': row.get(COL_CHANGE_RATE),
            'price_change_amount': row.get(COL_CHANGE_AMOUNT),
            'turnover_rate': row.get(COL_TURNOVER_RATE),
            'date': trade_date,
        })
    return rows


def normalize_trade_date_text(value):
    value = str(value or '').strip()
    if not value:
        raise ValueError('trade_date is required')
    return datetime.strptime(value, '%Y-%m-%d').strftime('%Y-%m-%d')


def format_ak_date(value):
    return normalize_trade_date_text(value).replace('-', '')


def build_stock_missing_task_key(target_date, stock_code):
    return f'{target_date}:{normalize_stock_code(stock_code)}'


def build_stock_missing_payload(target_date, stock_code):
    return {
        'task_name': STOCK_MISSING_DATE_TASK_NAME,
        'stage': 'daily',
        'target_date': target_date,
        'stock_code': normalize_stock_code(stock_code),
    }


def load_stock_items():
    df = pd.read_csv('allstock_em.csv', dtype={COL_CODE: str})
    return json.loads(df.to_json(orient='records'))


async def gather_in_stock_batches(items, worker_factory):
    results = []
    total = len(items)
    if total == 0:
        return results

    for start in range(0, total, STOCK_BATCH_SIZE):
        batch = items[start:start + STOCK_BATCH_SIZE]
        results.extend(await asyncio.gather(*[worker_factory(item) for item in batch]))

        processed_count = min(start + STOCK_BATCH_SIZE, total)
        print(f'stock batch progress: {processed_count}/{total}')

        if processed_count < total:
            pause_seconds = random.uniform(STOCK_BATCH_PAUSE_MIN_SECONDS, STOCK_BATCH_PAUSE_MAX_SECONDS)
            print(f'stock batch pause: sleeping {pause_seconds:.1f}s before next batch')
            await asyncio.sleep(pause_seconds)

    return results


async def record_stock_missing_failure(db_tools, target_date, stock_code, error_message):
    await db_tools.upsert_failed_task({
        'task_name': STOCK_MISSING_DATE_TASK_NAME,
        'task_stage': 'daily',
        'task_key': build_stock_missing_task_key(target_date, stock_code),
        'payload_json': build_stock_missing_payload(target_date, stock_code),
        'error_message': error_message,
    })


async def resolve_stock_missing_failure(db_tools, target_date, stock_code):
    await db_tools.resolve_failed_task_by_identity(
        STOCK_MISSING_DATE_TASK_NAME,
        'daily',
        build_stock_missing_task_key(target_date, stock_code),
    )


async def process_stock_daily_from_history(item, semaphore, end_date):
    raw_code = item.get(COL_CODE)
    stock_code = normalize_stock_code(raw_code)
    if not stock_code:
        log_error(str(raw_code), 'N/A', 'invalid stock code')
        return None

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_stock_history, stock_code, end_date)

        if history_df is None or history_df.empty:
            return None

        history_rows = build_stock_history_rows(history_df, stock_code)
        if not history_rows:
            return None
        history_rows.sort(key=lambda row: row['date'])
        return history_rows[-1]
    except Exception as exc:
        error_message = f'Error processing daily history for {stock_code}: {exc}'
        print(error_message)
        log_error(stock_code, 'N/A', error_message)
        return None


async def sync_daily_from_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        df = pd.read_csv('allstock_em.csv', dtype={COL_CODE: str})
        df_data = json.loads(df.to_json(orient='records'))
        if not df_data:
            print('No stock codes found in allstock_em.csv.')
            return 0

        semaphore = asyncio.Semaphore(STOCK_HISTORY_MAX_CONCURRENCY)
        end_date = datetime.now().strftime('%Y%m%d')
        gathered_rows = await gather_in_stock_batches(
            df_data,
            lambda item: process_stock_daily_from_history(item, semaphore, end_date),
        )
        daily_rows = [row for row in gathered_rows if row]
        if not daily_rows:
            print('No stock history rows fetched for daily sync.')
            return 0

        inserted_rows, updated_rows = await db_tools.upsert_stock_daily_snapshots(daily_rows)

        print(
            'stock daily finished, '
            f'stock_data inserted: {inserted_rows}, '
            f'stock_data updated: {updated_rows}'
        )
        return inserted_rows + updated_rows
    finally:
        await db_tools.close()


async def sync_daily_from_spot():
    return await sync_daily_from_history()


async def process_stock_missing_date(stock_code, target_date, db_tools, semaphore, swallow_exceptions=True):
    stock_code = normalize_stock_code(stock_code)
    if not stock_code:
        raise ValueError('invalid stock code')

    target_date_text = normalize_trade_date_text(target_date)
    target_dt = datetime.strptime(target_date_text, '%Y-%m-%d')
    start_date = (target_dt - timedelta(days=7)).strftime('%Y%m%d')
    end_date = target_dt.strftime('%Y%m%d')

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_stock_history_by_range, stock_code, start_date, end_date)

        if history_df is None or history_df.empty:
            raise ValueError(f'empty history data for {stock_code}')

        history_rows = build_stock_history_rows(history_df, stock_code)
        if not history_rows:
            raise ValueError(f'no parsed history rows for {stock_code}')

        target_rows = [row for row in history_rows if row['date'] == target_date_text]
        if not target_rows:
            raise ValueError(f'target trade_date {target_date_text} not found for {stock_code}')

        inserted = await db_tools.batch_stock_info(history_rows)
        await resolve_stock_missing_failure(db_tools, target_date_text, stock_code)
        print(f'stock missing-date filled: {stock_code}, trade_date={target_date_text}, inserted={inserted}')
        return inserted
    except Exception as exc:
        await record_stock_missing_failure(db_tools, target_date_text, stock_code, str(exc))
        error_message = f'{stock_code},{target_date_text},{exc}'
        print(error_message)
        log_error(stock_code, target_date_text, str(exc))
        if swallow_exceptions:
            return 0
        raise


async def process_stock(item, processed, db_tools, semaphore, progress_lock, end_date):
    raw_code = item.get(COL_CODE)
    stock_code = normalize_stock_code(raw_code)
    if not stock_code:
        log_error(str(raw_code), 'N/A', 'invalid stock code')
        return

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_stock_history, stock_code, end_date)

        if history_df is None or history_df.empty:
            return

        pending_updates = []
        new_progress_lines = []

        for _, row in history_df.iterrows():
            trade_date = str(row.get(COL_DATE))
            progress_key = f'{stock_code},{trade_date}'
            if progress_key in processed:
                continue

            pending_updates.append({
                'stock_code': normalize_stock_code(row.get(HISTORY_COL_STOCK_CODE, stock_code)) or stock_code,
                'open_price': row.get(COL_OPEN),
                'close_price': row.get(COL_CLOSE),
                'high_price': row.get(COL_HIGH),
                'low_price': row.get(COL_LOW),
                'volume': row.get(COL_VOLUME),
                'turnover': row.get(COL_AMOUNT),
                'amplitude': row.get(COL_AMPLITUDE),
                'price_change_rate': row.get(COL_CHANGE_RATE),
                'price_change_amount': row.get(COL_CHANGE_AMOUNT),
                'turnover_rate': row.get(COL_TURNOVER_RATE),
                'date': trade_date,
            })
            new_progress_lines.append(f'{progress_key}\n')

        if not pending_updates:
            return

        await db_tools.batch_stock_info(pending_updates)

        async with progress_lock:
            await asyncio.to_thread(save_progress_batch, new_progress_lines)
            processed.update(line.strip() for line in new_progress_lines)

    except Exception as exc:
        error_message = f'Error processing {stock_code}: {exc}'
        print(error_message)
        log_error(stock_code, 'N/A', error_message)


async def backfill_history():
    df_data = load_stock_items()
    processed = load_progress()

    db_tools = DbTools()
    await db_tools.init_pool()

    semaphore = asyncio.Semaphore(STOCK_HISTORY_MAX_CONCURRENCY)
    progress_lock = asyncio.Lock()
    end_date = datetime.now().strftime('%Y%m%d')

    try:
        await gather_in_stock_batches(
            df_data,
            lambda item: process_stock(item, processed, db_tools, semaphore, progress_lock, end_date),
        )
        print('stock history backfill finished.')
    finally:
        await db_tools.close()


async def backfill_missing_trade_date_once(target_date):
    target_date = normalize_trade_date_text(target_date)
    stock_items = load_stock_items()
    stock_codes = [normalize_stock_code(item.get(COL_CODE)) for item in stock_items]
    stock_codes = [code for code in stock_codes if code]

    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(STOCK_HISTORY_MAX_CONCURRENCY)

    try:
        existing_codes = await db_tools.get_existing_stock_codes_on_date(target_date, stock_codes)
        missing_codes = [code for code in stock_codes if code not in existing_codes]
        if not missing_codes:
            print(f'No missing stock_code found for {target_date}.')
            return 0

        results = await gather_in_stock_batches(
            missing_codes,
            lambda stock_code: process_stock_missing_date(
                stock_code,
                target_date,
                db_tools,
                semaphore,
                swallow_exceptions=True,
            ),
        )
        inserted = sum(results)
        print(
            f'stock missing-date backfill finished, '
            f'trade_date={target_date}, '
            f'missing_count={len(missing_codes)}, '
            f'inserted_rows={inserted}'
        )
        return inserted
    finally:
        await db_tools.close()


async def retry_missing_trade_date_failures_once(target_date):
    target_date = normalize_trade_date_text(target_date)
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(STOCK_HISTORY_MAX_CONCURRENCY)

    try:
        failed_tasks = await db_tools.get_pending_failed_tasks(task_name=STOCK_MISSING_DATE_TASK_NAME)
        failed_tasks = [
            task for task in failed_tasks
            if str((task.get('payload') or {}).get('target_date', '')).strip() == target_date
        ]
        if not failed_tasks:
            print(f'No pending stock missing-date failed tasks for {target_date}.')
            return 0

        total_inserted = 0
        for failure in failed_tasks:
            payload = failure.get('payload') or {}
            stock_code = payload.get('stock_code')
            try:
                total_inserted += await process_stock_missing_date(
                    stock_code,
                    target_date,
                    db_tools,
                    semaphore,
                    swallow_exceptions=False,
                )
                await db_tools.mark_failed_task_retry_result(failure['id'], success=True)
            except Exception as exc:
                await db_tools.mark_failed_task_retry_result(failure['id'], success=False, error_message=str(exc))

        print(
            f'stock missing-date retry round finished, '
            f'trade_date={target_date}, '
            f'pending_count={len(failed_tasks)}, '
            f'inserted_rows={total_inserted}'
        )
        return total_inserted
    finally:
        await db_tools.close()


async def repair_missing_trade_date_until_complete(target_date):
    target_date = normalize_trade_date_text(target_date)
    stock_items = load_stock_items()
    stock_codes = [normalize_stock_code(item.get(COL_CODE)) for item in stock_items]
    stock_codes = [code for code in stock_codes if code]
    round_no = 0
    total_inserted = 0

    while True:
        round_no += 1
        print(f'stock missing-date repair round {round_no} started: trade_date={target_date}')
        total_inserted += await backfill_missing_trade_date_once(target_date)
        total_inserted += await retry_missing_trade_date_failures_once(target_date)

        db_tools = DbTools()
        await db_tools.init_pool()
        try:
            existing_codes = await db_tools.get_existing_stock_codes_on_date(target_date, stock_codes)
            pending = await db_tools.get_pending_failed_tasks(task_name=STOCK_MISSING_DATE_TASK_NAME)
            pending = [
                task for task in pending
                if str((task.get('payload') or {}).get('target_date', '')).strip() == target_date
            ]
        finally:
            await db_tools.close()

        remaining_count = len([code for code in stock_codes if code not in existing_codes])
        print(
            f'stock missing-date repair round {round_no} finished, '
            f'trade_date={target_date}, '
            f'remaining_missing={remaining_count}, '
            f'pending_failures={len(pending)}'
        )

        if remaining_count == 0:
            print(
                f'stock missing-date repair completed, '
                f'trade_date={target_date}, '
                f'total_inserted={total_inserted}'
            )
            return total_inserted

        await asyncio.sleep(API_RETRY_SLEEP_SECONDS)


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else 'backfill'
    trade_date = sys.argv[2].strip() if len(sys.argv) > 2 else None

    if command == 'backfill':
        await backfill_history()
        return
    if command == 'daily':
        await sync_daily_from_history()
        return
    if command == 'repair-missing-date':
        if not trade_date:
            raise ValueError('usage: python main.py repair-missing-date YYYY-MM-DD')
        await repair_missing_trade_date_until_complete(trade_date)
        return

    raise ValueError('supported commands: backfill, daily, repair-missing-date')


if __name__ == '__main__':
    asyncio.run(main())
