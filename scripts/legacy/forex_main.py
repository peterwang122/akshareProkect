import asyncio
import os
import sys
import time
from datetime import datetime, timedelta

import akshare as ak

from util.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 6
USD_INDEX_SYMBOL_NAME = '\u7f8e\u5143\u6307\u6570'
USD_INDEX_POLL_SECONDS = 1800
FOREX_PROGRESS_LOG = 'forex_progress.log'
FOREX_ERROR_LOG = 'forex_error.log'

COL_CODE = '\u4ee3\u7801'
COL_NAME = '\u540d\u79f0'
COL_DATE = '\u65e5\u671f'
COL_OPEN = '\u4eca\u5f00'
COL_LATEST = '\u6700\u65b0\u4ef7'
COL_HIGH = '\u6700\u9ad8'
COL_LOW = '\u6700\u4f4e'
COL_PRE_CLOSE = '\u6628\u6536'
COL_AMPLITUDE = '\u632f\u5e45'


def save_progress_batch(progress_lines):
    if not progress_lines:
        return
    with open(FOREX_PROGRESS_LOG, 'a', encoding='utf-8') as file:
        file.writelines(progress_lines)


def load_progress():
    if not os.path.exists(FOREX_PROGRESS_LOG):
        return set()
    with open(FOREX_PROGRESS_LOG, 'r', encoding='utf-8') as file:
        return {line.strip() for line in file if line.strip()}


def log_error(symbol_code, trade_date, error_message):
    with open(FOREX_ERROR_LOG, 'a', encoding='utf-8') as file:
        file.write(f'{symbol_code},{trade_date},{error_message}\n')


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                print(f'{func.__name__} attempt {attempt}/{retries} failed: {exc}')
                time.sleep(sleep_seconds)
    raise last_error


def get_forex_spot():
    return fetch_with_retry(ak.forex_spot_em)


def get_forex_history(symbol_code):
    return fetch_with_retry(ak.forex_hist_em, symbol=symbol_code)


def get_usd_index_history():
    return fetch_with_retry(ak.index_global_hist_em, symbol=USD_INDEX_SYMBOL_NAME)


def normalize_symbol_code(value):
    return str(value or '').strip().upper()


def normalize_trade_date(value):
    if value is None:
        return ''
    return str(value).split(' ')[0]


def calculate_amplitude(high_price, low_price, pre_close):
    try:
        if high_price is None or low_price is None or pre_close in (None, 0):
            return None
        return (float(high_price) - float(low_price)) / float(pre_close) * 100
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def build_forex_basic_rows(spot_df):
    basic_rows = []
    seen_codes = set()

    for _, row in spot_df.iterrows():
        symbol_code = normalize_symbol_code(row.get(COL_CODE))
        if not symbol_code or symbol_code in seen_codes:
            continue
        seen_codes.add(symbol_code)
        basic_rows.append({
            'symbol_code': symbol_code,
            'symbol_name': str(row.get(COL_NAME, '')).strip() or None,
            'data_source': 'forex_spot_em',
        })

    return basic_rows


def build_forex_daily_rows(history_df, fallback_symbol_code='', fallback_symbol_name=''):
    rows = []
    for _, row in history_df.iterrows():
        symbol_code = normalize_symbol_code(row.get(COL_CODE) or fallback_symbol_code)
        trade_date = normalize_trade_date(row.get(COL_DATE))
        if not symbol_code or not trade_date:
            continue

        rows.append({
            'symbol_code': symbol_code,
            'symbol_name': str(row.get(COL_NAME) or fallback_symbol_name or '').strip() or None,
            'trade_date': trade_date,
            'open_price': row.get(COL_OPEN),
            'latest_price': row.get(COL_LATEST),
            'high_price': row.get(COL_HIGH),
            'low_price': row.get(COL_LOW),
            'amplitude': row.get(COL_AMPLITUDE),
            'data_source': 'forex_hist_em',
        })

    return rows


def build_forex_spot_daily_rows(spot_df, trade_date):
    rows = []
    for _, row in spot_df.iterrows():
        symbol_code = normalize_symbol_code(row.get(COL_CODE))
        if not symbol_code:
            continue

        high_price = row.get(COL_HIGH)
        low_price = row.get(COL_LOW)
        pre_close = row.get(COL_PRE_CLOSE)
        rows.append({
            'symbol_code': symbol_code,
            'symbol_name': str(row.get(COL_NAME, '')).strip() or None,
            'trade_date': trade_date,
            'open_price': row.get(COL_OPEN),
            'latest_price': row.get(COL_LATEST),
            'high_price': high_price,
            'low_price': low_price,
            'amplitude': calculate_amplitude(high_price, low_price, pre_close),
            'data_source': 'forex_spot_em',
        })

    return rows


def filter_rows_by_end_date(rows, end_date):
    end_date_text = end_date.strftime('%Y-%m-%d')
    return [row for row in rows if row['trade_date'] and row['trade_date'] <= end_date_text]


def build_usd_index_basic_rows():
    return [{
        'symbol_code': 'UDI',
        'symbol_name': USD_INDEX_SYMBOL_NAME,
        'data_source': 'index_global_hist_em',
    }]


def select_latest_usd_rows(rows):
    dated_rows = [row for row in rows if row.get('trade_date')]
    if not dated_rows:
        return []

    dated_rows.sort(key=lambda item: item['trade_date'])
    return dated_rows[-2:] if len(dated_rows) >= 2 else dated_rows


async def fetch_symbol_history_row_for_daily_refresh(symbol_row, target_trade_date, today_text, semaphore):
    symbol_code = symbol_row['symbol_code']
    symbol_name = symbol_row.get('symbol_name') or ''

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_forex_history, symbol_code)

        if history_df is None or history_df.empty:
            return None

        history_rows = build_forex_daily_rows(history_df, symbol_code, symbol_name)
        exact_rows = [row for row in history_rows if row['trade_date'] == target_trade_date]
        if exact_rows:
            return exact_rows[-1]

        closed_rows = [
            row for row in history_rows
            if row['trade_date'] and row['trade_date'] < today_text
        ]
        if not closed_rows:
            return None
        closed_rows.sort(key=lambda row: row['trade_date'])
        return closed_rows[-1]
    except Exception as exc:
        error_message = f'Error fetching history snapshot for {symbol_code}: {exc}'
        print(error_message)
        log_error(symbol_code, target_trade_date, error_message)
        return None


def group_pending_history_refresh_rows(rows):
    grouped = {}
    for row in rows:
        symbol_code = row['symbol_code']
        symbol_group = grouped.setdefault(symbol_code, {
            'symbol_code': symbol_code,
            'symbol_name': row.get('symbol_name') or '',
            'trade_dates': [],
        })
        symbol_group['trade_dates'].append(row['trade_date'])

    for symbol_group in grouped.values():
        symbol_group['trade_dates'] = sorted(set(symbol_group['trade_dates']))
    return list(grouped.values())


async def refresh_symbol_pending_history_rows(symbol_group, db_tools, semaphore):
    symbol_code = symbol_group['symbol_code']
    symbol_name = symbol_group.get('symbol_name') or ''
    target_dates = symbol_group.get('trade_dates') or []
    if not symbol_code or not target_dates:
        return {
            'symbol_code': symbol_code,
            'requested_dates': 0,
            'updated_rows': 0,
            'missing_dates': [],
            'error': None,
        }

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_forex_history, symbol_code)

        if history_df is None or history_df.empty:
            return {
                'symbol_code': symbol_code,
                'requested_dates': len(target_dates),
                'updated_rows': 0,
                'missing_dates': target_dates,
                'error': None,
            }

        history_rows = build_forex_daily_rows(history_df, symbol_code, symbol_name)
        history_map = {row['trade_date']: row for row in history_rows if row.get('trade_date')}
        rows_to_upsert = [history_map[trade_date] for trade_date in target_dates if trade_date in history_map]
        missing_dates = [trade_date for trade_date in target_dates if trade_date not in history_map]

        updated_rows = 0
        if rows_to_upsert:
            updated_rows = await db_tools.upsert_forex_daily_snapshots(rows_to_upsert)

        return {
            'symbol_code': symbol_code,
            'requested_dates': len(target_dates),
            'updated_rows': updated_rows,
            'missing_dates': missing_dates,
            'error': None,
        }
    except Exception as exc:
        error_message = f'Error refreshing pending history for {symbol_code}: {exc}'
        print(error_message)
        log_error(symbol_code, ','.join(target_dates), error_message)
        return {
            'symbol_code': symbol_code,
            'requested_dates': len(target_dates),
            'updated_rows': 0,
            'missing_dates': target_dates,
            'error': str(exc),
        }


async def process_symbol(symbol_row, processed, db_tools, semaphore, progress_lock):
    symbol_code = symbol_row['symbol_code']
    symbol_name = symbol_row.get('symbol_name') or ''

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_forex_history, symbol_code)

        if history_df is None or history_df.empty:
            return

        pending_updates = []
        new_progress_lines = []

        for update in build_forex_daily_rows(history_df, symbol_code, symbol_name):
            progress_key = f"{symbol_code},{update['trade_date']}"
            if progress_key in processed:
                continue
            pending_updates.append(update)
            new_progress_lines.append(f'{progress_key}\n')

        if not pending_updates:
            return

        inserted = await db_tools.batch_forex_daily_data(pending_updates)
        if inserted <= 0:
            return

        async with progress_lock:
            await asyncio.to_thread(save_progress_batch, new_progress_lines)
            processed.update(line.strip() for line in new_progress_lines)

        print(f'{symbol_code} inserted: {inserted}')

    except Exception as exc:
        error_message = f'Error processing {symbol_code}: {exc}'
        print(error_message)
        log_error(symbol_code, 'N/A', error_message)


async def backfill_history(selected_symbols=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    processed = load_progress()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    progress_lock = asyncio.Lock()

    try:
        spot_df = await asyncio.to_thread(get_forex_spot)
        if spot_df is None or spot_df.empty:
            print('No forex spot data fetched.')
            return

        basic_rows = build_forex_basic_rows(spot_df)
        if selected_symbols:
            selected_set = {normalize_symbol_code(item) for item in selected_symbols if normalize_symbol_code(item)}
            basic_rows = [row for row in basic_rows if row['symbol_code'] in selected_set]

        if not basic_rows:
            print('No forex symbols matched the current selection.')
            return

        upserted = await db_tools.upsert_forex_basic_info(basic_rows)
        print(f'forex_basic_info upserted: {upserted}')

        tasks = [
            process_symbol(symbol_row, processed, db_tools, semaphore, progress_lock)
            for symbol_row in basic_rows
        ]
        await asyncio.gather(*tasks)
        print('forex history backfill finished.')
    finally:
        await db_tools.close()


async def backfill_usd_index_history():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        history_df = await asyncio.to_thread(get_usd_index_history)
        if history_df is None or history_df.empty:
            print('No USD index history data fetched.')
            return 0

        yesterday = datetime.now().date() - timedelta(days=1)
        history_rows = build_forex_daily_rows(history_df, 'UDI', USD_INDEX_SYMBOL_NAME)
        history_rows = filter_rows_by_end_date(history_rows, yesterday)

        basic_upserted = await db_tools.upsert_forex_basic_info(build_usd_index_basic_rows())
        inserted = await db_tools.batch_forex_daily_data(history_rows)
        print(
            'usd index history backfill finished, '
            f'forex_basic_info upserted: {basic_upserted}, '
            f'forex_daily_data inserted: {inserted}'
        )
        return inserted
    finally:
        await db_tools.close()


async def sync_usd_index_once():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        history_df = await asyncio.to_thread(get_usd_index_history)
        if history_df is None or history_df.empty:
            print('No USD index history data fetched.')
            return 0

        recent_rows = select_latest_usd_rows(build_forex_daily_rows(history_df, 'UDI', USD_INDEX_SYMBOL_NAME))
        if not recent_rows:
            print('No USD index rows parsed.')
            return 0

        basic_upserted = await db_tools.upsert_forex_basic_info(build_usd_index_basic_rows())
        upserted = await db_tools.upsert_forex_daily_snapshots(recent_rows)
        latest_trade_date = recent_rows[-1]['trade_date']
        print(
            'usd index daily sync finished, '
            f'forex_basic_info upserted: {basic_upserted}, '
            f'forex_daily_data upserted: {upserted}, '
            f'latest_trade_date: {latest_trade_date}'
        )
        return upserted
    finally:
        await db_tools.close()


async def sync_usd_index_continuous(poll_seconds=USD_INDEX_POLL_SECONDS):
    print(f'usd index continuous sync started, interval_seconds: {poll_seconds}')
    while True:
        try:
            await sync_usd_index_once()
        except Exception as exc:
            log_error('UDI', datetime.now().strftime('%Y-%m-%d'), f'usd_index_daily: {exc}')
            print(f'usd index continuous sync failed: {exc}')
        await asyncio.sleep(poll_seconds)


async def sync_daily_from_spot(selected_symbols=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        spot_df = await asyncio.to_thread(get_forex_spot)
        if spot_df is None or spot_df.empty:
            print('No forex spot data fetched.')
            return 0

        basic_rows = build_forex_basic_rows(spot_df)
        if selected_symbols:
            selected_set = {normalize_symbol_code(item) for item in selected_symbols if normalize_symbol_code(item)}
            basic_rows = [row for row in basic_rows if row['symbol_code'] in selected_set]
            spot_df = spot_df[spot_df[COL_CODE].map(normalize_symbol_code).isin(selected_set)]

        if not basic_rows:
            print('No forex symbols matched the current selection.')
            return 0

        today = datetime.now().date()
        today_text = today.strftime('%Y-%m-%d')
        yesterday_text = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        daily_rows = build_forex_spot_daily_rows(spot_df, today_text)

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        history_refresh_rows = await asyncio.gather(*[
            fetch_symbol_history_row_for_daily_refresh(symbol_row, yesterday_text, today_text, semaphore)
            for symbol_row in basic_rows
        ])
        history_refresh_rows = [row for row in history_refresh_rows if row]

        merged_rows = daily_rows + history_refresh_rows
        basic_upserted = await db_tools.upsert_forex_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_forex_daily_snapshots(merged_rows)
        refreshed_dates = sorted({row['trade_date'] for row in history_refresh_rows if row.get('trade_date')})
        print(
            'forex daily finished, '
            f'forex_basic_info upserted: {basic_upserted}, '
            f'forex_daily_data upserted: {daily_upserted}, '
            f'today_rows: {len(daily_rows)}, '
            f'history_refresh_rows: {len(history_refresh_rows)}, '
            f'history_refresh_dates: {",".join(refreshed_dates) if refreshed_dates else "NONE"}'
        )
        return daily_upserted
    finally:
        await db_tools.close()


async def repair_unrefreshed_history_rows(selected_symbols=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        today_text = datetime.now().date().strftime('%Y-%m-%d')
        pending_rows = await db_tools.get_forex_rows_pending_history_refresh(today_text, selected_symbols)
        if not pending_rows:
            print('No forex rows pending history refresh.')
            return 0

        grouped_rows = group_pending_history_refresh_rows(pending_rows)
        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        results = await asyncio.gather(*[
            refresh_symbol_pending_history_rows(symbol_group, db_tools, semaphore)
            for symbol_group in grouped_rows
        ])

        total_updated = sum(result['updated_rows'] for result in results)
        total_missing = sum(len(result['missing_dates']) for result in results)
        error_symbols = [result['symbol_code'] for result in results if result.get('error')]

        print(
            'forex history repair finished, '
            f'pending_rows: {len(pending_rows)}, '
            f'symbols: {len(grouped_rows)}, '
            f'updated_rows: {total_updated}, '
            f'missing_dates: {total_missing}, '
            f'error_symbols: {",".join(error_symbols) if error_symbols else "NONE"}'
        )

        if total_missing:
            preview_missing = []
            for result in results:
                if not result['missing_dates']:
                    continue
                preview_missing.append(
                    f"{result['symbol_code']}:{'/'.join(result['missing_dates'][:3])}"
                )
                if len(preview_missing) >= 10:
                    break
            print(
                'forex history repair still missing exact history rows for: '
                + ', '.join(preview_missing)
            )

        return total_updated
    finally:
        await db_tools.close()


async def main():
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else 'backfill'
    selected_symbols = sys.argv[2:] if len(sys.argv) > 2 else []

    if command == 'backfill':
        await backfill_history(selected_symbols)
        return
    if command == 'daily':
        await sync_daily_from_spot(selected_symbols)
        return
    if command == 'usd-backfill':
        await backfill_usd_index_history()
        return
    if command == 'usd-daily':
        await sync_usd_index_continuous()
        return
    if command == 'usd-once':
        await sync_usd_index_once()
        return
    if command == 'repair-history':
        await repair_unrefreshed_history_rows(selected_symbols)
        return

    raise ValueError(
        'usage: python forex_main.py [backfill|daily|repair-history] [SYMBOL ...] '
        '| [usd-backfill|usd-daily|usd-once]'
    )


if __name__ == '__main__':
    asyncio.run(main())
