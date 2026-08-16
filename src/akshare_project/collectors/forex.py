import asyncio
import re
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 6
USD_INDEX_SYMBOL_NAME = '\u7f8e\u5143\u6307\u6570'
USD_INDEX_POLL_SECONDS = 1800
SINA_FOREX_DAY_KLINE_URL = (
    'https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/'
    'var%20_FX=/NewForexService.getDayKLine'
)
SINA_FOREX_REALTIME_URL = 'https://hq.sinajs.cn/'
SINA_FOREX_DATA_SOURCE = 'sina_forex_day_kline'
SINA_USD_INDEX_DATA_SOURCE = 'sina_dxy_day_kline'
SINA_FOREX_INTRADAY_DATA_SOURCE = 'sina_forex_realtime_intraday'
SINA_USD_INDEX_INTRADAY_DATA_SOURCE = 'sina_dxy_realtime_intraday'
SINA_FOREX_SYMBOLS = {
    'USDCNH': 'fx_susdcnh',
    'CNHJPY': 'fx_scnhjpy',
    'CNHEUR': 'fx_seurcnh',
    'CNHHKD': 'fx_scnhhkd',
    'USDHKD': 'fx_susdhkd',
    'USDJPY': 'fx_susdjpy',
    'USDEUR': 'fx_susdeur',
    'UDI': 'DINIW',
}
SINA_INVERTED_SYMBOLS = {'CNHEUR'}
SINA_SYMBOL_TO_CODE = {
    sina_symbol.lower(): symbol_code
    for symbol_code, sina_symbol in SINA_FOREX_SYMBOLS.items()
}
FOREX_SYMBOL_NAMES = {
    'USDCNH': '美元兑离岸人民币',
    'CNHJPY': '离岸人民币兑日元',
    'CNHEUR': '离岸人民币兑欧元',
    'CNHHKD': '离岸人民币兑港币',
    'USDHKD': '美元兑港币',
    'USDJPY': '美元兑日元',
    'USDEUR': '美元兑欧元',
    'UDI': USD_INDEX_SYMBOL_NAME,
}
DAILY_SYNC_SYMBOLS = [
    'USDCNH',
    'CNHJPY',
    'CNHEUR',
    'CNHHKD',
    'USDHKD',
    'USDJPY',
    'USDEUR',
]
LOGGER = get_logger('forex')
PROGRESS_STORE = ProgressStore('forex')

COL_CODE = '\u4ee3\u7801'
COL_NAME = '\u540d\u79f0'
COL_DATE = '\u65e5\u671f'
COL_OPEN = '\u4eca\u5f00'
COL_LATEST = '\u6700\u65b0\u4ef7'
COL_HIGH = '\u6700\u9ad8'
COL_LOW = '\u6700\u4f4e'
COL_PRE_CLOSE = '\u6628\u6536'
COL_AMPLITUDE = '\u632f\u5e45'


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def save_progress_batch(progress_lines):
    PROGRESS_STORE.append_lines(progress_lines)


def load_progress():
    return PROGRESS_STORE.load()


def log_error(symbol_code, trade_date, error_message):
    LOGGER.error('%s,%s,%s', symbol_code, trade_date, error_message)


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    return shared_fetch_with_retry(
        func,
        *args,
        retries=retries,
        sleep_seconds=sleep_seconds,
        logger=LOGGER,
        caller_name=LOGGER.name,
        **kwargs,
    )


def get_forex_spot():
    return pd.DataFrame([
        {
            COL_CODE: symbol_code,
            COL_NAME: FOREX_SYMBOL_NAMES[symbol_code],
        }
        for symbol_code in DAILY_SYNC_SYMBOLS
    ])


def get_forex_history(symbol_code):
    normalized_code = normalize_symbol_code(symbol_code)
    if normalized_code == 'UDI':
        return get_usd_index_history()
    return fetch_with_retry(fetch_sina_history_once, normalized_code)


def get_usd_index_history():
    return fetch_with_retry(fetch_sina_history_once, 'UDI')


def normalize_symbol_code(value):
    return str(value or '').strip().upper()


def normalize_selected_symbols(selected_symbols=None, default_symbols=None):
    source_symbols = selected_symbols if selected_symbols else (default_symbols or [])
    normalized = [normalize_symbol_code(item) for item in source_symbols if normalize_symbol_code(item)]
    return list(dict.fromkeys(normalized))


def normalize_trade_date(value):
    if value is None:
        return ''
    return str(value).split(' ')[0]


def parse_sina_history_response(response_text, symbol_code, invert=False):
    normalized_code = normalize_symbol_code(symbol_code)
    symbol_name = FOREX_SYMBOL_NAMES.get(normalized_code, normalized_code)
    match = re.search(
        r'\bvar\s+[A-Za-z0-9_$]+\s*=\s*\(\s*"(?P<data>.*)"\s*\)\s*;?\s*$',
        str(response_text or ''),
        flags=re.DOTALL,
    )
    if match is None:
        raise ValueError(f'Sina forex response format is invalid for {normalized_code}')

    rows_by_date = {}
    for raw_row in match.group('data').split('|'):
        fields = [field.strip() for field in raw_row.split(',')]
        if len(fields) < 5:
            continue
        trade_date = fields[0]
        try:
            datetime.strptime(trade_date, '%Y-%m-%d')
            open_price = float(fields[1])
            low_price = float(fields[2])
            high_price = float(fields[3])
            close_price = float(fields[4])
        except (TypeError, ValueError):
            continue
        if min(open_price, low_price, high_price, close_price) <= 0:
            continue
        if low_price > min(open_price, close_price) or high_price < max(open_price, close_price):
            continue
        if low_price > high_price:
            continue
        if invert:
            open_price, low_price, high_price, close_price = (
                1 / open_price,
                1 / high_price,
                1 / low_price,
                1 / close_price,
            )
        open_price, low_price, high_price, close_price = (
            round(open_price, 6),
            round(low_price, 6),
            round(high_price, 6),
            round(close_price, 6),
        )
        rows_by_date[trade_date] = {
            COL_CODE: normalized_code,
            COL_NAME: symbol_name,
            COL_DATE: trade_date,
            COL_OPEN: open_price,
            COL_LOW: low_price,
            COL_HIGH: high_price,
            COL_LATEST: close_price,
        }

    if not rows_by_date:
        raise ValueError(f'Sina forex response has no valid OHLC rows for {normalized_code}')

    parsed_rows = []
    previous_close = None
    for trade_date in sorted(rows_by_date):
        row = rows_by_date[trade_date]
        row[COL_AMPLITUDE] = calculate_amplitude(
            row[COL_HIGH],
            row[COL_LOW],
            previous_close,
        )
        previous_close = row[COL_LATEST]
        parsed_rows.append(row)
    return pd.DataFrame(parsed_rows)


def fetch_sina_history_once(symbol_code):
    normalized_code = normalize_symbol_code(symbol_code)
    sina_symbol = SINA_FOREX_SYMBOLS.get(normalized_code)
    if not sina_symbol:
        supported = ', '.join(sorted(SINA_FOREX_SYMBOLS))
        raise ValueError(
            f'Sina forex source does not support {normalized_code}; supported symbols: {supported}'
        )

    session = requests.Session()
    session.trust_env = False
    response = session.get(
        SINA_FOREX_DAY_KLINE_URL,
        params={
            'symbol': sina_symbol,
            '_': int(time.time() * 1000),
        },
        headers={
            'Accept': '*/*',
            'Referer': 'https://finance.sina.com.cn/',
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/136.0.0.0 Safari/537.36'
            ),
        },
        timeout=60,
    )
    response.raise_for_status()
    response.encoding = 'gbk'
    return parse_sina_history_response(
        response.text,
        normalized_code,
        invert=normalized_code in SINA_INVERTED_SYMBOLS,
    )


def parse_sina_realtime_response(response_text, expected_symbol_codes=None):
    expected_codes = set(normalize_selected_symbols(
        expected_symbol_codes,
        [*DAILY_SYNC_SYMBOLS, 'UDI'],
    ))
    rows = []
    for sina_symbol, payload in re.findall(
        r'var\s+hq_str_([A-Za-z0-9_]+)="([^"]*)"\s*;?',
        str(response_text or ''),
    ):
        symbol_code = SINA_SYMBOL_TO_CODE.get(sina_symbol.lower())
        if not symbol_code or symbol_code not in expected_codes:
            continue
        fields = [field.strip() for field in payload.split(',')]
        if len(fields) < 9:
            continue
        trade_date = next(
            (
                field
                for field in reversed(fields)
                if re.fullmatch(r'\d{4}-\d{2}-\d{2}', field)
            ),
            '',
        )
        try:
            datetime.strptime(trade_date, '%Y-%m-%d')
            latest_price = float(fields[1])
            previous_close = float(fields[3])
            open_price = float(fields[5])
            high_price = float(fields[6])
            low_price = float(fields[7])
        except (TypeError, ValueError):
            continue
        if min(open_price, low_price, high_price, latest_price, previous_close) <= 0:
            continue
        if low_price > min(open_price, latest_price) or high_price < max(open_price, latest_price):
            continue
        if symbol_code in SINA_INVERTED_SYMBOLS:
            open_price, low_price, high_price, latest_price, previous_close = (
                1 / open_price,
                1 / high_price,
                1 / low_price,
                1 / latest_price,
                1 / previous_close,
            )
        open_price, low_price, high_price, latest_price, previous_close = (
            round(open_price, 6),
            round(low_price, 6),
            round(high_price, 6),
            round(latest_price, 6),
            round(previous_close, 6),
        )
        rows.append({
            COL_CODE: symbol_code,
            COL_NAME: FOREX_SYMBOL_NAMES[symbol_code],
            COL_DATE: trade_date,
            COL_OPEN: open_price,
            COL_LOW: low_price,
            COL_HIGH: high_price,
            COL_LATEST: latest_price,
            COL_AMPLITUDE: calculate_amplitude(
                high_price,
                low_price,
                previous_close,
            ),
        })

    rows_by_code = {row[COL_CODE]: row for row in rows}
    missing_codes = sorted(expected_codes - set(rows_by_code))
    if missing_codes:
        raise ValueError(
            'Sina realtime forex response is missing symbols: '
            + ','.join(missing_codes)
        )
    return pd.DataFrame([rows_by_code[code] for code in sorted(expected_codes)])


def fetch_sina_realtime_once(symbol_codes=None):
    effective_symbols = normalize_selected_symbols(
        symbol_codes,
        [*DAILY_SYNC_SYMBOLS, 'UDI'],
    )
    unsupported = sorted(set(effective_symbols) - set(SINA_FOREX_SYMBOLS))
    if unsupported:
        raise ValueError(
            'Sina realtime forex source does not support: '
            + ','.join(unsupported)
        )
    sina_symbols = [SINA_FOREX_SYMBOLS[code] for code in effective_symbols]
    session = requests.Session()
    session.trust_env = False
    request_url = f'{SINA_FOREX_REALTIME_URL}list={",".join(sina_symbols)}'
    response = session.get(
        request_url,
        headers={
            'Accept': '*/*',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Referer': 'https://finance.sina.com.cn/',
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/136.0.0.0 Safari/537.36'
            ),
        },
        timeout=60,
    )
    response.raise_for_status()
    response.encoding = 'gbk'
    return parse_sina_realtime_response(response.text, effective_symbols)


def get_forex_realtime(symbol_codes=None):
    return fetch_with_retry(fetch_sina_realtime_once, symbol_codes)


def calculate_amplitude(high_price, low_price, pre_close):
    try:
        if high_price is None or low_price is None or pre_close in (None, 0):
            return None
        return round(
            (float(high_price) - float(low_price)) / float(pre_close) * 100,
            6,
        )
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
            'data_source': SINA_FOREX_DATA_SOURCE,
        })

    return basic_rows


def build_forex_daily_rows(
    history_df,
    fallback_symbol_code='',
    fallback_symbol_name='',
    data_source=SINA_FOREX_DATA_SOURCE,
):
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
            'data_source': data_source,
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
            'data_source': SINA_FOREX_DATA_SOURCE,
        })

    return rows


def select_latest_history_rows(rows, max_rows=2):
    dated_rows = [row for row in rows if row.get('trade_date')]
    if not dated_rows:
        return []

    dated_rows.sort(key=lambda item: item['trade_date'])
    return dated_rows[-max_rows:] if len(dated_rows) >= max_rows else dated_rows


def filter_rows_by_end_date(rows, end_date):
    end_date_text = end_date.strftime('%Y-%m-%d')
    return [row for row in rows if row['trade_date'] and row['trade_date'] <= end_date_text]


def build_usd_index_basic_rows():
    return [{
        'symbol_code': 'UDI',
        'symbol_name': USD_INDEX_SYMBOL_NAME,
        'data_source': SINA_USD_INDEX_DATA_SOURCE,
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


async def fetch_symbol_history_rows_for_daily_sync(symbol_code, semaphore):
    symbol_code = normalize_symbol_code(symbol_code)
    if not symbol_code:
        return {
            'symbol_code': '',
            'symbol_name': None,
            'basic_row': None,
            'daily_rows': [],
            'error': None,
        }

    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_forex_history, symbol_code)

        if history_df is None or history_df.empty:
            return {
                'symbol_code': symbol_code,
                'symbol_name': None,
                'basic_row': None,
                'daily_rows': [],
                'error': None,
            }

        history_rows = build_forex_daily_rows(history_df, symbol_code, symbol_code)
        today_text = datetime.now().date().isoformat()
        history_rows = [
            row for row in history_rows
            if row.get('trade_date') and row['trade_date'] < today_text
        ]
        latest_rows = select_latest_history_rows(history_rows, max_rows=2)
        if not latest_rows:
            return {
                'symbol_code': symbol_code,
                'symbol_name': None,
                'basic_row': None,
                'daily_rows': [],
                'error': None,
            }

        latest_name = latest_rows[-1].get('symbol_name') or symbol_code
        return {
            'symbol_code': symbol_code,
            'symbol_name': latest_name,
            'basic_row': {
                'symbol_code': symbol_code,
                'symbol_name': latest_name,
                'data_source': SINA_FOREX_DATA_SOURCE,
            },
            'daily_rows': latest_rows,
            'error': None,
        }
    except Exception as exc:
        error_message = f'Error fetching daily history for {symbol_code}: {exc}'
        print(error_message)
        log_error(symbol_code, 'N/A', error_message)
        return {
            'symbol_code': symbol_code,
            'symbol_name': None,
            'basic_row': None,
            'daily_rows': [],
            'error': str(exc),
        }


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
            if update['trade_date'] >= datetime.now().date().isoformat():
                continue
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


async def backfill_symbol_history(symbol_row, db_tools, semaphore):
    symbol_code = symbol_row['symbol_code']
    symbol_name = symbol_row.get('symbol_name') or symbol_code
    try:
        async with semaphore:
            history_df = await asyncio.to_thread(get_forex_history, symbol_code)
        if history_df is None or history_df.empty:
            return 0

        yesterday = datetime.now().date() - timedelta(days=1)
        history_rows = build_forex_daily_rows(history_df, symbol_code, symbol_name)
        history_rows = filter_rows_by_end_date(history_rows, yesterday)
        upserted = await db_tools.upsert_forex_daily_snapshots(history_rows)
        print(
            f'{symbol_code} Sina history upserted: {upserted}, '
            f'range: {history_rows[0]["trade_date"] if history_rows else "NONE"} -> '
            f'{history_rows[-1]["trade_date"] if history_rows else "NONE"}'
        )
        return upserted
    except Exception as exc:
        error_message = f'Error backfilling Sina history for {symbol_code}: {exc}'
        print(error_message)
        log_error(symbol_code, 'N/A', error_message)
        raise


async def backfill_history(selected_symbols=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

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

        results = await asyncio.gather(*[
            backfill_symbol_history(symbol_row, db_tools, semaphore)
            for symbol_row in basic_rows
        ])
        print(f'forex Sina history backfill finished, rows upserted: {sum(results)}.')
        return sum(results)
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
        history_rows = build_forex_daily_rows(
            history_df,
            'UDI',
            USD_INDEX_SYMBOL_NAME,
            data_source=SINA_USD_INDEX_DATA_SOURCE,
        )
        history_rows = filter_rows_by_end_date(history_rows, yesterday)

        basic_upserted = await db_tools.upsert_forex_basic_info(build_usd_index_basic_rows())
        inserted = await db_tools.upsert_forex_daily_snapshots(history_rows)
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

        history_rows = build_forex_daily_rows(
            history_df,
            'UDI',
            USD_INDEX_SYMBOL_NAME,
            data_source=SINA_USD_INDEX_DATA_SOURCE,
        )
        today_text = datetime.now().date().isoformat()
        history_rows = [
            row for row in history_rows
            if row.get('trade_date') and row['trade_date'] < today_text
        ]
        recent_rows = select_latest_usd_rows(history_rows)
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


async def sync_usd_index_intraday():
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        realtime_df = await asyncio.to_thread(get_forex_realtime, ['UDI'])
        today_text = datetime.now().date().isoformat()
        history_rows = build_forex_daily_rows(
            realtime_df,
            'UDI',
            USD_INDEX_SYMBOL_NAME,
            data_source=SINA_USD_INDEX_INTRADAY_DATA_SOURCE,
        )
        current_rows = [
            row for row in history_rows
            if row.get('trade_date') == today_text
        ]
        if len(current_rows) != 1:
            latest_date = max(
                (row.get('trade_date') for row in history_rows if row.get('trade_date')),
                default='NONE',
            )
            raise RuntimeError(
                f'Sina DXY realtime quote is not ready for {today_text}; '
                f'latest quote date: {latest_date}'
            )

        basic_upserted = await db_tools.upsert_forex_basic_info(build_usd_index_basic_rows())
        upserted = await db_tools.upsert_forex_daily_snapshots(current_rows)
        print(
            'usd index intraday sync finished, '
            f'forex_basic_info upserted: {basic_upserted}, '
            f'forex_daily_data upserted: {upserted}, '
            f'trade_date: {today_text}'
        )
        return upserted
    finally:
        await db_tools.close()


async def collect_symbol_history_for_request(symbol_code):
    normalized_code = normalize_symbol_code(symbol_code)
    if not normalized_code:
        raise ValueError('symbol_code is required')

    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        if normalized_code == 'UDI':
            history_df = await asyncio.to_thread(get_usd_index_history)
            fallback_name = USD_INDEX_SYMBOL_NAME
            basic_rows = build_usd_index_basic_rows()
            data_source = SINA_USD_INDEX_DATA_SOURCE
        else:
            history_df = await asyncio.to_thread(get_forex_history, normalized_code)
            fallback_name = FOREX_SYMBOL_NAMES.get(normalized_code, normalized_code)
            basic_rows = []
            data_source = SINA_FOREX_DATA_SOURCE

        if history_df is None or history_df.empty:
            raise RuntimeError(f'No forex history data fetched for {normalized_code}')

        history_rows = build_forex_daily_rows(
            history_df,
            normalized_code,
            fallback_name,
            data_source=data_source,
        )
        today_text = datetime.now().date().isoformat()
        history_rows = [
            row for row in history_rows
            if row.get('trade_date') and row['trade_date'] < today_text
        ]
        if not history_rows:
            raise RuntimeError(f'No forex history rows parsed for {normalized_code}')

        history_rows.sort(key=lambda row: row['trade_date'])
        latest_name = history_rows[-1].get('symbol_name') or fallback_name or normalized_code
        if not basic_rows:
            basic_rows = [{
                'symbol_code': normalized_code,
                'symbol_name': latest_name,
                'data_source': data_source,
            }]

        # Forex bars may be captured before the 24-hour day has fully settled, so refresh every returned row.
        basic_upserted = await db_tools.upsert_forex_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_forex_daily_snapshots(history_rows)
        trade_dates = sorted({row['trade_date'] for row in history_rows if row.get('trade_date')})

        print(
            'forex symbol history collect finished, '
            f'symbol_code: {normalized_code}, '
            f'forex_basic_info upserted: {basic_upserted}, '
            f'forex_daily_data upserted: {daily_upserted}, '
            f'rows_fetched: {len(history_rows)}, '
            f'latest_trade_date: {trade_dates[-1] if trade_dates else "NONE"}'
        )
        return {
            'status': 'SUCCESS',
            'symbol_code': normalized_code,
            'symbol_name': latest_name,
            'refresh_mode': 'full_history_upsert',
            'rows_fetched': len(history_rows),
            'upserted_rows': daily_upserted,
            'earliest_trade_date': trade_dates[0] if trade_dates else None,
            'latest_trade_date': trade_dates[-1] if trade_dates else None,
        }
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
    return await sync_daily_from_history(selected_symbols)


async def sync_daily_from_history(selected_symbols=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        effective_symbols = normalize_selected_symbols(selected_symbols, DAILY_SYNC_SYMBOLS)
        if not effective_symbols:
            print('No forex symbols matched the current selection.')
            return 0

        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        history_results = await asyncio.gather(*[
            fetch_symbol_history_rows_for_daily_sync(symbol_code, semaphore)
            for symbol_code in effective_symbols
        ])

        basic_rows = [result['basic_row'] for result in history_results if result.get('basic_row')]
        merged_rows = []
        for result in history_results:
            merged_rows.extend(result.get('daily_rows') or [])

        if not basic_rows or not merged_rows:
            print('No forex history rows fetched for daily sync.')
            return 0

        basic_upserted = await db_tools.upsert_forex_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_forex_daily_snapshots(merged_rows)
        refreshed_dates = sorted({row['trade_date'] for row in merged_rows if row.get('trade_date')})
        print(
            'forex daily finished, '
            f'forex_basic_info upserted: {basic_upserted}, '
            f'forex_daily_data upserted: {daily_upserted}, '
            f'symbols: {",".join(sorted(effective_symbols))}, '
            f'history_rows: {len(merged_rows)}, '
            f'history_refresh_dates: {",".join(refreshed_dates) if refreshed_dates else "NONE"}'
        )
        return daily_upserted
    finally:
        await db_tools.close()


async def sync_intraday_from_history(selected_symbols=None):
    effective_symbols = normalize_selected_symbols(selected_symbols, DAILY_SYNC_SYMBOLS)
    if not effective_symbols:
        print('No forex symbols matched the current intraday selection.')
        return 0

    db_tools = DbTools()
    await db_tools.init_pool()
    try:
        realtime_df = await asyncio.to_thread(get_forex_realtime, effective_symbols)
        today_text = datetime.now().date().isoformat()
        history_rows = build_forex_daily_rows(
            realtime_df,
            data_source=SINA_FOREX_INTRADAY_DATA_SOURCE,
        )
        current_rows = [
            row for row in history_rows
            if row.get('trade_date') == today_text
        ]
        current_codes = {row['symbol_code'] for row in current_rows}
        missing_codes = sorted(set(effective_symbols) - current_codes)
        if missing_codes:
            latest_date = max(
                (row.get('trade_date') for row in history_rows if row.get('trade_date')),
                default='NONE',
            )
            raise RuntimeError(
                f'Sina forex realtime quotes are not ready for {today_text}; '
                f'missing symbols: {",".join(missing_codes)}; '
                f'latest quote date: {latest_date}'
            )

        basic_rows = [{
            'symbol_code': symbol_code,
            'symbol_name': FOREX_SYMBOL_NAMES[symbol_code],
            'data_source': SINA_FOREX_DATA_SOURCE,
        } for symbol_code in effective_symbols]
        basic_upserted = await db_tools.upsert_forex_basic_info(basic_rows)
        daily_upserted = await db_tools.upsert_forex_daily_snapshots(current_rows)
        print(
            'forex intraday finished, '
            f'forex_basic_info upserted: {basic_upserted}, '
            f'forex_daily_data upserted: {daily_upserted}, '
            f'trade_date: {today_text}, '
            f'symbols: {",".join(sorted(current_codes))}'
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
    if command == 'intraday':
        await sync_intraday_from_history(selected_symbols)
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
    if command == 'usd-intraday':
        await sync_usd_index_intraday()
        return
    if command == 'repair-history':
        await repair_unrefreshed_history_rows(selected_symbols)
        return

    raise ValueError(
        'usage: python forex_main.py [backfill|daily|intraday|repair-history] [SYMBOL ...] '
        '| [usd-backfill|usd-daily|usd-once|usd-intraday]'
    )


if __name__ == '__main__':
    asyncio.run(main())
