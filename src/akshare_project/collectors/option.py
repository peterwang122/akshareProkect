import asyncio
import os
import re
import sys
import time
from datetime import datetime

import akshare as ak

from akshare_project.core.ak_scheduler_client import SchedulerContext
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.progress import ProgressStore
from akshare_project.core.retry import fetch_with_retry as shared_fetch_with_retry
from akshare_project.db.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 6
OPTION_DAILY_TASK_NAME = 'option_daily'
OPTION_MISSING_DATE_TASK_NAME = 'option_missing_date_backfill'
OPTION_STAGE_LIST = 'list'
OPTION_STAGE_SPOT = 'spot'
OPTION_STAGE_DAILY = 'daily'
LOGGER = get_logger('option')
PROGRESS_STORE = ProgressStore('option')

COL_STRIKE = '\u884c\u6743\u4ef7'
COL_CALL_ID = '\u770b\u6da8\u5408\u7ea6-\u6807\u8bc6'
COL_PUT_ID = '\u770b\u8dcc\u5408\u7ea6-\u6807\u8bc6'
COL_CALL_BUY_VOL = '\u770b\u6da8\u5408\u7ea6-\u4e70\u91cf'
COL_CALL_BUY_PRICE = '\u770b\u6da8\u5408\u7ea6-\u4e70\u4ef7'
COL_CALL_LATEST = '\u770b\u6da8\u5408\u7ea6-\u6700\u65b0\u4ef7'
COL_CALL_SELL_PRICE = '\u770b\u6da8\u5408\u7ea6-\u5356\u4ef7'
COL_CALL_SELL_VOL = '\u770b\u6da8\u5408\u7ea6-\u5356\u91cf'
COL_CALL_OI = '\u770b\u6da8\u5408\u7ea6-\u6301\u4ed3\u91cf'
COL_CALL_CHANGE = '\u770b\u6da8\u5408\u7ea6-\u6da8\u8dcc'
COL_PUT_BUY_VOL = '\u770b\u8dcc\u5408\u7ea6-\u4e70\u91cf'
COL_PUT_BUY_PRICE = '\u770b\u8dcc\u5408\u7ea6-\u4e70\u4ef7'
COL_PUT_LATEST = '\u770b\u8dcc\u5408\u7ea6-\u6700\u65b0\u4ef7'
COL_PUT_SELL_PRICE = '\u770b\u8dcc\u5408\u7ea6-\u5356\u4ef7'
COL_PUT_SELL_VOL = '\u770b\u8dcc\u5408\u7ea6-\u5356\u91cf'
COL_PUT_OI = '\u770b\u8dcc\u5408\u7ea6-\u6301\u4ed3\u91cf'
COL_PUT_CHANGE = '\u770b\u8dcc\u5408\u7ea6-\u6da8\u8dcc'

OPTION_CONFIG = {
    'SZ50': {
        'index_name': '\u4e0a\u8bc150',
        'product_code': 'ho',
        'list_func': ak.option_cffex_sz50_list_sina,
        'spot_func': ak.option_cffex_sz50_spot_sina,
        'daily_func': ak.option_cffex_sz50_daily_sina,
        'spot_source': 'option_cffex_sz50_spot_sina',
        'daily_source': 'option_cffex_sz50_daily_sina',
    },
    'HS300': {
        'index_name': '\u6caa\u6df1300',
        'product_code': 'io',
        'list_func': ak.option_cffex_hs300_list_sina,
        'spot_func': ak.option_cffex_hs300_spot_sina,
        'daily_func': ak.option_cffex_hs300_daily_sina,
        'spot_source': 'option_cffex_hs300_spot_sina',
        'daily_source': 'option_cffex_hs300_daily_sina',
    },
    'ZZ1000': {
        'index_name': '\u4e2d\u8bc11000',
        'product_code': 'mo',
        'list_func': ak.option_cffex_zz1000_list_sina,
        'spot_func': ak.option_cffex_zz1000_spot_sina,
        'daily_func': ak.option_cffex_zz1000_daily_sina,
        'spot_source': 'option_cffex_zz1000_spot_sina',
        'daily_source': 'option_cffex_zz1000_daily_sina',
    },
}


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def load_progress():
    return PROGRESS_STORE.load()


def save_progress(option_symbol):
    PROGRESS_STORE.append(option_symbol)


def log_error(context, error_message):
    LOGGER.error('%s,%s', context, error_message)


def fetch_with_retry(
    func,
    *args,
    retries=API_RETRY_COUNT,
    sleep_seconds=API_RETRY_SLEEP_SECONDS,
    scheduler_context=None,
    return_scheduler_meta=False,
    request_key=None,
    **kwargs,
):
    return shared_fetch_with_retry(
        func,
        *args,
        retries=retries,
        sleep_seconds=sleep_seconds,
        logger=LOGGER,
        scheduler_context=scheduler_context,
        return_scheduler_meta=return_scheduler_meta,
        caller_name=LOGGER.name,
        request_key=request_key,
        **kwargs,
    )


def flatten_contract_symbols(contract_mapping):
    symbols = []
    for contract_list in contract_mapping.values():
        for contract_symbol in contract_list:
            contract_text = str(contract_symbol or '').strip().lower()
            if contract_text:
                symbols.append(contract_text)
    return list(dict.fromkeys(symbols))


def parse_option_type(option_symbol):
    symbol = str(option_symbol or '').strip().upper()
    match = re.search(r'([CP])\d+(?:\.\d+)?$', symbol)
    if match:
        return 'CALL' if match.group(1) == 'C' else 'PUT'
    if 'C' in symbol:
        return 'CALL'
    if 'P' in symbol:
        return 'PUT'
    return None


def build_spot_rows(index_type, config, contract_symbol, spot_df):
    rows = []
    for _, row in spot_df.iterrows():
        strike_price = row.get(COL_STRIKE)
        call_option_symbol = str(row.get(COL_CALL_ID, '')).strip()
        put_option_symbol = str(row.get(COL_PUT_ID, '')).strip()
        if not call_option_symbol and not put_option_symbol:
            continue

        rows.append({
            'index_type': index_type,
            'index_name': config['index_name'],
            'product_code': config['product_code'],
            'contract_symbol': contract_symbol,
            'strike_price': strike_price,
            'call_option_symbol': call_option_symbol,
            'call_buy_volume': row.get(COL_CALL_BUY_VOL),
            'call_buy_price': row.get(COL_CALL_BUY_PRICE),
            'call_latest_price': row.get(COL_CALL_LATEST),
            'call_sell_price': row.get(COL_CALL_SELL_PRICE),
            'call_sell_volume': row.get(COL_CALL_SELL_VOL),
            'call_open_interest': row.get(COL_CALL_OI),
            'call_change': row.get(COL_CALL_CHANGE),
            'put_option_symbol': put_option_symbol,
            'put_buy_volume': row.get(COL_PUT_BUY_VOL),
            'put_buy_price': row.get(COL_PUT_BUY_PRICE),
            'put_latest_price': row.get(COL_PUT_LATEST),
            'put_sell_price': row.get(COL_PUT_SELL_PRICE),
            'put_sell_volume': row.get(COL_PUT_SELL_VOL),
            'put_open_interest': row.get(COL_PUT_OI),
            'put_change': row.get(COL_PUT_CHANGE),
            'data_source': config['spot_source'],
        })
    return rows


def extract_option_meta(spot_rows):
    option_meta = {}
    for row in spot_rows:
        common_meta = {
            'index_type': row['index_type'],
            'index_name': row['index_name'],
            'product_code': row['product_code'],
            'contract_symbol': row['contract_symbol'],
            'strike_price': row['strike_price'],
        }
        if row.get('call_option_symbol'):
            option_meta[row['call_option_symbol']] = {
                **common_meta,
                'option_symbol': row['call_option_symbol'],
                'option_type': 'CALL',
            }
        if row.get('put_option_symbol'):
            option_meta[row['put_option_symbol']] = {
                **common_meta,
                'option_symbol': row['put_option_symbol'],
                'option_type': 'PUT',
            }
    return option_meta


def build_daily_rows(option_meta, daily_df, daily_source, latest_only=False):
    if latest_only and daily_df is not None and not daily_df.empty:
        daily_df = daily_df.tail(1)

    rows = []
    for _, row in daily_df.iterrows():
        trade_date = str(row.get('date', '')).split(' ')[0]
        if not trade_date:
            continue
        rows.append({
            'index_type': option_meta['index_type'],
            'index_name': option_meta['index_name'],
            'product_code': option_meta['product_code'],
            'contract_symbol': option_meta['contract_symbol'],
            'option_symbol': option_meta['option_symbol'],
            'option_type': option_meta.get('option_type') or parse_option_type(option_meta['option_symbol']),
            'strike_price': option_meta['strike_price'],
            'trade_date': trade_date,
            'open_price': row.get('open'),
            'high_price': row.get('high'),
            'low_price': row.get('low'),
            'close_price': row.get('close'),
            'volume': row.get('volume'),
            'data_source': daily_source,
        })
    return rows


def fetch_contract_symbols(config, scheduler_context=None, return_scheduler_meta=False):
    result = fetch_with_retry(
        config['list_func'],
        scheduler_context=scheduler_context,
        return_scheduler_meta=return_scheduler_meta,
    )
    contract_mapping = result.value if return_scheduler_meta else result
    flattened = flatten_contract_symbols(contract_mapping)
    if return_scheduler_meta:
        return flattened, result
    return flattened


def fetch_spot_df(config, contract_symbol, scheduler_context=None, return_scheduler_meta=False):
    return fetch_with_retry(
        config['spot_func'],
        symbol=contract_symbol,
        scheduler_context=scheduler_context,
        return_scheduler_meta=return_scheduler_meta,
    )


def fetch_daily_df(config, option_symbol, scheduler_context=None, return_scheduler_meta=False):
    return fetch_with_retry(
        config['daily_func'],
        symbol=option_symbol,
        scheduler_context=scheduler_context,
        return_scheduler_meta=return_scheduler_meta,
    )


def get_option_mode(latest_only):
    return 'daily' if latest_only else 'backfill'


def normalize_trade_date_text(value):
    value = str(value or '').strip()
    if not value:
        raise ValueError('trade_date is required')
    return datetime.strptime(value, '%Y-%m-%d').strftime('%Y-%m-%d')


def build_option_task_key(stage, latest_only, index_type, contract_symbol=None, option_symbol=None):
    parts = [get_option_mode(latest_only), str(index_type or '').strip().upper()]
    if stage in (OPTION_STAGE_SPOT, OPTION_STAGE_DAILY) and contract_symbol:
        parts.append(str(contract_symbol or '').strip().lower())
    if stage == OPTION_STAGE_DAILY and option_symbol:
        parts.append(str(option_symbol or '').strip())
    return ':'.join(parts)


def build_option_failure_payload(stage, latest_only, index_type, contract_symbol=None, option_meta=None):
    payload = {
        'task_name': OPTION_DAILY_TASK_NAME,
        'stage': stage,
        'mode': get_option_mode(latest_only),
        'index_type': str(index_type or '').strip().upper(),
    }
    if contract_symbol:
        payload['contract_symbol'] = str(contract_symbol or '').strip().lower()
    if option_meta:
        payload['option_meta'] = option_meta
    return payload


def build_option_missing_task_key(target_date, option_symbol):
    return f'{target_date}:{str(option_symbol or "").strip()}'


def build_option_missing_failure_payload(target_date, option_meta):
    return {
        'task_name': OPTION_MISSING_DATE_TASK_NAME,
        'stage': OPTION_STAGE_DAILY,
        'target_date': target_date,
        'index_type': str(option_meta.get('index_type', '')).strip().upper(),
        'option_meta': option_meta,
    }


async def record_option_failure(db_tools, stage, latest_only, index_type, error_message, contract_symbol=None, option_meta=None):
    task_key = build_option_task_key(
        stage,
        latest_only,
        index_type,
        contract_symbol=contract_symbol,
        option_symbol=(option_meta or {}).get('option_symbol'),
    )
    payload = build_option_failure_payload(
        stage,
        latest_only,
        index_type,
        contract_symbol=contract_symbol,
        option_meta=option_meta,
    )
    await db_tools.upsert_failed_task({
        'task_name': OPTION_DAILY_TASK_NAME,
        'task_stage': stage,
        'task_key': task_key,
        'payload_json': payload,
        'error_message': error_message,
    })


async def resolve_option_failure(db_tools, stage, latest_only, index_type, contract_symbol=None, option_symbol=None):
    task_key = build_option_task_key(
        stage,
        latest_only,
        index_type,
        contract_symbol=contract_symbol,
        option_symbol=option_symbol,
    )
    await db_tools.resolve_failed_task_by_identity(OPTION_DAILY_TASK_NAME, stage, task_key)


async def record_option_missing_failure(db_tools, target_date, option_meta, error_message):
    await db_tools.upsert_failed_task({
        'task_name': OPTION_MISSING_DATE_TASK_NAME,
        'task_stage': OPTION_STAGE_DAILY,
        'task_key': build_option_missing_task_key(target_date, option_meta.get('option_symbol')),
        'payload_json': build_option_missing_failure_payload(target_date, option_meta),
        'error_message': error_message,
    })


async def resolve_option_missing_failure(db_tools, target_date, option_symbol):
    await db_tools.resolve_failed_task_by_identity(
        OPTION_MISSING_DATE_TASK_NAME,
        OPTION_STAGE_DAILY,
        build_option_missing_task_key(target_date, option_symbol),
    )


async def mark_option_missing_success(db_tools, target_date, option_meta):
    await db_tools.upsert_success_task({
        'task_name': OPTION_MISSING_DATE_TASK_NAME,
        'task_stage': OPTION_STAGE_DAILY,
        'task_key': build_option_missing_task_key(target_date, option_meta.get('option_symbol')),
        'payload_json': build_option_missing_failure_payload(target_date, option_meta),
    })


async def collect_contract_option_meta(
    db_tools,
    index_type,
    config,
    contract_symbol,
    latest_only=False,
    record_failures=False,
    scheduler_context=None,
):
    spot_result = await asyncio.to_thread(
        fetch_spot_df,
        config,
        contract_symbol,
        scheduler_context,
        True,
    )
    spot_df = spot_result.value
    if spot_df is None or spot_df.empty:
        if record_failures:
            await resolve_option_failure(
                db_tools,
                OPTION_STAGE_SPOT,
                latest_only,
                index_type,
                contract_symbol=contract_symbol,
            )
        return {}

    spot_rows = build_spot_rows(index_type, config, contract_symbol, spot_df)
    inserted = await db_tools.batch_option_spot_data(spot_rows)
    print(f'{index_type} {contract_symbol} spot inserted: {inserted}')
    if record_failures:
        await resolve_option_failure(
            db_tools,
            OPTION_STAGE_SPOT,
            latest_only,
            index_type,
            contract_symbol=contract_symbol,
        )
    option_meta = extract_option_meta(spot_rows)
    for meta in option_meta.values():
        meta['_scheduler_parent_job_id'] = spot_result.job_id
        meta['_scheduler_root_job_id'] = spot_result.root_job_id
        meta['_scheduler_workflow_name'] = f"option:{index_type}:{contract_symbol}"
    return option_meta


async def collect_spot_and_meta(db_tools, latest_only=False, record_failures=False):
    all_option_meta = {}

    for index_type, config in OPTION_CONFIG.items():
        try:
            contract_symbols, list_result = await asyncio.to_thread(fetch_contract_symbols, config, None, True)
            if record_failures:
                await resolve_option_failure(db_tools, OPTION_STAGE_LIST, latest_only, index_type)
        except Exception as exc:
            error_message = f'{index_type}:list:{exc}'
            print(error_message)
            log_error(f'{index_type}:list', str(exc))
            if record_failures:
                await record_option_failure(db_tools, OPTION_STAGE_LIST, latest_only, index_type, str(exc))
            continue

        list_scheduler_context = SchedulerContext(
            parent_job_id=list_result.job_id,
            root_job_id=list_result.root_job_id,
            workflow_name=f'option:{index_type}:list',
        )

        for contract_symbol in contract_symbols:
            try:
                all_option_meta.update(
                    await collect_contract_option_meta(
                        db_tools,
                        index_type,
                        config,
                        contract_symbol,
                        latest_only=latest_only,
                        record_failures=record_failures,
                        scheduler_context=list_scheduler_context,
                    )
                )
            except Exception as exc:
                error_message = f'{index_type}:{contract_symbol}:{exc}'
                print(error_message)
                log_error(f'{index_type}:{contract_symbol}', str(exc))
                if record_failures:
                    await record_option_failure(
                        db_tools,
                        OPTION_STAGE_SPOT,
                        latest_only,
                        index_type,
                        str(exc),
                        contract_symbol=contract_symbol,
                    )

    return all_option_meta


async def process_option_symbol(db_tools, config, option_meta, latest_only, processed, semaphore, record_failures=False, swallow_exceptions=True):
    option_symbol = option_meta['option_symbol']
    if not latest_only and option_symbol in processed:
        return 0

    try:
        scheduler_context = None
        if option_meta.get('_scheduler_parent_job_id'):
            scheduler_context = SchedulerContext(
                parent_job_id=option_meta.get('_scheduler_parent_job_id'),
                root_job_id=option_meta.get('_scheduler_root_job_id'),
                workflow_name=option_meta.get('_scheduler_workflow_name') or f"option:{option_meta['index_type']}",
            )
        async with semaphore:
            daily_df = await asyncio.to_thread(
                fetch_daily_df,
                config,
                option_symbol,
                scheduler_context,
            )

        if daily_df is None or daily_df.empty:
            if not latest_only:
                save_progress(option_symbol)
                processed.add(option_symbol)
            if record_failures:
                await resolve_option_failure(
                    db_tools,
                    OPTION_STAGE_DAILY,
                    latest_only,
                    option_meta['index_type'],
                    contract_symbol=option_meta.get('contract_symbol'),
                    option_symbol=option_symbol,
                )
            print(f"{option_meta['index_type']} {option_symbol} daily no-data response, skipped")
            return 0

        daily_rows = build_daily_rows(option_meta, daily_df, config['daily_source'], latest_only=latest_only)
        if not daily_rows:
            if not latest_only:
                save_progress(option_symbol)
                processed.add(option_symbol)
            if record_failures:
                await resolve_option_failure(
                    db_tools,
                    OPTION_STAGE_DAILY,
                    latest_only,
                    option_meta['index_type'],
                    contract_symbol=option_meta.get('contract_symbol'),
                    option_symbol=option_symbol,
                )
            print(f"{option_meta['index_type']} {option_symbol} daily no-trade rows, skipped")
            return 0

        inserted = await db_tools.batch_option_daily_data(daily_rows)
        if not latest_only:
            save_progress(option_symbol)
            processed.add(option_symbol)
        if record_failures:
            await resolve_option_failure(
                db_tools,
                OPTION_STAGE_DAILY,
                latest_only,
                option_meta['index_type'],
                contract_symbol=option_meta.get('contract_symbol'),
                option_symbol=option_symbol,
            )
        print(f"{option_meta['index_type']} {option_symbol} daily inserted: {inserted}")
        return inserted
    except Exception as exc:
        error_message = f'{option_meta["index_type"]}:{option_symbol}:{exc}'
        print(error_message)
        log_error(f'{option_meta["index_type"]}:{option_symbol}', str(exc))
        if record_failures:
            await record_option_failure(
                db_tools,
                OPTION_STAGE_DAILY,
                latest_only,
                option_meta['index_type'],
                str(exc),
                contract_symbol=option_meta.get('contract_symbol'),
                option_meta=option_meta,
            )
        if swallow_exceptions:
            return 0
        raise


async def backfill_history():
    db_tools = DbTools()
    await db_tools.init_pool()
    processed = load_progress()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    try:
        option_meta_map = await collect_spot_and_meta(db_tools, latest_only=False, record_failures=False)
        total_inserted = 0
        for option_symbol, option_meta in option_meta_map.items():
            config = OPTION_CONFIG[option_meta['index_type']]
            total_inserted += await process_option_symbol(
                db_tools,
                config,
                option_meta,
                latest_only=False,
                processed=processed,
                semaphore=semaphore,
                record_failures=False,
            )
        print(f'option backfill finished, inserted rows: {total_inserted}')
        return total_inserted
    finally:
        await db_tools.close()


async def sync_daily(record_failures=False):
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    try:
        option_meta_map = await collect_spot_and_meta(db_tools, latest_only=True, record_failures=record_failures)
        total_inserted = 0
        processed = set()
        for option_symbol, option_meta in option_meta_map.items():
            config = OPTION_CONFIG[option_meta['index_type']]
            total_inserted += await process_option_symbol(
                db_tools,
                config,
                option_meta,
                latest_only=True,
                processed=processed,
                semaphore=semaphore,
                record_failures=record_failures,
            )
        print(f'option daily finished, inserted rows: {total_inserted}')
        return total_inserted
    finally:
        await db_tools.close()


async def retry_failed_daily_task(payload):
    stage = str(payload.get('stage', '')).strip().lower()
    mode = str(payload.get('mode', 'daily')).strip().lower()
    latest_only = mode != 'backfill'
    index_type = str(payload.get('index_type', '')).strip().upper()
    if index_type not in OPTION_CONFIG:
        raise ValueError(f'unsupported option index_type: {index_type}')

    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    try:
        if stage == OPTION_STAGE_LIST:
            config = OPTION_CONFIG[index_type]
            contract_symbols, list_result = await asyncio.to_thread(fetch_contract_symbols, config, None, True)
            await resolve_option_failure(db_tools, OPTION_STAGE_LIST, latest_only, index_type)
            list_scheduler_context = SchedulerContext(
                parent_job_id=list_result.job_id,
                root_job_id=list_result.root_job_id,
                workflow_name=f'option:{index_type}:list',
            )

            total_inserted = 0
            for contract_symbol in contract_symbols:
                try:
                    option_meta_map = await collect_contract_option_meta(
                        db_tools,
                        index_type,
                        config,
                        contract_symbol,
                        latest_only=latest_only,
                        record_failures=True,
                        scheduler_context=list_scheduler_context,
                    )
                except Exception as exc:
                    error_message = f'{index_type}:{contract_symbol}:{exc}'
                    print(error_message)
                    log_error(f'{index_type}:{contract_symbol}', str(exc))
                    await record_option_failure(
                        db_tools,
                        OPTION_STAGE_SPOT,
                        latest_only,
                        index_type,
                        str(exc),
                        contract_symbol=contract_symbol,
                    )
                    continue

                for option_symbol, option_meta in option_meta_map.items():
                    config = OPTION_CONFIG[option_meta['index_type']]
                    total_inserted += await process_option_symbol(
                        db_tools,
                        config,
                        option_meta,
                        latest_only=latest_only,
                        processed=set(),
                        semaphore=semaphore,
                        record_failures=True,
                        swallow_exceptions=True,
                    )
                print(f'{index_type} list retry finished, inserted rows: {total_inserted}')
            return total_inserted

        if stage == OPTION_STAGE_SPOT:
            contract_symbol = str(payload.get('contract_symbol', '')).strip().lower()
            if not contract_symbol:
                raise ValueError('missing contract_symbol in failed spot task payload')

            config = OPTION_CONFIG[index_type]
            option_meta_map = await collect_contract_option_meta(
                db_tools,
                index_type,
                config,
                contract_symbol,
                latest_only=latest_only,
                record_failures=True,
            )

            total_inserted = 0
            for option_symbol, option_meta in option_meta_map.items():
                total_inserted += await process_option_symbol(
                    db_tools,
                    config,
                    option_meta,
                    latest_only=latest_only,
                    processed=set(),
                    semaphore=semaphore,
                    record_failures=True,
                    swallow_exceptions=True,
                )
            print(f'{index_type} {contract_symbol} retry finished, inserted rows: {total_inserted}')
            return total_inserted

        if stage == OPTION_STAGE_DAILY:
            option_meta = payload.get('option_meta') or {}
            if not option_meta.get('option_symbol'):
                raise ValueError('missing option_meta.option_symbol in failed daily task payload')
            config = OPTION_CONFIG[index_type]
            inserted = await process_option_symbol(
                db_tools,
                config,
                option_meta,
                latest_only=latest_only,
                processed=set(),
                semaphore=semaphore,
                record_failures=True,
                swallow_exceptions=False,
            )
            print(f'{index_type} {option_meta["option_symbol"]} retry finished, inserted rows: {inserted}')
            return inserted

        raise ValueError(f'unsupported option failed task stage: {stage}')
    finally:
        await db_tools.close()


async def process_option_missing_date_symbol(db_tools, option_meta, target_date, semaphore, swallow_exceptions=True):
    index_type = str(option_meta.get('index_type', '')).strip().upper()
    if index_type not in OPTION_CONFIG:
        raise ValueError(f'unsupported option index_type: {index_type}')

    option_symbol = str(option_meta.get('option_symbol', '')).strip()
    if not option_symbol:
        raise ValueError('missing option_symbol in option_meta')

    config = OPTION_CONFIG[index_type]

    try:
        async with semaphore:
            daily_df = await asyncio.to_thread(fetch_daily_df, config, option_symbol)

        if daily_df is None or daily_df.empty:
            await mark_option_missing_success(db_tools, target_date, option_meta)
            await resolve_option_missing_failure(db_tools, target_date, option_symbol)
            print(f'option missing-date no-data response: {option_symbol}, trade_date={target_date}, skipped')
            return 0

        daily_rows = build_daily_rows(option_meta, daily_df, config['daily_source'], latest_only=False)
        if not daily_rows:
            await mark_option_missing_success(db_tools, target_date, option_meta)
            await resolve_option_missing_failure(db_tools, target_date, option_symbol)
            print(f'option missing-date no-trade rows: {option_symbol}, trade_date={target_date}, skipped')
            return 0

        target_rows = [row for row in daily_rows if row['trade_date'] == target_date]
        if not target_rows:
            await mark_option_missing_success(db_tools, target_date, option_meta)
            await resolve_option_missing_failure(db_tools, target_date, option_symbol)
            print(f'option missing-date target not present: {option_symbol}, trade_date={target_date}, skipped')
            return 0

        inserted = await db_tools.batch_option_daily_data(target_rows)
        await mark_option_missing_success(db_tools, target_date, option_meta)
        await resolve_option_missing_failure(db_tools, target_date, option_symbol)
        print(f'option missing-date filled: {option_symbol}, trade_date={target_date}, inserted={inserted}')
        return inserted
    except Exception as exc:
        await record_option_missing_failure(db_tools, target_date, option_meta, str(exc))
        error_message = f'{target_date}:{option_symbol}:{exc}'
        print(error_message)
        log_error(f'{target_date}:{option_symbol}', str(exc))
        if swallow_exceptions:
            return 0
        raise


async def backfill_missing_trade_date_once(target_date):
    target_date = normalize_trade_date_text(target_date)
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    try:
        missing_option_meta = await db_tools.get_option_symbols_missing_trade_date(
            target_date,
            exclude_success_task_name=OPTION_MISSING_DATE_TASK_NAME,
        )
        if not missing_option_meta:
            print(f'No missing option_symbol found for {target_date}.')
            return 0

        results = await asyncio.gather(*[
            process_option_missing_date_symbol(
                db_tools,
                option_meta,
                target_date,
                semaphore,
                swallow_exceptions=True,
            )
            for option_meta in missing_option_meta
        ])
        inserted = sum(results)
        print(
            f'option missing-date backfill finished, '
            f'trade_date={target_date}, '
            f'missing_count={len(missing_option_meta)}, '
            f'inserted_rows={inserted}'
        )
        return inserted
    finally:
        await db_tools.close()


async def retry_missing_trade_date_failures_once(target_date):
    target_date = normalize_trade_date_text(target_date)
    db_tools = DbTools()
    await db_tools.init_pool()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    try:
        failed_tasks = await db_tools.get_pending_failed_tasks(task_name=OPTION_MISSING_DATE_TASK_NAME)
        failed_tasks = [
            task for task in failed_tasks
            if str((task.get('payload') or {}).get('target_date', '')).strip() == target_date
        ]
        if not failed_tasks:
            print(f'No pending option missing-date failed tasks for {target_date}.')
            return 0

        total_inserted = 0
        for failure in failed_tasks:
            payload = failure.get('payload') or {}
            option_meta = payload.get('option_meta') or {}
            try:
                total_inserted += await process_option_missing_date_symbol(
                    db_tools,
                    option_meta,
                    target_date,
                    semaphore,
                    swallow_exceptions=False,
                )
                await db_tools.mark_failed_task_retry_result(failure['id'], success=True)
            except Exception as exc:
                await db_tools.mark_failed_task_retry_result(failure['id'], success=False, error_message=str(exc))

        print(
            f'option missing-date retry round finished, '
            f'trade_date={target_date}, '
            f'pending_count={len(failed_tasks)}, '
            f'inserted_rows={total_inserted}'
        )
        return total_inserted
    finally:
        await db_tools.close()


async def repair_missing_trade_date_until_complete(target_date):
    target_date = normalize_trade_date_text(target_date)
    round_no = 0
    total_inserted = 0

    while True:
        round_no += 1
        print(f'option missing-date repair round {round_no} started: trade_date={target_date}')
        total_inserted += await backfill_missing_trade_date_once(target_date)
        total_inserted += await retry_missing_trade_date_failures_once(target_date)

        db_tools = DbTools()
        await db_tools.init_pool()
        try:
            remaining = await db_tools.get_option_symbols_missing_trade_date(
                target_date,
                exclude_success_task_name=OPTION_MISSING_DATE_TASK_NAME,
            )
            pending = await db_tools.get_pending_failed_tasks(task_name=OPTION_MISSING_DATE_TASK_NAME)
            pending = [
                task for task in pending
                if str((task.get('payload') or {}).get('target_date', '')).strip() == target_date
            ]
        finally:
            await db_tools.close()

        print(
            f'option missing-date repair round {round_no} finished, '
            f'trade_date={target_date}, '
            f'remaining_missing={len(remaining)}, '
            f'pending_failures={len(pending)}'
        )

        if not remaining:
            print(
                f'option missing-date repair completed, '
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
        await sync_daily()
        return
    if command == 'repair-missing-date':
        if not trade_date:
            raise ValueError('usage: python option_main.py repair-missing-date YYYY-MM-DD')
        await repair_missing_trade_date_until_complete(trade_date)
        return

    raise ValueError('supported commands: backfill, daily, repair-missing-date')


if __name__ == '__main__':
    asyncio.run(main())
