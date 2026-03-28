import asyncio
import sys

from akshare_project.collectors import cffex, etf, forex, futures, index, option, quant_index, stock
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools

LOGGER = get_logger('failed_tasks')


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


async def dispatch_failed_task(failure):
    task_name = failure['task_name']
    task_stage = failure['task_stage']
    payload = failure.get('payload') or {}

    if task_name == 'stock_daily':
        return await stock.sync_daily()
    if task_name == 'index_daily':
        return await index.sync_daily_from_spot()
    if task_name == 'cffex_daily':
        return await cffex.sync_latest_daily_data(headless=True)
    if task_name == 'forex_daily':
        return await forex.sync_daily_from_spot()
    if task_name == 'usd_index_once':
        return await forex.sync_usd_index_once()
    if task_name == 'futures_daily':
        return await futures.sync_today()
    if task_name == 'etf_daily':
        return await etf.sync_daily()
    if task_name == 'option_daily':
        if task_stage != 'task':
            return 0
        return await option.sync_daily(record_failures=False)
    if task_name == 'quant_index_daily':
        return await quant_index.sync_daily()
    if task_name == 'option_missing_date_backfill':
        return 0
    if task_name == 'stock_missing_date_backfill':
        return 0

    raise ValueError(f'unsupported failed task: {task_name}/{task_stage}')


async def retry_failed_tasks(task_name=None, limit=None):
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        failed_tasks = await db_tools.get_pending_failed_tasks(task_name=task_name, limit=limit)
        if not failed_tasks:
            print('No pending failed tasks found.')
            return 0

        resolved_count = 0
        for failure in failed_tasks:
            failure_label = f"{failure['task_name']}:{failure['task_stage']}:{failure['task_key']}"
            print(f'[retry] started -> {failure_label}')
            try:
                result = await dispatch_failed_task(failure)
                await db_tools.mark_failed_task_retry_result(failure['id'], success=True)
                resolved_count += 1
                print(f'[retry] resolved -> {failure_label}, result: {result}')
            except Exception as exc:
                await db_tools.mark_failed_task_retry_result(failure['id'], success=False, error_message=str(exc))
                print(f'[retry] failed -> {failure_label}, error: {exc}')

        print(f'failed task retry finished: resolved={resolved_count}, total={len(failed_tasks)}')
        return resolved_count
    finally:
        await db_tools.close()


async def main():
    task_name = sys.argv[1].strip().lower() if len(sys.argv) > 1 else None
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    await retry_failed_tasks(task_name=task_name, limit=limit)


if __name__ == '__main__':
    asyncio.run(main())
