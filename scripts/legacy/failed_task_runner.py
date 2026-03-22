import asyncio
import sys

import cffex_main
import forex_main
import futures_main
import index_main
import main as stock_main
import option_main
from util.db_tool import DbTools


async def dispatch_failed_task(failure):
    task_name = failure['task_name']
    task_stage = failure['task_stage']
    payload = failure.get('payload') or {}

    if task_name == 'stock_daily':
        return await stock_main.sync_daily_from_history()
    if task_name == 'index_daily':
        return await index_main.sync_daily_from_spot()
    if task_name == 'cffex_daily':
        return await cffex_main.sync_latest_daily_data(headless=True)
    if task_name == 'forex_daily':
        return await forex_main.sync_daily_from_spot()
    if task_name == 'usd_index_once':
        return await forex_main.sync_usd_index_once()
    if task_name == 'futures_daily':
        return await futures_main.sync_today()
    if task_name == 'option_daily':
        if task_stage == 'task':
            return await option_main.sync_daily(record_failures=True)
        return await option_main.retry_failed_daily_task(payload)
    if task_name == 'option_missing_date_backfill':
        target_date = payload.get('target_date')
        if not target_date:
            raise ValueError('missing target_date for option_missing_date_backfill')
        return await option_main.retry_missing_trade_date_failures_once(target_date)
    if task_name == 'stock_missing_date_backfill':
        target_date = payload.get('target_date')
        if not target_date:
            raise ValueError('missing target_date for stock_missing_date_backfill')
        return await stock_main.retry_missing_trade_date_failures_once(target_date)

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
