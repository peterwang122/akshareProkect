import asyncio
from datetime import datetime

import cffex_main
import forex_main
import futures_main
import index_main
import option_main
from util.db_tool import DbTools


async def save_task_failure(db_tools, task_name, error_message):
    await db_tools.upsert_failed_task({
        'task_name': task_name,
        'task_stage': 'task',
        'task_key': task_name,
        'payload_json': {
            'task_name': task_name,
            'task_stage': 'task',
        },
        'error_message': error_message,
    })


async def resolve_task_failure(db_tools, task_name):
    await db_tools.resolve_failed_task_by_identity(task_name, 'task', task_name)


async def run_task(task_name, coro_factory, db_tools):
    print(f'[{task_name}] started')
    try:
        result = await coro_factory()
        await resolve_task_failure(db_tools, task_name)
        print(f'[{task_name}] finished: {result}')
        return True, result
    except Exception as exc:
        await save_task_failure(db_tools, task_name, str(exc))
        print(f'[{task_name}] failed: {exc}')
        return False, str(exc)


async def main():
    started_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'daily runner started at {started_at}')
    db_tools = DbTools()
    await db_tools.init_pool()

    try:
        tasks = [
            ('index_daily', index_main.sync_daily_from_spot),
            ('cffex_daily', lambda: cffex_main.sync_latest_daily_data(headless=True)),
            ('forex_daily', forex_main.sync_daily_from_spot),
            ('usd_index_once', forex_main.sync_usd_index_once),
            ('futures_daily', futures_main.sync_today),
            ('option_daily', lambda: option_main.sync_daily(record_failures=True)),
        ]

        results = []
        for task_name, coro_factory in tasks:
            results.append((task_name, *await run_task(task_name, coro_factory, db_tools)))

        success_count = sum(1 for _, success, _ in results if success)
        failed_count = len(results) - success_count
        print(f'daily runner finished: success={success_count}, failed={failed_count}')

        for task_name, success, detail in results:
            status = 'SUCCESS' if success else 'FAILED'
            print(f'{task_name}: {status} -> {detail}')
    finally:
        await db_tools.close()


if __name__ == '__main__':
    asyncio.run(main())
