import asyncio
from datetime import datetime

from akshare_project.collectors import cffex, etf, forex, futures, index, option
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.db.db_tool import DbTools

LOGGER = get_logger('runner')


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


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
            ('index_daily', index.sync_daily_from_spot),
            ('cffex_daily', lambda: cffex.sync_latest_daily_data(headless=True)),
            ('forex_daily', forex.sync_daily_from_spot),
            ('usd_index_once', forex.sync_usd_index_once),
            ('futures_daily', futures.sync_today),
            ('etf_daily', etf.sync_daily),
            ('option_daily', lambda: option.sync_daily(record_failures=True)),
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
