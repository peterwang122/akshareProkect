import asyncio
import json
import os
import time
from datetime import datetime

import akshare as ak
import pandas as pd

from util.db_tool import DbTools

API_RETRY_COUNT = 5
API_RETRY_SLEEP_SECONDS = 3
MAX_CONCURRENCY = 8


def normalize_stock_code(stock_code):
    """标准化股票代码，避免 000356 被解析为 356。"""
    code = str(stock_code).strip()
    if '.' in code:
        code = code.split('.')[0]
    code = ''.join(ch for ch in code if ch.isdigit())
    if not code:
        return ""
    return code.zfill(6)


def save_progress_batch(progress_lines):
    """批量保存进度，减少频繁文件 IO。"""
    if not progress_lines:
        return
    with open('progress.log', 'a') as f:
        f.writelines(progress_lines)


def load_progress():
    """加载进度文件，返回已处理的记录集合"""
    if not os.path.exists('progress.log'):
        return set()
    with open('progress.log', 'r') as f:
        lines = f.readlines()
    return set(line.strip() for line in lines)


def log_error(stock_code, date, error_message):
    """记录错误信息"""
    with open('error.log', 'a') as f:
        f.write(f"{stock_code},{date},{error_message}\n")


def fetch_with_retry(func, *args, retries=API_RETRY_COUNT, sleep_seconds=API_RETRY_SLEEP_SECONDS, **kwargs):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            if attempt < retries:
                print(f"{func.__name__} attempt {attempt}/{retries} failed: {e}")
                time.sleep(sleep_seconds)
    raise last_error


def get_stock_history(stock_code, end_date):
    """同步调用 akshare，带重试，供 asyncio.to_thread 使用。"""
    info_df = fetch_with_retry(ak.stock_individual_info_em, symbol=stock_code)
    listing_date_series = info_df.loc[info_df['item'] == '上市时间', 'value']
    if listing_date_series.empty:
        raise ValueError(f"未找到上市时间: {stock_code}")
    listing_date = str(listing_date_series.values[0])

    history_df = fetch_with_retry(
        ak.stock_zh_a_hist,
        symbol=stock_code,
        period="daily",
        start_date=listing_date,
        end_date=end_date,
        adjust="hfq",
    )
    return history_df


async def process_stock(item, processed, db_tools, semaphore, progress_lock, end_date):
    raw_code = item['代码']
    stock_code = normalize_stock_code(raw_code)
    if not stock_code:
        log_error(raw_code, "N/A", "invalid stock code")
        return

    try:
        async with semaphore:
            index_data = await asyncio.to_thread(get_stock_history, stock_code, end_date)

        if index_data is None or index_data.empty:
            return

        pending_updates = []
        new_progress_lines = []

        for _, row in index_data.iterrows():
            row_date = str(row['日期'])
            progress_key = f"{stock_code},{row_date}"
            if progress_key in processed:
                continue

            pending_updates.append({
                'stock_code': normalize_stock_code(row.get('股票代码', stock_code)) or stock_code,
                'open_price': row['开盘'],
                'close_price': row['收盘'],
                'high_price': row['最高'],
                'low_price': row['最低'],
                'volume': row['成交量'],
                'turnover': row['成交额'],
                'amplitude': row['振幅'],
                'price_change_rate': row['涨跌幅'],
                'price_change_amount': row['涨跌额'],
                'turnover_rate': row['换手率'],
                'date': row_date,
            })
            new_progress_lines.append(f"{progress_key}\n")

        if not pending_updates:
            return

        await db_tools.batch_stock_info(pending_updates)

        async with progress_lock:
            await asyncio.to_thread(save_progress_batch, new_progress_lines)
            processed.update(line.strip() for line in new_progress_lines)

    except Exception as e:
        error_message = f"Error processing {stock_code}: {e}"
        print(error_message)
        log_error(stock_code, "N/A", error_message)


async def run():
    df = pd.read_csv('allstock_em.csv', dtype={'代码': str})
    df_data = json.loads(df.to_json(orient='records'))
    processed = load_progress()

    db_tools = DbTools()
    await db_tools.init_pool()

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    progress_lock = asyncio.Lock()
    end_date = datetime.now().strftime("%Y%m%d")

    try:
        tasks = [
            process_stock(item, processed, db_tools, semaphore, progress_lock, end_date)
            for item in df_data
        ]
        await asyncio.gather(*tasks)
    finally:
        await db_tools.close()


if __name__ == '__main__':
    asyncio.run(run())
