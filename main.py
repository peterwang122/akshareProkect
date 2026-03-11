import json
import akshare as ak
import pandas as pd
import os
from util.db_tool import DbTools  # 请确保这个模块存在
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def save_progress(stock_code, date):
    """保存进度到文件"""
    with open('progress.log', 'a') as f:
        f.write(f'{stock_code},{date}\n')


def load_progress():
    """加载进度文件，返回已处理的记录集合"""
    if not os.path.exists('progress.log'):
        return set()
    with open('progress.log', 'r') as f:
        lines = f.readlines()
    processed = set(line.strip() for line in lines)
    return processed


def log_error(stock_code, date, error_message):
    """记录错误信息"""
    with open('error.log', 'a') as f:
        f.write(f"{stock_code},{date},{error_message}\n")


def process_stock(item, processed):
    stock_code = item['代码']

    try:
        stock_individual_info_em_df = ak.stock_individual_info_em(symbol=str(stock_code))
        listing_date = stock_individual_info_em_df.loc[stock_individual_info_em_df['item'] == '上市时间', 'value'].values[0]
        print(listing_date)
        index_data = ak.stock_zh_a_hist(symbol=str(stock_code), period="daily", start_date=listing_date,
                                        end_date="20260308", adjust="hfq")
        print(index_data)

        for index, row in index_data.iterrows():
            # 跳过已处理的记录
            progress_key = f"{stock_code},{row['日期']}"
            if progress_key in processed:
                continue

            # 插入数据库，并处理可能的错误
            retry_count = 3
            for attempt in range(retry_count):
                try:
                    DbTools().create_stock_info(
                        stock_code=row['股票代码'],
                        open_price=row['开盘'],
                        close_price=row['收盘'],
                        high_price=row['最高'],
                        low_price=row['最低'],
                        volume=row['成交量'],
                        turnover=row['成交额'],
                        amplitude=row['振幅'],
                        price_change_rate=row['涨跌幅'],
                        price_change_amount=row['涨跌额'],
                        turnover_rate=row['换手率'],
                        date=row['日期']
                    )
                    # 保存进度
                    save_progress(stock_code, row['日期'])
                    break  # 插入成功，退出重试循环
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed: {e}")
                    if attempt == retry_count - 1:
                        log_error(stock_code, row['日期'], str(e))  # 记录错误信息
                    else:
                        time.sleep(5)  # 等待几秒钟后重试

    except Exception as e:
        error_message = f"Error processing {stock_code}: {e}"
        print(error_message)
        log_error(stock_code, "N/A", error_message)  # 记录无法获取数据的错误


def run():
    df = pd.read_csv('allstock_em.csv')
    df_data = json.loads(df.to_json(orient='records'))
    processed = load_progress()  # 加载已处理的记录

    # 使用多线程处理
    with ThreadPoolExecutor(max_workers=10) as executor:  # 你可以调整max_workers以控制线程数
        futures = {executor.submit(process_stock, item, processed): item['代码'] for item in df_data}

        for future in as_completed(futures):
            stock_code = futures[future]
            try:
                future.result()  # 获取线程执行结果
            except Exception as e:
                print(f"Error in thread processing {stock_code}: {e}")


if __name__ == '__main__':
    run()
