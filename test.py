# import akshare as ak
# import pandas as pd
#
# # 获取沪深300指数的历史数据
# # index_code = "300043"
# # start_date = "20200101"
# # end_date = "20240901"
# # index_data = ak.stock_zh_a_hist(symbol=index_code, period="daily", start_date=start_date, end_date=end_date, adjust="hfq")
# # index_data.to_csv('test.csv',index=False,encoding='utf-8-sig')
# all_stock = ak.stock_zh_a_spot_em()
# all_stock.to_csv('allstock_em.csv',index=False,encoding='utf-8-sig')
# stock_code = 603505
# stock_individual_info_em_df = ak.stock_individual_info_em(symbol=str(stock_code))
#
# # 数据预览
# print(stock_individual_info_em_df)



import akshare as ak
import time
import requests

def fetch_with_retry(func, retry_delay=5, *args, **kwargs):
    """无限重试调用函数，直到成功"""
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # 判断是否是连接相关的异常（可选，更精确地捕获）
            if isinstance(e, requests.exceptions.ConnectionError):
                print(f"连接失败: {e}，{retry_delay}秒后重试...")
            else:
                print(f"调用失败: {e}，{retry_delay}秒后重试...")
            time.sleep(retry_delay)

# 获取A股实时行情（自动重试直到成功）
all_stock = fetch_with_retry(ak.stock_zh_a_spot_em)

# 保存为CSV
all_stock.to_csv('allstock_em.csv', index=False, encoding='utf-8-sig')

# 查询个股信息
stock_code = 603505
stock_individual_info_em_df = ak.stock_individual_info_em(symbol=str(stock_code))
print(stock_individual_info_em_df)