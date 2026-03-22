import akshare as ak
print(ak.__version__)
import pandas as pd
#
# 获取沪深300指数的历史数据
# index_code = "301071"
# start_date = "20210924"
# end_date = "20210925"
# index_data = ak.stock_zh_a_hist(symbol=index_code, period="daily", start_date=start_date, end_date=end_date, adjust="hfq")
# index_data.to_csv('test.csv',index=False,encoding='utf-8-sig')
# all_stock = ak.stock_zh_a_spot_em()
# print(all_stock)
# all_stock.to_csv('allstock_em.csv',index=False,encoding='utf-8-sig')
# stock_code = 832
# stock_individual_info_em_df = ak.stock_individual_info_em(symbol=str(stock_code))
#
# # 数据预览
# print(stock_individual_info_em_df)
# forex_spot_em_df = ak.futures_hist_table_em()
# with pd.option_context(
#     'display.max_rows', None,
#     'display.max_columns', None,
#     'display.width', None,
#     'display.max_colwidth', None
# ):
#     print(forex_spot_em_df)

# futures_hist_em_df = ak.futures_hist_em(symbol="中证1000股指2603", period="daily")
# print(futures_hist_em_df)

# futures_zh_daily_sina_df =  ak.match_main_contract(symbol="cffex")
# with pd.option_context(
#     'display.max_rows', None,
#     'display.max_columns', None,
#     'display.width', None,
#     'display.max_colwidth', None
# ):
#     print(futures_zh_daily_sina_df)
# futures_zh_daily_sina_df = ak.futures_zh_daily_sina(symbol="IM2503")
# print(futures_zh_daily_sina_df)

# import akshare as ak
# import time
# import requests
#
# def fetch_with_retry(func, retry_delay=5, *args, **kwargs):
#     """无限重试调用函数，直到成功"""
#     while True:
#         try:
#             return func(*args, **kwargs)
#         except Exception as e:
#             # 判断是否是连接相关的异常（可选，更精确地捕获）
#             if isinstance(e, requests.exceptions.ConnectionError):
#                 print(f"连接失败: {e}，{retry_delay}秒后重试...")
#             else:
#                 print(f"调用失败: {e}，{retry_delay}秒后重试...")
#             time.sleep(retry_delay)
#
# # 获取A股实时行情（自动重试直到成功）
# all_stock = fetch_with_retry(ak.stock_zh_a_spot_em)
#
# # 保存为CSV
# all_stock.to_csv('allstock_em.csv', index=False, encoding='utf-8-sig')
#
# # 查询个股信息
# stock_code = 603505
# stock_individual_info_em_df = ak.stock_individual_info_em(symbol=str(stock_code))
# print(stock_individual_info_em_df)


# all_stock = ak.option_cffex_zz1000_spot_sina()
# print(all_stock)

if __name__ == "__main__":
    # all_stock = ak.option_cffex_zz1000_list_sina()
    # with pd.option_context(
    #     'display.max_rows', None,
    #     'display.max_columns', None,
    #     'display.width', None,
    #     'display.max_colwidth', None
    # ):
    #     print(all_stock)
    # df_usd_index = ak.index_global_hist_em(
    #     symbol="美元指数"
    # )
    # print(df_usd_index)
    index_code = "301071"
    start_date = "20210924"
    end_date = "20210925"
    index_data = ak.stock_zh_a_hist(symbol=index_code, period="daily", start_date=start_date, end_date=end_date, adjust="hfq")
    print(index_data)