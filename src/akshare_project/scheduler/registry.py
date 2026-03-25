from dataclasses import dataclass

import akshare as ak


@dataclass(frozen=True)
class SchedulerFunctionSpec:
    function_name: str
    source_group: str
    callable: object


FUNCTION_SPECS = {
    "stock_individual_info_em": SchedulerFunctionSpec("stock_individual_info_em", "eastmoney", ak.stock_individual_info_em),
    "stock_zh_a_hist": SchedulerFunctionSpec("stock_zh_a_hist", "eastmoney", ak.stock_zh_a_hist),
    "stock_zh_a_spot_em": SchedulerFunctionSpec("stock_zh_a_spot_em", "eastmoney", ak.stock_zh_a_spot_em),
    "fund_etf_spot_em": SchedulerFunctionSpec("fund_etf_spot_em", "eastmoney", ak.fund_etf_spot_em),
    "fund_etf_hist_em": SchedulerFunctionSpec("fund_etf_hist_em", "eastmoney", ak.fund_etf_hist_em),
    "fund_etf_category_sina": SchedulerFunctionSpec("fund_etf_category_sina", "sina", ak.fund_etf_category_sina),
    "fund_etf_hist_sina": SchedulerFunctionSpec("fund_etf_hist_sina", "sina", ak.fund_etf_hist_sina),
    "stock_zh_index_daily_em": SchedulerFunctionSpec("stock_zh_index_daily_em", "eastmoney", ak.stock_zh_index_daily_em),
    "index_global_hist_em": SchedulerFunctionSpec("index_global_hist_em", "eastmoney", ak.index_global_hist_em),
    "forex_spot_em": SchedulerFunctionSpec("forex_spot_em", "eastmoney", ak.forex_spot_em),
    "forex_hist_em": SchedulerFunctionSpec("forex_hist_em", "eastmoney", ak.forex_hist_em),
    "futures_hist_em": SchedulerFunctionSpec("futures_hist_em", "eastmoney", ak.futures_hist_em),
    "get_futures_daily": SchedulerFunctionSpec("get_futures_daily", "eastmoney", ak.get_futures_daily),
    "stock_zh_index_spot_sina": SchedulerFunctionSpec("stock_zh_index_spot_sina", "sina", ak.stock_zh_index_spot_sina),
    "index_zh_a_hist": SchedulerFunctionSpec("index_zh_a_hist", "sina", ak.index_zh_a_hist),
    "option_cffex_sz50_list_sina": SchedulerFunctionSpec("option_cffex_sz50_list_sina", "sina", ak.option_cffex_sz50_list_sina),
    "option_cffex_sz50_spot_sina": SchedulerFunctionSpec("option_cffex_sz50_spot_sina", "sina", ak.option_cffex_sz50_spot_sina),
    "option_cffex_sz50_daily_sina": SchedulerFunctionSpec("option_cffex_sz50_daily_sina", "sina", ak.option_cffex_sz50_daily_sina),
    "option_cffex_hs300_list_sina": SchedulerFunctionSpec("option_cffex_hs300_list_sina", "sina", ak.option_cffex_hs300_list_sina),
    "option_cffex_hs300_spot_sina": SchedulerFunctionSpec("option_cffex_hs300_spot_sina", "sina", ak.option_cffex_hs300_spot_sina),
    "option_cffex_hs300_daily_sina": SchedulerFunctionSpec("option_cffex_hs300_daily_sina", "sina", ak.option_cffex_hs300_daily_sina),
    "option_cffex_zz1000_list_sina": SchedulerFunctionSpec("option_cffex_zz1000_list_sina", "sina", ak.option_cffex_zz1000_list_sina),
    "option_cffex_zz1000_spot_sina": SchedulerFunctionSpec("option_cffex_zz1000_spot_sina", "sina", ak.option_cffex_zz1000_spot_sina),
    "option_cffex_zz1000_daily_sina": SchedulerFunctionSpec("option_cffex_zz1000_daily_sina", "sina", ak.option_cffex_zz1000_daily_sina),
    "fund_etf_spot_ths": SchedulerFunctionSpec("fund_etf_spot_ths", "ths", ak.fund_etf_spot_ths),
}


def get_function_spec(function_name: str) -> SchedulerFunctionSpec | None:
    return FUNCTION_SPECS.get(str(function_name or "").strip())


def resolve_callable_spec(func) -> SchedulerFunctionSpec | None:
    function_name = getattr(func, "__name__", "")
    return get_function_spec(function_name)
