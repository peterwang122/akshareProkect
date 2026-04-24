from dataclasses import dataclass

import akshare as ak
from akshare.index import index_option_qvix as ak_qvix


@dataclass(frozen=True)
class SchedulerFunctionSpec:
    function_name: str
    source_group: str
    callable: object


def clear_optbbs_qvix_cache():
    cache_getter = getattr(ak_qvix, "__get_optbbs_daily", None)
    if hasattr(cache_getter, "cache_clear"):
        cache_getter.cache_clear()


def fresh_qvix_callable(func):
    def wrapper(*args, **kwargs):
        clear_optbbs_qvix_cache()
        return func(*args, **kwargs)

    wrapper.__name__ = getattr(func, "__name__", "qvix_callable")
    return wrapper


FUNCTION_SPECS = {
    "stock_individual_info_em": SchedulerFunctionSpec("stock_individual_info_em", "eastmoney", ak.stock_individual_info_em),
    "stock_zh_a_hist": SchedulerFunctionSpec("stock_zh_a_hist", "eastmoney", ak.stock_zh_a_hist),
    "stock_zh_a_spot_em": SchedulerFunctionSpec("stock_zh_a_spot_em", "eastmoney", ak.stock_zh_a_spot_em),
    "stock_info_sh_name_code": SchedulerFunctionSpec("stock_info_sh_name_code", "sse", ak.stock_info_sh_name_code),
    "stock_info_sz_name_code": SchedulerFunctionSpec("stock_info_sz_name_code", "szse", ak.stock_info_sz_name_code),
    "stock_info_bj_name_code": SchedulerFunctionSpec("stock_info_bj_name_code", "bse", ak.stock_info_bj_name_code),
    "stock_zh_a_spot": SchedulerFunctionSpec("stock_zh_a_spot", "sina", ak.stock_zh_a_spot),
    "stock_zh_a_hist_tx": SchedulerFunctionSpec("stock_zh_a_hist_tx", "tencent", ak.stock_zh_a_hist_tx),
    "stock_zh_a_daily": SchedulerFunctionSpec("stock_zh_a_daily", "sina", ak.stock_zh_a_daily),
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
    "stock_zh_index_daily": SchedulerFunctionSpec("stock_zh_index_daily", "sina", ak.stock_zh_index_daily),
    "index_us_stock_sina": SchedulerFunctionSpec("index_us_stock_sina", "sina", ak.index_us_stock_sina),
    "stock_hk_index_spot_sina": SchedulerFunctionSpec("stock_hk_index_spot_sina", "sina", ak.stock_hk_index_spot_sina),
    "stock_hk_index_daily_sina": SchedulerFunctionSpec("stock_hk_index_daily_sina", "sina", ak.stock_hk_index_daily_sina),
    "index_option_50etf_qvix": SchedulerFunctionSpec("index_option_50etf_qvix", "optbbs", fresh_qvix_callable(ak.index_option_50etf_qvix)),
    "index_option_300etf_qvix": SchedulerFunctionSpec("index_option_300etf_qvix", "optbbs", fresh_qvix_callable(ak.index_option_300etf_qvix)),
    "index_option_500etf_qvix": SchedulerFunctionSpec("index_option_500etf_qvix", "optbbs", fresh_qvix_callable(ak.index_option_500etf_qvix)),
    "index_option_cyb_qvix": SchedulerFunctionSpec("index_option_cyb_qvix", "optbbs", fresh_qvix_callable(ak.index_option_cyb_qvix)),
    "index_option_kcb_qvix": SchedulerFunctionSpec("index_option_kcb_qvix", "optbbs", fresh_qvix_callable(ak.index_option_kcb_qvix)),
    "index_news_sentiment_scope": SchedulerFunctionSpec("index_news_sentiment_scope", "chinascope", ak.index_news_sentiment_scope),
    "index_zh_a_hist": SchedulerFunctionSpec("index_zh_a_hist", "eastmoney", ak.index_zh_a_hist),
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


def get_registered_source_groups() -> list[str]:
    return sorted({spec.source_group for spec in FUNCTION_SPECS.values() if str(spec.source_group or "").strip()})
