"""局域网测试环境（lan-test）运行时配置与采集白名单。

测试机开发时通过环境变量开启防护，契约如下：

- ``AK_RUNTIME_PROFILE=lan-test``
- ``AK_COLLECTION_EXECUTION_MODE=allowlist``
- ``AK_ALLOWED_COLLECTORS=stock_daily,index_cn_daily``（空表示全部拒绝）
- ``AK_DB_NAME=stock_info_test``

所有采集入口（``run.py``、``stock_temp_service.py``、调度服务）都必须先经过
本模块的校验，才能在允许的情况下访问上游网络或写入数据库。
"""

import os


LAN_TEST_PROFILE = "lan-test"
LAN_TEST_DB_NAME = "stock_info_test"
LAN_TEST_FORBIDDEN_COLLECTOR_KEYS = {"runner_daily", "runner_retry_failures"}


def _first_env(names: tuple[str, ...]) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def get_runtime_profile() -> str:
    value = _first_env(("AK_RUNTIME_PROFILE", "RUNTIME_PROFILE"))
    return (value or "production").strip().lower()


def get_collection_execution_mode() -> str:
    value = _first_env(("AK_COLLECTION_EXECUTION_MODE", "COLLECTION_EXECUTION_MODE"))
    return (value or "enabled").strip().lower()


def get_allowed_collectors() -> set[str]:
    raw = _first_env(("AK_ALLOWED_COLLECTORS", "ALLOWED_COLLECTORS")) or ""
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def is_lan_test() -> bool:
    return get_runtime_profile() == LAN_TEST_PROFILE


def is_collector_allowed(collector_key: str) -> bool:
    """allowlist 模式下只放行白名单内的采集键；其他模式一律放行。"""
    key = str(collector_key or "").strip().lower()
    if is_lan_test() and key in LAN_TEST_FORBIDDEN_COLLECTOR_KEYS:
        # 复合入口会一次执行多个未单独获准的采集器，lan-test 下直接禁止。
        return False
    if get_collection_execution_mode() != "allowlist":
        return True
    if not key:
        return False
    return key in get_allowed_collectors()


def resolve_db_name() -> str:
    from akshare_project.db.config import load_db_info

    db_info = load_db_info()
    return str(db_info.get("db") or "").strip()


def enforce_lan_test_runtime_guard(db_name: str | None = None) -> None:
    """lan-test 模式下强制校验数据库名与采集模式，不满足时拒绝启动/执行。"""
    if not is_lan_test():
        return
    if get_collection_execution_mode() != "allowlist":
        raise RuntimeError(
            "lan-test requires AK_COLLECTION_EXECUTION_MODE=allowlist "
            f"(current: {get_collection_execution_mode()!r})"
        )
    resolved = (db_name or resolve_db_name()).strip()
    if resolved != LAN_TEST_DB_NAME:
        raise RuntimeError(
            f"lan-test requires AK_DB_NAME={LAN_TEST_DB_NAME} "
            f"(current: {resolved!r})"
        )


CLI_COLLECTOR_KEY_ALIASES = {
    ("stock", "daily"): "stock_daily",
    ("stock", "backfill"): "stock_backfill",
    ("stock", "repair-backfill"): "stock_repair_backfill",
    ("stock", "repair-daily-dates"): "stock_repair_daily_dates",
    ("stock", "repair-hist-metrics"): "stock_repair_hist_metrics",
    ("douyin", "daily"): "douyin_coze_emotion_daily",
    ("index", "daily"): "index_cn_daily",
    ("index", "backfill"): "index_cn_backfill",
    ("index", "backfill-bj899050"): "index_bj50_backfill",
    ("index", "backfill-us"): "index_us_backfill",
    ("index", "backfill-hk"): "index_hk_backfill",
    ("index", "backfill-qvix"): "index_qvix_backfill",
    ("index", "daily-qvix"): "index_qvix_daily",
    ("index", "backfill-news-sentiment"): "index_news_sentiment_backfill",
    ("index", "daily-news-sentiment"): "index_news_sentiment_daily",
    ("index", "daily-csi-dividend"): "index_csi_dividend_daily",
    ("index", "daily-cn-market-fear-greed"): "index_cn_market_fear_greed_daily",
    ("index", "daily-cn-baifenwei-fear-greed"): "index_cn_baifenwei_fear_greed_daily",
    ("index", "backfill-us-vix"): "index_us_vix_backfill",
    ("index", "daily-us-vix"): "index_us_vix_daily",
    ("index", "backfill-us-fear-greed"): "index_us_fear_greed_backfill",
    ("index", "daily-us-fear-greed"): "index_us_fear_greed_daily",
    ("index", "backfill-us-hedge-proxy"): "index_us_hedge_proxy_backfill",
    ("index", "daily-us-hedge-proxy"): "index_us_hedge_proxy_daily",
    ("index", "backfill-us-market-sentiment"): "index_us_market_sentiment_backfill",
    ("index", "daily-us-market-sentiment"): "index_us_market_sentiment_daily",
    ("quant-index", "daily"): "quant_index_daily",
    ("quant-index", "backfill"): "quant_index_backfill",
    ("quant-index", "refresh-breadth"): "quant_index_refresh_breadth",
    ("quant-index", "repair-recent"): "quant_index_repair_recent",
    ("quant-index", "repair-market-recent"): "quant_index_repair_market_recent",
    ("quant-index", "repair-market-previous"): "quant_index_repair_market_previous",
    ("cffex", "daily"): "cffex_daily",
    ("cffex", "backfill"): "cffex_backfill",
    ("cffex", "single"): "cffex_single",
    ("forex", "daily"): "forex_daily",
    ("forex", "backfill"): "forex_backfill",
    ("forex", "repair-history"): "forex_repair_history",
    ("forex", "usd-backfill"): "usd_index_backfill",
    ("forex", "usd-daily"): "usd_index_daily",
    ("forex", "usd-once"): "usd_index_once",
    ("futures", "daily"): "futures_daily",
    ("futures", "backfill"): "futures_backfill",
    ("futures", "trade-date"): "futures_trade_date",
    ("futures", "market-daily"): "futures_market_daily",
    ("futures", "market-backfill"): "futures_market_backfill",
    ("futures", "hist-daily"): "futures_hist_daily",
    ("futures", "hist-backfill"): "futures_hist_backfill",
    ("futures", "daily-us-index"): "us_index_futures_daily",
    ("futures", "daily-us-index-official"): "us_index_futures_official_daily",
    ("futures", "daily-hk-index"): "hk_index_futures_daily",
    ("futures", "backfill-us-index"): "us_index_futures_backfill",
    ("futures", "backfill-us-index-official"): "us_index_futures_official_backfill",
    ("futures", "backfill-hk-index"): "hk_index_futures_backfill",
    ("etf", "daily"): "etf_daily",
    ("etf", "backfill"): "etf_backfill",
    ("etf", "weekly-repair"): "etf_weekly_repair",
    ("etf", "repair-backfill"): "etf_repair_backfill",
    ("option", "daily"): "option_daily",
    ("option", "backfill"): "option_backfill",
    ("option", "repair-backfill"): "option_repair_backfill",
    ("option-minute", "daily"): "option_minute_daily",
    ("option-minute", "backfill"): "option_minute_backfill",
    ("exchange-option", "daily"): "exchange_option_daily",
    ("exchange-option", "stats-daily"): "exchange_option_stats_daily",
    ("exchange-option", "backfill"): "exchange_option_backfill",
    ("exchange-option", "repair-backfill"): "exchange_option_repair_backfill",
    ("risk-free-rate", "daily"): "cn_risk_free_rate_daily",
    ("risk-free-rate", "backfill"): "cn_risk_free_rate_backfill",
    ("macro", "daily"): "cn_macro_daily",
    ("macro", "backfill"): "cn_macro_backfill",
    ("margin-trading", "daily"): "margin_trading_daily",
    ("margin-trading", "backfill"): "margin_trading_backfill",
    ("fund-purchase-limit", "daily"): "fund_purchase_limit_daily",
    ("fund-purchase-limit", "backfill"): "fund_purchase_limit_backfill",
    ("global-risk", "daily"): "global_risk_daily",
    ("global-risk", "daily-tech"): "csi_tech_concentration_daily",
    ("global-risk", "daily-concentration"): "a_share_turnover_concentration_daily",
    ("global-risk", "backfill"): "global_risk_backfill",
    ("runner", "daily"): "runner_daily",
    ("runner", "retry-failures"): "runner_retry_failures",
    ("emotion-excel", "import"): "excel_emotion_import",
}


def collector_key_for_cli(domain: str, command: str) -> str:
    """把 run.py 的 domain/command 映射为稳定采集键。

    优先使用别名表（与 stock-temp 路由 / FIT 任务键对齐），未登记的
    命令回退为 ``<domain>_<command>``，保证新增命令默认也不会绕过白名单。
    """
    domain_key = str(domain or "").strip().lower()
    command_key = str(command or "").strip().lower()
    key = CLI_COLLECTOR_KEY_ALIASES.get((domain_key, command_key))
    if key:
        return key
    return f"{domain_key.replace('-', '_')}_{command_key.replace('-', '_')}"
