import asyncio
import json
import sys
import threading
from http.server import ThreadingHTTPServer

import pytest
import requests

import run
from akshare_project.core import runtime_config
from akshare_project.services import stock_temp_service


def _set_lan_test_env(monkeypatch, allowed_collectors=""):
    monkeypatch.setenv("AK_RUNTIME_PROFILE", "lan-test")
    monkeypatch.setenv("AK_COLLECTION_EXECUTION_MODE", "allowlist")
    monkeypatch.setenv("AK_ALLOWED_COLLECTORS", allowed_collectors)
    monkeypatch.setenv("AK_DB_NAME", "stock_info_test")


def _clear_runtime_env(monkeypatch):
    for name in (
        "AK_RUNTIME_PROFILE",
        "RUNTIME_PROFILE",
        "AK_COLLECTION_EXECUTION_MODE",
        "COLLECTION_EXECUTION_MODE",
        "AK_ALLOWED_COLLECTORS",
        "ALLOWED_COLLECTORS",
        "AK_DB_NAME",
    ):
        monkeypatch.delenv(name, raising=False)


def _start_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), stock_temp_service.StockTempHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _post(path, body):
    server, thread = _start_server()
    try:
        port = server.server_address[1]
        return requests.post(f"http://127.0.0.1:{port}{path}", json=body, timeout=10)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_defaults_without_env(monkeypatch):
    _clear_runtime_env(monkeypatch)
    assert runtime_config.get_runtime_profile() == "production"
    assert runtime_config.get_collection_execution_mode() == "enabled"
    assert runtime_config.is_lan_test() is False
    assert runtime_config.is_collector_allowed("stock_daily") is True


def test_allowlist_parsing_and_gating(monkeypatch):
    monkeypatch.setenv("AK_RUNTIME_PROFILE", "lan-test")
    monkeypatch.setenv("AK_COLLECTION_EXECUTION_MODE", "allowlist")
    monkeypatch.setenv("AK_ALLOWED_COLLECTORS", " stock_daily , quant_index_daily ")
    assert runtime_config.is_lan_test() is True
    assert runtime_config.get_allowed_collectors() == {
        "stock_daily",
        "quant_index_daily",
    }
    assert runtime_config.is_collector_allowed("stock_daily") is True
    assert runtime_config.is_collector_allowed("QUANT_INDEX_DAILY") is True
    assert runtime_config.is_collector_allowed("etf_daily") is False
    assert runtime_config.is_collector_allowed("") is False


def test_cli_collector_key_mapping():
    assert runtime_config.collector_key_for_cli("stock", "daily") == "stock_daily"
    assert runtime_config.collector_key_for_cli("index", "backfill") == "index_cn_backfill"
    assert runtime_config.collector_key_for_cli("index", "daily") == "index_cn_daily"
    assert runtime_config.collector_key_for_cli("douyin", "daily") == "douyin_coze_emotion_daily"
    assert runtime_config.collector_key_for_cli("global-risk", "daily-tech") == (
        "csi_tech_concentration_daily"
    )
    assert runtime_config.collector_key_for_cli("global-risk", "daily-concentration") == (
        "a_share_turnover_concentration_daily"
    )
    assert runtime_config.collector_key_for_cli(
        "index",
        "daily-cn-market-fear-greed",
    ) == "index_cn_market_fear_greed_daily"
    assert runtime_config.collector_key_for_cli("option-minute", "daily") == "option_minute_daily"
    assert runtime_config.collector_key_for_cli("exchange-option", "repair-backfill") == (
        "exchange_option_repair_backfill"
    )
    assert runtime_config.collector_key_for_cli("emotion-excel", "import") == "excel_emotion_import"
    assert runtime_config.collector_key_for_cli("unknown-domain", "anything") == (
        "unknown_domain_anything"
    )


def test_lan_test_guard_requires_allowlist_mode(monkeypatch):
    monkeypatch.setenv("AK_RUNTIME_PROFILE", "lan-test")
    monkeypatch.setenv("AK_COLLECTION_EXECUTION_MODE", "enabled")
    with pytest.raises(RuntimeError, match="allowlist"):
        runtime_config.enforce_lan_test_runtime_guard(db_name="stock_info_test")


def test_lan_test_guard_requires_test_db(monkeypatch):
    monkeypatch.setenv("AK_RUNTIME_PROFILE", "lan-test")
    monkeypatch.setenv("AK_COLLECTION_EXECUTION_MODE", "allowlist")
    with pytest.raises(RuntimeError, match="stock_info_test"):
        runtime_config.enforce_lan_test_runtime_guard(db_name="stock_info")


def test_lan_test_guard_accepts_test_env(monkeypatch):
    _set_lan_test_env(monkeypatch)
    runtime_config.enforce_lan_test_runtime_guard()


def test_cli_rejects_command_when_not_allowlisted(monkeypatch):
    _set_lan_test_env(monkeypatch, allowed_collectors="")
    monkeypatch.setattr(sys, "argv", ["run.py", "stock", "daily"])

    def _should_not_run():
        raise AssertionError("collector main must not be called when denied")

    monkeypatch.setattr(run.stock, "main", _should_not_run)
    with pytest.raises(SystemExit) as exc_info:
        asyncio.run(run.dispatch())
    assert exc_info.value.code == 2


def test_cli_rejects_backfill_command_when_not_allowlisted(monkeypatch):
    _set_lan_test_env(monkeypatch, allowed_collectors="stock_daily")
    monkeypatch.setattr(sys, "argv", ["run.py", "index", "backfill"])

    def _should_not_run():
        raise AssertionError("collector main must not be called when denied")

    monkeypatch.setattr(run.index, "main", _should_not_run)
    with pytest.raises(SystemExit) as exc_info:
        asyncio.run(run.dispatch())
    assert exc_info.value.code == 2


def test_cli_allows_command_when_allowlisted(monkeypatch):
    _set_lan_test_env(monkeypatch, allowed_collectors="stock_daily")
    monkeypatch.setattr(sys, "argv", ["run.py", "stock", "daily"])
    calls = []

    async def _fake_stock_main():
        calls.append(sys.argv[:])

    monkeypatch.setattr(run.stock, "main", _fake_stock_main)
    asyncio.run(run.dispatch())
    assert calls == [["stock", "daily"]]


def test_daily_route_denied_before_handler(monkeypatch):
    monkeypatch.setenv("AK_COLLECTION_EXECUTION_MODE", "allowlist")
    monkeypatch.setenv("AK_ALLOWED_COLLECTORS", "")

    handler = stock_temp_service.StockTempHandler.__new__(stock_temp_service.StockTempHandler)
    captured = {}
    handler._send_json = lambda status, payload: captured.update(
        status=status,
        payload=payload,
    )

    async def _should_not_run():
        raise AssertionError("handler must not be called when denied")

    route = stock_temp_service.DailyRoute(
        path="/collect-stock-daily",
        task_name="stock_daily",
        handler=_should_not_run,
        direct_network=True,
    )
    handler._run_daily_route(route, {})
    assert captured["status"] == 403
    assert captured["payload"]["status"] == "DENIED"
    assert captured["payload"]["task_name"] == "stock_daily"


def test_daily_route_allowed_runs_handler(monkeypatch):
    monkeypatch.setenv("AK_COLLECTION_EXECUTION_MODE", "allowlist")
    monkeypatch.setenv("AK_ALLOWED_COLLECTORS", "stock_daily")

    handler = stock_temp_service.StockTempHandler.__new__(stock_temp_service.StockTempHandler)
    captured = {}
    handler._send_json = lambda status, payload: captured.update(
        status=status,
        payload=payload,
    )

    async def _fake_sync_daily():
        return {"written": 3}

    route = stock_temp_service.DailyRoute(
        path="/collect-stock-daily",
        task_name="stock_daily",
        handler=_fake_sync_daily,
        direct_network=True,
    )
    handler._run_daily_route(route, {})
    assert captured["status"] == 200
    assert captured["payload"]["status"] == "SUCCESS"
    assert captured["payload"]["result"] == {"written": 3}


def test_http_legacy_collect_denied_before_network(monkeypatch):
    _set_lan_test_env(monkeypatch, allowed_collectors="")

    async def _should_not_run(**kwargs):
        raise AssertionError("collect_hfq_for_request must not be called when denied")

    monkeypatch.setattr(stock_temp_service.stock, "collect_hfq_for_request", _should_not_run)
    response = _post("/collect", {"stock_code": "600000"})
    assert response.status_code == 403
    payload = response.json()
    assert payload["status"] == "DENIED"
    assert payload["task_name"] == "stock_hfq_single"


def test_http_legacy_forex_denied_before_network(monkeypatch):
    _set_lan_test_env(monkeypatch, allowed_collectors="")

    async def _should_not_run(**kwargs):
        raise AssertionError("forex collect must not be called when denied")

    monkeypatch.setattr(
        stock_temp_service.forex,
        "collect_symbol_history_for_request",
        _should_not_run,
    )
    response = _post("/collect-forex", {"symbol_code": "USDCNH"})
    assert response.status_code == 403
    payload = response.json()
    assert payload["status"] == "DENIED"
    assert payload["task_name"] == "forex_collect"


def test_http_daily_route_denied_before_network(monkeypatch):
    _set_lan_test_env(monkeypatch, allowed_collectors="")

    async def _should_not_run():
        raise AssertionError("sync_daily must not be called when denied")

    monkeypatch.setattr(stock_temp_service.stock, "sync_daily", _should_not_run)
    response = _post("/collect-stock-daily", {})
    assert response.status_code == 403
    payload = response.json()
    assert payload["status"] == "DENIED"
    assert payload["task_name"] == "stock_daily"


def test_http_legacy_collect_allowed_when_allowlisted(monkeypatch):
    _set_lan_test_env(monkeypatch, allowed_collectors="stock_hfq_single")

    async def _fake_collect(**kwargs):
        return {"status": "SUCCESS", "stock_code": kwargs.get("stock_code")}

    monkeypatch.setattr(stock_temp_service.stock, "collect_hfq_for_request", _fake_collect)
    response = _post("/collect", {"stock_code": "600000"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "SUCCESS"
    assert payload["stock_code"] == "600000"


def test_legacy_route_keys_match_fit_collector_keys():
    assert stock_temp_service.LEGACY_ROUTE_COLLECTOR_KEYS == {
        "/collect": "stock_hfq_single",
        "/collect-forex": "forex_collect",
    }


def test_runner_composite_keys_forbidden_in_lan_test(monkeypatch):
    _set_lan_test_env(
        monkeypatch,
        allowed_collectors="runner_daily,runner_retry_failures",
    )
    assert runtime_config.is_collector_allowed("runner_daily") is False
    assert runtime_config.is_collector_allowed("runner_retry_failures") is False
    # 白名单里即便写了 runner 项，也不能把整个复合入口当单采集器放行
    assert runtime_config.get_allowed_collectors() == {
        "runner_daily",
        "runner_retry_failures",
    }


@pytest.mark.parametrize(
    "domain,command,route_path",
    [
        ("stock", "daily", "/collect-stock-daily"),
        ("index", "daily", "/collect-index-cn-daily"),
        ("index", "daily-qvix", "/collect-index-qvix-daily"),
        ("index", "daily-news-sentiment", "/collect-index-news-sentiment-daily"),
        ("index", "daily-csi-dividend", "/collect-index-csi-dividend-daily"),
        (
            "index",
            "daily-cn-market-fear-greed",
            "/collect-index-cn-market-fear-greed-daily",
        ),
        (
            "index",
            "daily-cn-baifenwei-fear-greed",
            "/collect-index-cn-baifenwei-fear-greed-daily",
        ),
        ("index", "daily-us-vix", "/collect-index-us-vix-daily"),
        ("index", "daily-us-fear-greed", "/collect-index-us-fear-greed-daily"),
        ("index", "daily-us-hedge-proxy", "/collect-index-us-hedge-proxy-daily"),
        ("douyin", "daily", "/collect-douyin-coze-emotion-daily"),
        ("quant-index", "daily", "/collect-quant-index-daily"),
        ("cffex", "daily", "/collect-cffex-daily"),
        ("forex", "daily", "/collect-forex-daily"),
        ("forex", "usd-daily", "/collect-usd-index-daily"),
        ("futures", "daily", "/collect-futures-daily"),
        ("futures", "daily-us-index", "/collect-us-index-futures-daily"),
        (
            "futures",
            "daily-us-index-official",
            "/collect-us-index-futures-official-daily",
        ),
        ("futures", "daily-hk-index", "/collect-hk-index-futures-daily"),
        ("etf", "daily", "/collect-etf-daily"),
        ("option", "daily", "/collect-option-daily"),
        ("exchange-option", "daily", "/collect-exchange-option-daily"),
        ("exchange-option", "stats-daily", "/collect-exchange-option-stats-daily"),
        ("option-minute", "daily", "/collect-option-minute-daily"),
        ("risk-free-rate", "daily", "/collect-cn-risk-free-rate-daily"),
        ("macro", "daily", "/collect-cn-macro-daily"),
        ("margin-trading", "daily", "/collect-margin-trading-daily"),
        ("fund-purchase-limit", "daily", "/collect-fund-purchase-limit-daily"),
        ("global-risk", "daily", "/collect-global-risk-daily"),
        ("global-risk", "daily-tech", "/collect-csi-tech-concentration-daily"),
        (
            "global-risk",
            "daily-concentration",
            "/collect-a-share-turnover-concentration-daily",
        ),
        ("emotion-excel", "import", "/import-emotion-excel"),
    ],
)
def test_cli_key_matches_http_route_key(domain, command, route_path):
    routes = stock_temp_service.build_daily_routes()
    assert route_path in routes, f"missing route {route_path}"
    assert runtime_config.collector_key_for_cli(domain, command) == (
        routes[route_path].task_name
    )


def test_health_payload_includes_runtime_fields_without_secrets(monkeypatch):
    _set_lan_test_env(monkeypatch, allowed_collectors="stock_daily, index_cn_daily")
    payload = stock_temp_service.build_health_payload()
    assert payload["runtime_profile"] == "lan-test"
    assert payload["database"] == "stock_info_test"
    assert payload["collection_execution_mode"] == "allowlist"
    assert payload["allowed_collectors"] == ["index_cn_daily", "stock_daily"]
    serialized = json.dumps(payload).lower()
    assert "passwd" not in serialized
    assert "password" not in serialized


def test_db_name_env_override(monkeypatch):
    monkeypatch.setenv("AK_DB_NAME", "stock_info_test")
    from akshare_project.db.config import load_db_info

    assert load_db_info()["db"] == "stock_info_test"
