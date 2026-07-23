import asyncio
import os

import pytest

from akshare_project.collectors import stock
from akshare_project.core.network import direct_network_env, without_proxy_env


def test_build_sse_official_row_converts_trade_amount_from_ten_thousand_yuan():
    stock_row = {
        "stock_code": "600000",
        "prefixed_code": "sh600000",
        "stock_name": "浦发银行",
    }
    result_row = {
        "SEC_NAME": "浦发银行",
        "TX_DATE": "2026-04-30",
        "OPEN_PRICE": "11.23",
        "CLOSE_PRICE": "11.34",
        "HIGH_PRICE": "11.45",
        "LOW_PRICE": "11.10",
        "CHANGE_PRICE": "0.12",
        "CHANGE_RATE": "1.07",
        "TRADE_VOL": "7843.88",
        "TRADE_AMT": "72893.99",
        "TOTAL_VALUE": "33299877.12",
        "NEGO_VALUE": "33299877.12",
        "PE_RATE": "6.25",
        "TO_RATE": "0.27",
        "SWING_RATE": "3.12",
    }

    row = stock.build_sse_official_row(stock_row, "2026-04-30", result_row)

    assert row["turnover_amount"] == pytest.approx(728_939_900.0)
    assert row["volume"] == pytest.approx(78_438_800.0)
    assert row["pre_close_price"] == pytest.approx(11.22)
    assert row["total_market_value"] == pytest.approx(332_998_771_200.0)
    assert row["raw_trading_json"] == result_row
    assert row["raw_metrics_json"] == result_row


def test_build_szse_official_row_merges_daily_and_key_metrics():
    stock_row = {
        "stock_code": "000001",
        "prefixed_code": "sz000001",
        "stock_name": "平安银行",
    }
    daily_row = stock.normalize_szse_history_row(
        ["2026-04-30", "11.00", "11.20", "10.90", "11.35", "0.20", "1.82", "936958", "1048807160.00"]
    )
    metrics_payload = {
        "lastDate": "2026-04-30",
        "data": [
            {
                "now_sjzz": "2173.91",
                "now_ltsz": "2173.88",
                "now_zgb": "194.06",
                "now_ltgb": "194.06",
                "now_syl": "4.87",
                "now_hsl": "0.48",
            }
        ],
    }

    row = stock.build_szse_official_row(stock_row, "2026-04-30", daily_row, metrics_payload)

    assert row["stock_name"] == "平安银行"
    assert row["volume"] == pytest.approx(93_695_800.0)
    assert row["turnover_amount"] == "1048807160.00"
    assert row["pre_close_price"] == pytest.approx(11.0)
    assert row["total_market_value"] == pytest.approx(217_391_000_000.0)
    assert row["circulating_share_capital"] == pytest.approx(19_406_000_000.0)
    assert row["raw_trading_json"] == daily_row["raw"]
    assert row["raw_metrics_json"] == metrics_payload


def test_build_szse_official_row_keeps_historical_trade_data_when_metrics_are_newer():
    stock_row = {
        "stock_code": "000001",
        "prefixed_code": "sz000001",
        "stock_name": "平安银行",
    }
    daily_row = stock.normalize_szse_history_row(
        ["2026-07-10", "11.00", "11.20", "10.90", "11.35", "0.20", "1.82", "936958", "1048807160.00"]
    )
    metrics_payload = {
        "lastDate": "2026-07-13",
        "data": [{"now_sjzz": "2173.91", "now_syl": "4.87"}],
    }

    row = stock.build_szse_official_row(
        stock_row,
        "2026-07-10",
        daily_row,
        metrics_payload,
        allow_historical_metrics_fallback=True,
    )

    assert row["trade_date"] == "2026-07-10"
    assert row["turnover_amount"] == "1048807160.00"
    assert row["total_market_value"] is None
    assert row["pe_rate"] is None
    assert row["data_source"] == "szse_official_daily_history_no_metrics"
    assert row["raw_metrics_json"]["status"] == "metrics_not_available_for_target_date"
    assert row["raw_metrics_json"]["source_latest_date"] == "2026-07-13"


def test_official_exchange_rate_limiter_spacing():
    current = [100.0]
    sleeps = []

    def monotonic():
        return current[0]

    async def sleep(seconds):
        sleeps.append(seconds)
        current[0] += seconds

    limiter = stock.OfficialExchangeRateLimiter(
        interval_seconds=2,
        sleep_func=sleep,
        monotonic_func=monotonic,
    )

    async def run_waits():
        await limiter.wait()
        await limiter.wait()
        current[0] += 1.25
        await limiter.wait()

    asyncio.run(run_waits())

    assert sleeps == pytest.approx([2.0, 0.75])


def test_official_exchange_http_client_retries_transient_connection_error(monkeypatch):
    sleeps = []
    attempts = []
    client = stock.OfficialExchangeHttpClient(
        "SH",
        limiter=stock.OfficialExchangeRateLimiter(interval_seconds=0),
        max_attempts=3,
        retry_backoff_seconds=2,
        sleep_func=lambda seconds: _record_sleep(sleeps, seconds),
    )

    def fake_get_text_sync(_url, _params, _headers):
        attempts.append(1)
        if len(attempts) == 1:
            raise stock.requests.ConnectionError("temporary disconnect")
        return "ok"

    async def run_request():
        monkeypatch.setattr(client, "_get_text_sync", fake_get_text_sync)
        try:
            return await client.get_text("https://example.test")
        finally:
            client.close()

    async def _unused():
        return None

    result = asyncio.run(run_request())

    assert result == "ok"
    assert len(attempts) == 2
    assert sleeps == [2.0]


async def _record_sleep(target, seconds):
    target.append(seconds)


def test_sync_exchange_official_daily_starts_sh_and_sz_collectors_concurrently(monkeypatch):
    class FakeDbTools:
        def __init__(self):
            self.upserted_rows = []

        async def ensure_stock_exchange_official_daily_table(self):
            return None

        async def get_all_stock_info_rows(self):
            return [
                {"stock_code": "600000", "prefixed_code": "sh600000", "exchange": "SH", "security_type": "A"},
                {"stock_code": "000001", "prefixed_code": "sz000001", "exchange": "SZ", "security_type": "A"},
            ]

        async def upsert_stock_exchange_official_daily_data(self, rows):
            self.upserted_rows.extend(rows)
            return len(rows)

    started = []
    both_started = asyncio.Event()

    async def fake_collect(exchange, stock_rows, target_date, db_tools=None, request_interval_seconds=2.0):
        started.append(exchange)
        if len(started) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=0.2)
        if db_tools is not None:
            await db_tools.upsert_stock_exchange_official_daily_data([{"exchange": exchange, "trade_date": target_date}])
        return {
            "exchange": exchange,
            "target_count": len(stock_rows),
            "row_count": 1,
            "upserted_count": 1,
            "turnover_amount_count": 1,
            "collected_codes": [stock_rows[0]["prefixed_code"]],
            "missing_codes": [],
            "missing_count": 0,
            "failed_items": [],
            "failed_count": 0,
            "latest_source_date": target_date,
        }

    monkeypatch.setattr(stock, "collect_exchange_official_rows", fake_collect)
    db_tools = FakeDbTools()

    result = asyncio.run(
        stock.sync_exchange_official_daily(
            db_tools=db_tools,
            target_date="2026-04-30",
            request_interval_seconds=2,
        )
    )

    assert set(started) == {"SH", "SZ"}
    assert result["upserted"] == 2
    assert result["turnover_amount_count"] == 2
    assert len(db_tools.upserted_rows) == 2


def test_sync_exchange_official_daily_resumes_only_missing_codes(monkeypatch):
    class FakeDbTools:
        async def ensure_stock_exchange_official_daily_table(self):
            return None

        async def get_all_stock_info_rows(self):
            return [
                {"stock_code": "600000", "prefixed_code": "sh600000", "exchange": "SH", "security_type": "A"},
                {"stock_code": "600004", "prefixed_code": "sh600004", "exchange": "SH", "security_type": "A"},
                {"stock_code": "000001", "prefixed_code": "sz000001", "exchange": "SZ", "security_type": "A"},
            ]

        async def get_stock_exchange_official_daily_coverage_by_date(self, _trade_date, exchange):
            if exchange == "SH":
                return {"sh600000": True}
            return {"sz000001": True}

    collected = {}

    async def fake_collect(exchange, stock_rows, target_date, db_tools=None, request_interval_seconds=2.0):
        collected[exchange] = [row["prefixed_code"] for row in stock_rows]
        return {
            "exchange": exchange,
            "target_count": len(stock_rows),
            "row_count": len(stock_rows),
            "upserted_count": len(stock_rows),
            "turnover_amount_count": len(stock_rows),
            "collected_codes": collected[exchange],
            "missing_codes": [],
            "missing_count": 0,
            "failed_items": [],
            "failed_count": 0,
            "latest_source_date": target_date if stock_rows else None,
        }

    monkeypatch.setattr(stock, "collect_exchange_official_rows", fake_collect)

    result = asyncio.run(
        stock.sync_exchange_official_daily(
            db_tools=FakeDbTools(),
            target_date="2026-04-30",
            request_interval_seconds=2,
        )
    )

    assert collected == {"SH": ["sh600004"], "SZ": []}
    assert result["upserted"] == 1
    assert result["sh"]["row_count"] == 2
    assert result["sh"]["resumed_count"] == 1
    assert result["sh"]["pending_count"] == 1
    assert result["sz"]["row_count"] == 1
    assert result["sz"]["resumed_count"] == 1
    assert result["sz"]["pending_count"] == 0


def test_collect_exchange_official_rows_upserts_each_row_immediately(monkeypatch):
    class FakeDbTools:
        def __init__(self):
            self.upserted_batches = []

        async def upsert_stock_exchange_official_daily_data(self, rows):
            self.upserted_batches.append(list(rows))
            return len(rows)

    class FakeClient:
        def __init__(self, exchange, limiter=None):
            self.exchange = exchange

        def close(self):
            return None

    rows = [
        {"exchange": "SH", "stock_code": "600000", "prefixed_code": "sh600000", "trade_date": "2026-04-30"},
        {"exchange": "SH", "stock_code": "600004", "prefixed_code": "sh600004", "trade_date": "2026-04-30"},
    ]

    async def fake_fetch(_client, stock_row, target_date):
        return rows.pop(0), {"result": [{"TX_DATE": target_date}]}

    monkeypatch.setattr(stock, "OfficialExchangeHttpClient", FakeClient)
    monkeypatch.setattr(stock, "fetch_sse_official_daily_row", fake_fetch)

    db_tools = FakeDbTools()
    result = asyncio.run(
        stock.collect_exchange_official_rows(
            "SH",
            [
                {"stock_code": "600000", "prefixed_code": "sh600000"},
                {"stock_code": "600004", "prefixed_code": "sh600004"},
            ],
            "2026-04-30",
            db_tools=db_tools,
            request_interval_seconds=0,
        )
    )

    assert result["row_count"] == 2
    assert result["upserted_count"] == 2
    assert len(db_tools.upserted_batches) == 2
    assert db_tools.upserted_batches[0][0]["stock_code"] == "600000"
    assert db_tools.upserted_batches[1][0]["stock_code"] == "600004"


def test_without_proxy_env_temporarily_forces_direct_network(monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.local:8080")
    monkeypatch.setenv("https_proxy", "http://proxy.local:8080")
    monkeypatch.setenv("NO_PROXY", "localhost")

    with without_proxy_env():
        assert "HTTP_PROXY" not in os.environ
        assert "https_proxy" not in os.environ
        assert os.environ["NO_PROXY"] == "*"
        assert os.environ["no_proxy"] == "*"

    assert os.environ["HTTP_PROXY"] == "http://proxy.local:8080"
    assert os.environ["https_proxy"] == "http://proxy.local:8080"
    assert os.environ["NO_PROXY"] == "localhost"


def test_direct_network_env_strips_proxy_settings(monkeypatch):
    monkeypatch.setenv("ALL_PROXY", "socks5://proxy.local:1080")
    env = direct_network_env({"HTTP_PROXY": "http://proxy.local:8080", "KEEP_ME": "1"})

    assert "HTTP_PROXY" not in env
    assert "ALL_PROXY" not in env
    assert env["KEEP_ME"] == "1"
    assert env["NO_PROXY"] == "*"
