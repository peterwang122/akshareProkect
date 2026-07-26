import asyncio

from akshare_project.services import stock_temp_service


def test_hk_index_futures_route_accepts_target_date(monkeypatch):
    captured = {}

    async def _fake_sync_hk_index_futures_daily(**kwargs):
        captured.update(kwargs)
        return {
            "status": "SUCCESS",
            "target_date": "2026-07-23",
            "collection": 36,
            "available_products": ["HSI", "HHI", "HTI"],
            "expected_products": ["HSI", "HHI", "HTI"],
        }

    async def _fake_repair_market_previous_trade_day(market, reference_date=None):
        captured["refresh_market"] = market
        captured["refresh_reference_date"] = reference_date
        return 9

    monkeypatch.setattr(
        stock_temp_service.futures,
        "sync_hk_index_futures_daily",
        _fake_sync_hk_index_futures_daily,
    )
    monkeypatch.setattr(
        stock_temp_service.quant_index,
        "repair_market_previous_trade_day",
        _fake_repair_market_previous_trade_day,
    )

    route = stock_temp_service.build_daily_routes()["/collect-hk-index-futures-daily"]
    result = asyncio.run(route.handler(target_date="2026-07-23"))

    assert captured["trade_date"].isoformat() == "2026-07-23"
    assert captured["return_details"] is True
    assert captured["refresh_market"] == "hk"
    assert captured["refresh_reference_date"].isoformat() == "2026-07-24"
    assert result["status"] == "SUCCESS"
    assert result["quant_index_refresh"] == 9


def test_hk_index_futures_not_ready_skips_dashboard_refresh(monkeypatch):
    refreshed = False

    async def _fake_sync_hk_index_futures_daily(**_kwargs):
        return {
            "status": "SOURCE_NOT_READY",
            "target_date": "2026-07-23",
            "collection": 0,
            "available_products": [],
            "expected_products": ["HSI", "HHI", "HTI"],
        }

    async def _fake_repair_market_previous_trade_day(*_args, **_kwargs):
        nonlocal refreshed
        refreshed = True
        return 9

    monkeypatch.setattr(
        stock_temp_service.futures,
        "sync_hk_index_futures_daily",
        _fake_sync_hk_index_futures_daily,
    )
    monkeypatch.setattr(
        stock_temp_service.quant_index,
        "repair_market_previous_trade_day",
        _fake_repair_market_previous_trade_day,
    )

    result = asyncio.run(
        stock_temp_service.run_hk_index_futures_handler(target_date="2026-07-23")
    )

    assert result["status"] == "SOURCE_NOT_READY"
    assert result["quant_index_refresh"] == 0
    assert refreshed is False
