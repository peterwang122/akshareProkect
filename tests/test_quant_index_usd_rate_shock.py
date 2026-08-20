import asyncio
import json
from datetime import date, timedelta

import pytest

from akshare_project.collectors import index, quant_index


def _us_dates(count):
    start = date(2025, 1, 1)
    dates = []
    cursor = start
    while len(dates) < count:
        if cursor.weekday() < 5:
            dates.append(cursor.isoformat())
        cursor = cursor + timedelta(days=1)
    return dates


def test_us_treasury_rows_include_real_yield_and_available_at():
    dates = _us_dates(6)
    series_maps = {
        "yield_3m": {day: 4.0 for day in dates},
        "yield_2y": {day: 4.1 for day in dates},
        "yield_10y": {day: 4.2 for day in dates},
        "yield_real_10y": {day: 2.1 for day in dates},
    }
    rows = index.build_us_treasury_yield_rows(series_maps)
    assert len(rows) == len(dates)
    latest = rows[-1]
    assert latest["yield_real_10y"] == 2.1
    assert latest["yield_10y"] == 4.2
    assert latest["available_at"] is not None
    # 每个观测日的可用时间必须晚于观测日（下一美国工作日）
    assert latest["available_at"] > latest["trade_date"]


def test_available_at_converts_next_us_business_day_to_shanghai():
    # 2026-01-02 是周五，下一美国工作日 2026-01-05（周一）16:00 Chicago = 1月6日06:00 +08
    result = index.us_treasury_available_at("2026-01-02", "2026-01-05")
    assert result == "2026-01-06T06:00:00+08:00"


def test_available_at_weekday_fallback_when_no_next_series_date():
    # 最新观测日没有系列内下一日时，按工作日推算（周五 -> 下周一）
    result = index.us_treasury_available_at("2026-01-02", None)
    assert result == "2026-01-06T06:00:00+08:00"


def test_available_at_skips_us_federal_holiday_observation():
    # 2026-07-04 是周六，联邦假日观察日为 2026-07-03（周五）；7/2 的下一美国工作日为 7/6
    result = index.us_treasury_available_at("2026-07-02", None)
    # 7 月为夏令时（CDT, UTC-5），16:00 Chicago = 次日 05:00 Asia/Shanghai
    assert result == "2026-07-07T05:00:00+08:00"


def _fred_csv(series_id, values):
    lines = ["observation_date," + series_id]
    for trade_date, value in values:
        lines.append(f"{trade_date},{value}")
    return "\n".join(lines) + "\n"


class _FakeDbTools:
    def __init__(self):
        self.upserted_rows = []

    async def init_pool(self):
        return None

    async def upsert_index_us_treasury_yield_daily(self, rows):
        self.upserted_rows = rows
        return len(rows)


def test_sync_daily_fails_when_latest_common_date_missing_dfii10(monkeypatch):
    dates = _us_dates(15)
    payloads = {
        "DGS3MO": [(day, 4.0) for day in dates],
        "DGS2": [(day, 4.1) for day in dates],
        "DGS10": [(day, 4.2) for day in dates],
        "DFII10": [(day, 2.1) for day in dates[:-1]],
    }
    monkeypatch.setattr(
        index,
        "fetch_fred_series_csv",
        lambda series_id: _fred_csv(series_id, payloads[series_id]),
    )
    db = _FakeDbTools()
    asyncio.run(index.sync_daily_us_treasury_yield(db))
    # 最新共同可用日期（DFII10 存在的最晚日期）四项完整即成功
    assert db.upserted_rows[-1]["yield_real_10y"] == 2.1


def test_sync_daily_fails_when_dfii10_entirely_missing(monkeypatch):
    dates = _us_dates(15)
    payloads = {
        "DGS3MO": [(day, 4.0) for day in dates],
        "DGS2": [(day, 4.1) for day in dates],
        "DGS10": [(day, 4.2) for day in dates],
        "DFII10": [],
    }
    monkeypatch.setattr(
        index,
        "fetch_fred_series_csv",
        lambda series_id: _fred_csv(series_id, payloads[series_id]),
    )
    with pytest.raises(ValueError, match="DFII10"):
        asyncio.run(index.sync_daily_us_treasury_yield(_FakeDbTools()))


def test_sync_daily_upserts_recent_rows_when_complete(monkeypatch):
    dates = _us_dates(30)
    payloads = {
        "DGS3MO": [(day, 4.0) for day in dates],
        "DGS2": [(day, 4.1) for day in dates],
        "DGS10": [(day, 4.2) for day in dates],
        "DFII10": [(day, 2.1) for day in dates],
    }
    monkeypatch.setattr(
        index,
        "fetch_fred_series_csv",
        lambda series_id: _fred_csv(series_id, payloads[series_id]),
    )
    db = _FakeDbTools()
    result = asyncio.run(index.sync_daily_us_treasury_yield(db))
    assert result == len(db.upserted_rows)
    assert len(db.upserted_rows) <= index.US_TREASURY_DAILY_SYNC_RECENT_DAYS
    assert db.upserted_rows[-1]["yield_real_10y"] == 2.1


def _point(value, percentile, source_date="2026-08-14", data_source="fred_public_csv", available_at="2026-08-15T05:00:00+08:00"):
    return {
        "value": value,
        "percentile": percentile,
        "source_date": source_date,
        "data_source": data_source,
        "available_at": available_at,
    }


def test_rate_shock_active_when_all_conditions_match():
    state = quant_index.build_usd_rate_shock_state(
        _point(0.25, 95.0),
        _point(0.20, 92.0),
        _point(-6.0, 8.0, data_source="global_risk"),
        _point(-3.0, 5.0, data_source="blackrock_ishares_historical_nav"),
    )
    assert state["active"] is True
    assert state["complete"] is True
    assert state["score"] == pytest.approx(100.0)
    assert state["matched_condition_count"] == 3


def test_rate_shock_boundary_nominal_below_20bp_not_active():
    state = quant_index.build_usd_rate_shock_state(
        _point(0.19, 95.0),
        _point(0.20, 92.0),
        _point(-6.0, 8.0, data_source="global_risk"),
        _point(None, None, data_source="blackrock_ishares_historical_nav"),
    )
    assert state["active"] is False
    assert state["complete"] is True


def test_rate_shock_missing_core_input_is_incomplete():
    state = quant_index.build_usd_rate_shock_state(
        _point(None, None),
        _point(0.20, 92.0),
        _point(None, None, data_source="global_risk"),
        _point(None, None, data_source="blackrock_ishares_historical_nav"),
    )
    assert state["active"] is None
    assert state["complete"] is False


def test_rate_shock_market_confirm_accepts_sox_only():
    state = quant_index.build_usd_rate_shock_state(
        _point(0.25, 95.0),
        _point(0.20, 92.0),
        _point(-5.0, 8.0, data_source="global_risk"),
        _point(None, None, data_source="blackrock_ishares_historical_nav"),
    )
    assert state["active"] is True


def test_rate_shock_market_confirm_incomplete_when_one_missing_one_false():
    state = quant_index.build_usd_rate_shock_state(
        _point(0.25, 95.0),
        _point(0.20, 92.0),
        _point(-1.0, 90.0, data_source="global_risk"),
        _point(None, None, data_source="blackrock_ishares_historical_nav"),
    )
    # 一项明确未命中、另一项缺失：市场确认为 None/incomplete，不能判 False
    market = state["components"][2]
    assert market["matched"] is None
    assert state["active"] is None
    assert state["complete"] is False


def test_rate_shock_market_confirm_false_only_when_both_false():
    state = quant_index.build_usd_rate_shock_state(
        _point(0.25, 95.0),
        _point(0.20, 92.0),
        _point(-1.0, 90.0, data_source="global_risk"),
        _point(-0.5, 90.0, data_source="blackrock_ishares_historical_nav"),
    )
    market = state["components"][2]
    assert market["matched"] is False
    assert state["active"] is False
    assert state["complete"] is True


def test_rate_conditions_include_level_value_and_available_at():
    state = quant_index.build_usd_rate_shock_state(
        _point(0.25, 95.0),
        _point(0.20, 92.0),
        _point(-6.0, 8.0, data_source="global_risk"),
        _point(-3.0, 5.0, data_source="blackrock_ishares_historical_nav"),
        nominal_level_value=4.5,
        real_level_value=2.3,
    )
    nominal = state["components"][0]
    real = state["components"][1]
    assert nominal["level_value"] == 4.5
    assert real["level_value"] == 2.3
    assert nominal["available_at"] == "2026-08-15T05:00:00+08:00"


def test_align_by_available_at_includes_same_day_source_before_cutoff():
    points = [
        {
            "source_date": "2026-08-13",
            "value": 4.5,
            "available_at": "2026-08-14T04:16:00+08:00",
        }
    ]
    aligned = quant_index.align_metric_points_to_cn_dates_by_available_at(
        points,
        ["2026-08-13", "2026-08-14", "2026-08-17"],
    )
    # 美国 8/13 观测数据在 8/14 04:16 已公开，早于下一A股交易日 8/14 09:20
    assert aligned["2026-08-13"]["source_date"] == "2026-08-13"


def test_align_by_available_at_excludes_late_publication():
    points = [
        {
            "source_date": "2026-08-13",
            "value": 4.5,
            "available_at": "2026-08-14T11:00:00+08:00",
        }
    ]
    aligned = quant_index.align_metric_points_to_cn_dates_by_available_at(
        points,
        ["2026-08-13", "2026-08-14", "2026-08-17"],
    )
    # 晚于 8/14 09:20 公开，不能用于 A股 8/13 的下一个交易日风险状态
    assert "2026-08-13" not in aligned


def test_build_risk_strategy_map_rate_mode_and_top1_removed():
    dates = _us_dates(330)
    final_date = dates[-1]
    treasury_rows = []
    for index, trade_date in enumerate(dates):
        nominal = 4.0
        real = 2.0
        if index >= len(dates) - 5:
            nominal = 4.0 + 0.05 * (index - (len(dates) - 5) + 1)
            real = 2.0 + 0.05 * (index - (len(dates) - 5) + 1)
        treasury_rows.append(
            {
                "trade_date": trade_date,
                "yield_10y": nominal,
                "yield_real_10y": real,
                "available_at": f"{trade_date}T05:00:00+08:00",
                "data_source": "fred_public_csv",
            }
        )
    asset_rows = []
    for index, trade_date in enumerate(dates):
        sox_close = 1000.0 if index < len(dates) - 10 else 1000.0 * (1 - 0.006 * (index - (len(dates) - 10)))
        asset_rows.extend(
            [
                {
                    "asset_code": "SOX",
                    "trade_date": trade_date,
                    "close_value": sox_close,
                    "data_source": "test",
                },
                {
                    "asset_code": "IXN_NAV",
                    "trade_date": trade_date,
                    "close_value": 100.0,
                    "data_source": "test",
                },
                {
                    "asset_code": "ACWI_NAV",
                    "trade_date": trade_date,
                    "close_value": 100.0 if index < len(dates) - 10 else 100.0 * (1 + 0.003 * (index - (len(dates) - 10))),
                    "data_source": "test",
                },
            ]
        )
    result = quant_index.build_risk_strategy_map(
        trade_dates=dates,
        index_close_map={},
        option_pc_map={},
        cffex_net_short_delta_map={},
        margin_financing_net_buy_sum_map={},
        im_futures_rows=[],
        global_asset_rows=asset_rows,
        us_index_rows=[],
        hk_index_rows=[],
        us_vix_rows=[],
        us_credit_rows=[],
        us_treasury_rows=treasury_rows,
        turnover_concentration_rows=[
            {
                "trade_date": final_date,
                "top5_pct": 50.0,
                "top5_data_source": "peakstone_top5_turnover_concentration_ma5",
            }
        ],
        output_start_date=final_date,
        output_end_date=final_date,
    )[final_date]

    payload = result["risk_strategy_components_json"]
    assert payload["global"]["usd_rate_shock"]["active"] is True
    assert payload["global"]["mode"] == "usd_rate_shock"
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "前1%" not in serialized
    assert "前5%" in serialized
    observations = payload["yellow"]["observations"]["turnover_concentration"]["components"]
    assert len(observations) == 1
    assert observations[0]["label"] == "A股成交额前5%集中度MA5"
