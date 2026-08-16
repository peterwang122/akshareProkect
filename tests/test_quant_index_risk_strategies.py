from datetime import date, timedelta

import pytest

from akshare_project.collectors import quant_index


def _dates(count):
    start = date(2023, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def test_strict_prior_percentile_excludes_current_and_requires_252_prior_samples():
    values = list(range(252)) + [125.5]

    assert quant_index.strict_prior_percentile(values, 251) is None
    percentile = quant_index.strict_prior_percentile(values, 252)
    assert percentile is not None
    assert 49 < percentile < 51


def test_combined_metric_points_preserves_missing_source_values():
    first = [
        {"source_date": "2026-01-01", "value": None, "data_source": "first"},
        {"source_date": "2026-01-02", "value": 2.0, "data_source": "first"},
    ]
    second = [
        {"source_date": "2026-01-01", "value": None, "data_source": "second"},
        {"source_date": "2026-01-02", "value": 4.0, "data_source": "second"},
    ]

    result = quant_index._combined_metric_points(
        first, second, lambda left, right: (left + right) / 2.0
    )

    assert result[0]["value"] is None
    assert result[1]["value"] == pytest.approx(3.0)


def test_risk_condition_can_require_strict_absolute_threshold():
    result = quant_index.risk_condition(
        0.0,
        100.0,
        direction="high",
        absolute_threshold=0.0,
        percentile_threshold=80.0,
        absolute_inclusive=False,
    )

    assert result["matched"] is False
    assert result["direction"] == "high"


def test_dominant_im_basis_uses_highest_open_interest_numeric_contract():
    trade_dates = _dates(40)
    target_date = trade_dates[-1]
    index_close_map = {
        (trade_date, "中证1000"): 1000.0 for trade_date in trade_dates
    }
    rows = [
        {
            "trade_date": target_date,
            "symbol": "IM2508",
            "close_price": 990.0,
            "open_interest": 100,
            "data_source": "test",
        },
        {
            "trade_date": target_date,
            "symbol": "IM2509",
            "close_price": 980.0,
            "open_interest": 200,
            "data_source": "test",
        },
    ]

    metrics = quant_index.build_dominant_im_basis_metrics(
        trade_dates, index_close_map, rows
    )

    assert metrics["level"][target_date]["contract"] == "IM2509"
    assert metrics["level"][target_date]["value"] == pytest.approx(-200.0)


def test_domestic_yellow_and_red_risk_states_match_all_hybrid_thresholds():
    trade_dates = _dates(400)
    final_date = trade_dates[-1]
    index_close_map = {
        (trade_date, "中证1000"): 1000.0 for trade_date in trade_dates
    }
    option_pc_map = {}
    cffex_map = {}
    margin_map = {}
    futures_rows = []
    for index, trade_date in enumerate(trade_dates):
        pc_value = 2.0 if trade_date == final_date else 1.0 + index / 10000
        option_pc_map[(trade_date, "中证1000")] = {
            "option_pc_current_month": pc_value,
            "option_pc_next_month": pc_value,
            "option_pc_quarter_1": pc_value,
            "option_pc_quarter_2": pc_value,
        }
        cffex_map[(trade_date, "中证1000")] = {
            "cffex_citic_net_short_delta_14d": (
                5000 if trade_date == final_date else index
            )
        }
        margin_map[trade_date] = {
            "margin_financing_net_buy_sum_120d": (
                10_000_000_000 + index * 1_000_000
                if trade_date != final_date
                else 50_000_000_000
            ),
            "margin_financing_net_buy_sum_5d": (
                index * 1_000_000 if trade_date != final_date else -7_000_000_000
            ),
        }
        futures_rows.append({
            "trade_date": trade_date,
            "symbol": "IM2509",
            "close_price": 980.0 if trade_date == final_date else 1000.0,
            "open_interest": 1000,
            "volume": 100,
            "data_source": "test",
        })

    result = quant_index.build_risk_strategy_map(
        trade_dates=trade_dates,
        index_close_map=index_close_map,
        option_pc_map=option_pc_map,
        cffex_net_short_delta_map=cffex_map,
        margin_financing_net_buy_sum_map=margin_map,
        im_futures_rows=futures_rows,
        global_asset_rows=[],
        us_index_rows=[],
        hk_index_rows=[],
        us_vix_rows=[],
        us_credit_rows=[],
        turnover_concentration_rows=[],
        output_start_date=final_date,
        output_end_date=final_date,
    )[final_date]

    assert result["risk_yellow_vulnerability"] == 1
    assert result["risk_red_escalation"] == 1
    assert result["risk_global_shock"] is None
    assert result["risk_strategy_components_json"]["yellow"]["dominant_im_contract"] == "IM2509"


def test_missing_history_is_incomplete_instead_of_not_matched():
    trade_dates = _dates(100)
    result = quant_index.build_risk_strategy_map(
        trade_dates=trade_dates,
        index_close_map={},
        option_pc_map={},
        cffex_net_short_delta_map={},
        margin_financing_net_buy_sum_map={},
        im_futures_rows=[],
        global_asset_rows=[],
        us_index_rows=[],
        hk_index_rows=[],
        us_vix_rows=[],
        us_credit_rows=[],
        turnover_concentration_rows=[],
        output_start_date=trade_dates[-1],
        output_end_date=trade_dates[-1],
    )[trade_dates[-1]]

    assert result["risk_yellow_vulnerability"] is None
    assert result["risk_red_escalation"] is None
    assert result["risk_global_shock"] is None
