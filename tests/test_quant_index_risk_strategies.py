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


def test_risk_date_alignment_uses_same_day_2230_cutoff_without_holiday_backfill():
    points = [
        {
            "source_date": "2025-04-02",
            "value": 10.0,
            "available_at": "2025-04-03T08:00:00+08:00",
        },
        {
            "source_date": "2025-04-03",
            "value": 20.0,
            "available_at": "2025-04-04T08:00:00+08:00",
        },
        {
            "source_date": "2025-04-04",
            "value": 30.0,
            "available_at": "2025-04-07T08:00:00+08:00",
        },
    ]

    aligned = quant_index.align_metric_points_to_cn_risk_dates(
        points,
        ["2025-04-03", "2025-04-07"],
    )

    assert aligned["2025-04-03"]["source_date"] == "2025-04-02"
    assert aligned["2025-04-03"]["value"] == 10.0
    assert aligned["2025-04-07"]["source_date"] == "2025-04-04"


def test_global_leading_state_holds_retriggers_and_clears_on_domestic_release():
    calendar = [
        "2025-03-31", "2025-04-01", "2025-04-02", "2025-04-03",
        "2025-04-07", "2025-04-08", "2025-04-09", "2025-04-10", "2025-04-11",
    ]
    state, first = quant_index.advance_global_leading_state(
        {},
        trade_date="2025-03-31",
        cn_calendar_dates=calendar,
        raw_active=True,
        raw_modes=["volatility_repricing"],
        domestic_vulnerability_score=60.0,
    )
    assert first["active"] is True
    assert first["triggered_today"] is True
    assert first["valid_through"] == "2025-04-08"

    state, missing = quant_index.advance_global_leading_state(
        state,
        trade_date="2025-04-01",
        cn_calendar_dates=calendar,
        raw_active=None,
        raw_modes=[],
        domestic_vulnerability_score=None,
    )
    assert missing["active"] is True

    state, retriggered = quant_index.advance_global_leading_state(
        state,
        trade_date="2025-04-03",
        cn_calendar_dates=calendar,
        raw_active=True,
        raw_modes=["asia_em_transmission"],
        domestic_vulnerability_score=65.0,
    )
    assert retriggered["triggered_today"] is False
    assert retriggered["trigger_date"] == "2025-03-31"
    assert retriggered["last_trigger_date"] == "2025-04-03"
    assert retriggered["valid_through"] == "2025-04-11"
    assert retriggered["mode"] == "volatility_repricing+asia_em_transmission"

    _state, released = quant_index.advance_global_leading_state(
        state,
        trade_date="2025-04-07",
        cn_calendar_dates=calendar,
        raw_active=None,
        raw_modes=[],
        domestic_vulnerability_score=39.9,
    )
    assert released["active"] is False
    assert released["trigger_date"] is None


def test_hs300_global_leading_state_uses_40_30_gate_and_preserves_missing_days():
    calendar = [
        "2026-06-24", "2026-06-25", "2026-06-26", "2026-06-29",
        "2026-06-30", "2026-07-01", "2026-07-02",
    ]
    state, triggered = quant_index.advance_global_leading_state(
        {},
        trade_date="2026-06-24",
        cn_calendar_dates=calendar,
        raw_active=True,
        raw_modes=["asia_em_transmission"],
        domestic_vulnerability_score=40.0,
        trigger_threshold=40.0,
        release_threshold=30.0,
        missing_gate_is_incomplete=True,
    )
    assert triggered["active"] is True
    assert triggered["valid_through"] == "2026-07-01"

    state, missing = quant_index.advance_global_leading_state(
        state,
        trade_date="2026-06-25",
        cn_calendar_dates=calendar,
        raw_active=None,
        raw_modes=[],
        domestic_vulnerability_score=None,
        trigger_threshold=40.0,
        release_threshold=30.0,
        missing_gate_is_incomplete=True,
    )
    assert missing["active"] is True

    state, held = quant_index.advance_global_leading_state(
        state,
        trade_date="2026-06-26",
        cn_calendar_dates=calendar,
        raw_active=False,
        raw_modes=[],
        domestic_vulnerability_score=35.0,
        trigger_threshold=40.0,
        release_threshold=30.0,
        missing_gate_is_incomplete=True,
    )
    assert held["active"] is True

    _state, released = quant_index.advance_global_leading_state(
        state,
        trade_date="2026-06-29",
        cn_calendar_dates=calendar,
        raw_active=False,
        raw_modes=[],
        domestic_vulnerability_score=29.9,
        trigger_threshold=40.0,
        release_threshold=30.0,
        missing_gate_is_incomplete=True,
    )
    assert released["active"] is False


def test_hs300_global_leading_is_incomplete_when_raw_triggers_without_gate_data():
    _state, status = quant_index.advance_global_leading_state(
        {},
        trade_date="2025-03-31",
        cn_calendar_dates=["2025-03-31", "2025-04-01"],
        raw_active=True,
        raw_modes=["volatility_repricing"],
        domestic_vulnerability_score=None,
        trigger_threshold=40.0,
        release_threshold=30.0,
        missing_gate_is_incomplete=True,
    )

    assert status["active"] is None
    assert status["trigger_date"] is None


def test_raw_global_leading_is_preserved_when_domestic_gate_is_not_met(monkeypatch):
    trade_dates = _dates(300)
    final_date = trade_dates[-1]
    monkeypatch.setattr(
        quant_index,
        "build_volatility_repricing_route",
        lambda *_args: {"active": True, "score": 100.0},
    )
    monkeypatch.setattr(
        quant_index,
        "build_asia_em_transmission_route",
        lambda *_args: {"active": False, "score": 0.0},
    )

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
        cn_calendar_dates=[*trade_dates, "2024-01-01"],
        output_start_date=final_date,
        output_end_date=final_date,
    )[final_date]

    assert result["risk_global_raw_leading"] == 1
    assert result["risk_global_raw_leading_mode"] == "volatility_repricing"
    assert result["risk_global_leading"] == 0
    leading = result["risk_strategy_components_json"]["global"]["leading"]
    assert leading["raw_active"] is True
    assert leading["raw_mode"] == "volatility_repricing"
    assert leading["active"] is False


def test_asia_em_route_allows_strong_regional_volatility_to_lead_third_market():
    regional = quant_index.risk_condition(
        3,
        99,
        direction="high",
        absolute_threshold=2,
        percentile_threshold=80,
    )
    broad = quant_index.risk_condition(
        2,
        88,
        direction="high",
        absolute_threshold=3,
        percentile_threshold=80,
    )

    route = quant_index.build_asia_em_transmission_route(regional, broad)

    assert route["score"] == 75
    assert route["strict_active"] is False
    assert route["active"] is True
    assert route["activation_basis"] == "regional_volatility_lead"


def test_asia_em_route_does_not_activate_with_only_two_regional_hits():
    regional = quant_index.risk_condition(
        2,
        98,
        direction="high",
        absolute_threshold=2,
        percentile_threshold=80,
    )
    broad = quant_index.risk_condition(
        2,
        88,
        direction="high",
        absolute_threshold=3,
        percentile_threshold=80,
    )

    route = quant_index.build_asia_em_transmission_route(regional, broad)

    assert route["score"] == 75
    assert route["active"] is False
    assert route["activation_basis"] is None


def test_volatility_repricing_route_activates_at_graded_score_75_or_higher():
    term_structure = quant_index.risk_condition(
        1.03,
        91,
        direction="high",
        absolute_threshold=1.0,
        percentile_threshold=80,
    )
    vvix = quant_index.risk_condition(
        4,
        85,
        direction="high",
        absolute_threshold=10,
        percentile_threshold=80,
    )
    pressure = quant_index.risk_condition(
        2,
        88,
        direction="high",
        absolute_threshold=2,
        percentile_threshold=80,
    )

    route = quant_index.build_volatility_repricing_route(
        term_structure,
        vvix,
        pressure,
    )

    assert route["score"] > 75
    assert route["strict_active"] is False
    assert route["active"] is True
    assert route["activation_basis"] == "graded_repricing"


def test_volatility_repricing_route_stays_off_below_graded_threshold():
    term_structure = quant_index.risk_condition(
        0.95,
        70,
        direction="high",
        absolute_threshold=1.0,
        percentile_threshold=80,
    )
    vvix = quant_index.risk_condition(
        4,
        85,
        direction="high",
        absolute_threshold=10,
        percentile_threshold=80,
    )
    pressure = quant_index.risk_condition(
        2,
        88,
        direction="high",
        absolute_threshold=2,
        percentile_threshold=80,
    )

    route = quant_index.build_volatility_repricing_route(
        term_structure,
        vvix,
        pressure,
    )

    assert route["score"] < 75
    assert route["active"] is False
    assert route["activation_basis"] is None


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


def test_risk_condition_scores_partial_confirmation_as_half():
    result = quant_index.risk_condition(
        2.0,
        50.0,
        direction="high",
        absolute_threshold=1.6,
        percentile_threshold=80.0,
    )

    assert result["absolute_matched"] is True
    assert result["percentile_matched"] is False
    assert result["matched"] is False
    assert result["score"] == 50.0


def test_weighted_condition_score_keeps_missing_group_incomplete():
    complete = {"score": 100.0}
    missing = {"score": None}

    assert quant_index.weighted_condition_score([(complete, 0.5), (missing, 0.5)]) is None


def test_unified_risk_color_uses_overall_score_boundaries_only():
    assert quant_index.classify_overall_risk_score(None) is None
    assert quant_index.classify_overall_risk_score(39.999) == "stable"
    assert quant_index.classify_overall_risk_score(40.0) == "yellow"
    assert quant_index.classify_overall_risk_score(49.999) == "yellow"
    assert quant_index.classify_overall_risk_score(50.0) == "yellow"
    assert quant_index.classify_overall_risk_score(50.001) == "red"
    assert quant_index.classify_overall_risk_score(63.541667) == "red"


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


def _hs300_flat_inputs(trade_dates):
    final_date = trade_dates[-1]
    index_close_map = {
        (trade_date, "沪深300"): 1000.0 for trade_date in trade_dates
    }
    margin_map = {}
    cffex_map = {}
    futures_rows = []
    concentration_rows = []
    macro_rows = []
    qvix_rows = []
    for index, trade_date in enumerate(trade_dates):
        is_final = trade_date == final_date
        margin_map[trade_date] = {
            "margin_financing_net_buy_sum_30d": 50_000_000_000
            if is_final else 1_000_000_000 + index * 1_000_000,
        }
        cffex_map[(trade_date, "沪深300")] = {
            "cffex_citic_net_short_delta_14d": 3000 if is_final else index,
        }
        futures_rows.append({
            "trade_date": trade_date,
            "symbol": "IF2509",
            "close_price": 990.0 if is_final else 1000.0,
            "open_interest": 1000,
            "volume": 100,
            "data_source": "test_if",
        })
        concentration_rows.append({
            "trade_date": trade_date,
            "top5_pct": 50.0 if is_final else 30.0 + index / 100.0,
            "top5_data_source": "official_stock_turnover",
        })
        macro_rows.append({
            "trade_date": trade_date,
            "hs300_equity_bond_spread_pp": 3.0 if is_final else 5.0 + index / 1000.0,
            "data_source": "cn_macro_indicator_daily",
        })
        qvix_rows.append({
            "trade_date": trade_date,
            "close_price": 25.0 if is_final else 15.0 + index / 100.0,
            "data_source": "index_option_300etf_qvix",
        })
    grouped_map = {
        final_date: {
            "risk_strategy_components_json": {
                "global": {
                    "modules": {},
                    "confirmation": {
                        "active": False,
                        "score": 50.0,
                        "mode": None,
                        "active_modules": [],
                    },
                    "leading": {
                        "raw_active": True,
                        "raw_mode": "volatility_repricing",
                        "score": 100.0,
                        "active": True,
                    },
                }
            }
        }
    }
    return {
        "trade_dates": trade_dates,
        "index_close_map": index_close_map,
        "cffex_net_short_delta_map": cffex_map,
        "margin_financing_net_buy_sum_map": margin_map,
        "if_futures_rows": futures_rows,
        "turnover_concentration_rows": concentration_rows,
        "macro_indicator_rows": macro_rows,
        "qvix_rows": qvix_rows,
        "grouped_risk_map": grouped_map,
        "cn_calendar_dates": [
            *trade_dates,
            *[
                (date.fromisoformat(final_date) + timedelta(days=offset)).isoformat()
                for offset in range(1, 7)
            ],
        ],
        "output_start_date": final_date,
        "output_end_date": final_date,
    }


def test_hs300_flat_risk_uses_seven_unique_factors_and_confirmation_once():
    trade_dates = _dates(300)
    final_date = trade_dates[-1]

    result = quant_index.build_hs300_flat_risk_map(
        **_hs300_flat_inputs(trade_dates)
    )[final_date]
    components = result["risk_strategy_components_json"]
    factors = components["factors"]

    assert components["scoring_mode"] == "flat"
    assert len(factors) == 7
    assert len({factor["key"] for factor in factors}) == 7
    assert sum(factor["weight"] for factor in factors) == pytest.approx(1.0)
    assert result["risk_yellow_vulnerability_score"] is None
    assert result["risk_red_escalation_score"] is None
    assert result["risk_global_raw_leading"] == 1
    assert result["risk_global_leading"] == 1
    assert result["risk_global_score"] == 50.0
    assert result["risk_overall_score"] == pytest.approx(87.5)
    assert result["risk_display_state"] == "red"
    assert components["model_version"] == "hs300-flat-v3"
    assert components["factor_weights"] == {
        "hs300_equity_bond_spread": 0.15,
        "margin_financing_net_buy_30d": 0.15,
        "if_basis_change_5d": 0.15,
        "citic_if_net_short_change_14d": 0.20,
        "turnover_concentration_top5_ma5": 0.05,
        "qvix_300etf_close": 0.05,
        "global_confirmation": 0.25,
    }
    assert components["leading_gate_weights"] == {
        "hs300_equity_bond_spread": 0.15,
        "margin_financing_net_buy_30d": 0.15,
        "if_basis_change_5d": 0.15,
        "citic_if_net_short_change_14d": 0.10,
        "turnover_concentration_top5_ma5": 0.10,
        "qvix_300etf_close": 0.05,
    }
    leading = components["global"]["leading"]
    assert leading["domestic_gate_score"] == pytest.approx(100.0)
    assert leading["trigger_threshold"] == 40.0
    assert leading["release_threshold"] == 30.0
    assert leading["affects_total_score"] is False
    assert leading["affects_display_state"] is False


def test_hs300_score_weights_do_not_change_effective_leading_gate_weights():
    trade_dates = _dates(300)
    final_date = trade_dates[-1]
    inputs = _hs300_flat_inputs(trade_dates)
    inputs["turnover_concentration_rows"][-1]["top5_pct"] = 30.0

    result = quant_index.build_hs300_flat_risk_map(**inputs)[final_date]
    components = result["risk_strategy_components_json"]
    factors = {factor["key"]: factor for factor in components["factors"]}

    assert factors["citic_if_net_short_change_14d"]["contribution"] == pytest.approx(20.0)
    assert factors["turnover_concentration_top5_ma5"]["contribution"] == pytest.approx(0.0)
    assert result["risk_overall_score"] == pytest.approx(82.5)
    assert components["global"]["leading"]["domestic_gate_score"] == pytest.approx(
        60.0 / 0.70
    )


def test_hs300_flat_risk_keeps_total_incomplete_when_one_required_factor_is_missing():
    trade_dates = _dates(300)
    final_date = trade_dates[-1]
    inputs = _hs300_flat_inputs(trade_dates)
    inputs["qvix_rows"] = [
        row for row in inputs["qvix_rows"] if row["trade_date"] != final_date
    ]

    result = quant_index.build_hs300_flat_risk_map(**inputs)[final_date]

    assert result["risk_overall_score"] is None
    assert result["risk_base_state"] is None
    assert result["risk_display_state"] == "incomplete"
    assert result["risk_global_leading"] is None
    leading = result["risk_strategy_components_json"]["global"]["leading"]
    assert leading["domestic_gate_score"] is None


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
