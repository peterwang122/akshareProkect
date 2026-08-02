from datetime import date, timedelta

from akshare_project.collectors import quant_index


def _fixture(day_count=320):
    dates = [(date(2024, 1, 1) + timedelta(days=index)).isoformat() for index in range(day_count)]
    close_map = {}
    futures_map = {}
    option_pc_map = {}
    option_flow_map = {}
    exchange_map = {}
    vix_map = {}
    for core_index, index_name in enumerate(quant_index.CORE_INDEX_NAMES):
        month_symbol = quant_index.INDEX_FUTURES_SYMBOLS[index_name]["month_symbol"]
        for index, trade_date in enumerate(dates):
            close = 1000.0 + core_index * 100.0 + index * 1.5 + (index % 9) * 0.2
            close_map[(trade_date, index_name)] = close
            futures_map[(trade_date, month_symbol)] = close * (1.002 + (index % 5) * 0.0001)
            option_pc_map[(trade_date, index_name)] = {"option_pc_current_month": 0.8 + (index % 17) * 0.01}
            option_flow_map[(trade_date, index_name)] = {
                "option_volume_pc_ratio": 0.9 + (index % 13) * 0.01,
                "option_turnover_pc_ratio": 0.85 + (index % 11) * 0.01,
            }
            if index_name == "中证500":
                exchange_map[(trade_date, index_name)] = {
                    "sse:510500": {
                        "option_pc_current_month": 0.75 + (index % 17) * 0.01,
                        "option_volume_pc_ratio": 0.88 + (index % 13) * 0.01,
                        "option_turnover_pc_ratio": 0.82 + (index % 11) * 0.01,
                    }
                }
            exchange, product = quant_index.OPTION_VIX_SOURCES_BY_INDEX[index_name][0]
            vix_map[(trade_date, index_name)] = {
                f"{exchange.lower()}:{product}": {
                    "vix_close": 18.0 + (index % 19) * 0.2,
                    "vix_term_structure": -2.0 + (index % 13) * 0.25,
                    "downside_skew_25d": 3.0 + (index % 17) * 0.2,
                }
            }
    return dates, close_map, futures_map, option_pc_map, option_flow_map, exchange_map, vix_map


def test_self_sentiment_is_bounded_and_aggregates_core_indices():
    fixture = _fixture()
    result = quant_index.build_self_sentiment_map(*fixture)
    latest_date = fixture[0][-1]

    for index_name in quant_index.CORE_INDEX_NAMES:
        payload = result[(latest_date, index_name)]
        assert 0 <= payload["self_sentiment_score"] <= 100
        assert 0 <= payload["self_sentiment_core_score"] <= 100
        assert 0 <= payload["self_sentiment_derivative_score"] <= 100
        assert payload["self_sentiment_components_json"]["component_count"] >= 7

    shanghai = result[(latest_date, "上证指数")]
    expected = sum(
        result[(latest_date, index_name)]["self_sentiment_score"]
        for index_name in quant_index.CORE_INDEX_NAMES
    ) / len(quant_index.CORE_INDEX_NAMES)
    assert shanghai["self_sentiment_score"] == expected


def test_self_sentiment_does_not_change_when_future_rows_are_appended():
    fixture = _fixture()
    full_result = quant_index.build_self_sentiment_map(*fixture)
    cutoff = 260
    partial_fixture = tuple(
        value[:cutoff] if isinstance(value, list) else {
            key: item for key, item in value.items() if key[0] in set(fixture[0][:cutoff])
        }
        for value in fixture
    )
    partial_result = quant_index.build_self_sentiment_map(*partial_fixture)
    compare_date = fixture[0][cutoff - 1]
    for index_name in quant_index.CORE_INDEX_NAMES:
        assert (
            full_result[(compare_date, index_name)]["self_sentiment_score"]
            == partial_result[(compare_date, index_name)]["self_sentiment_score"]
        )


def test_self_sentiment_keeps_core_score_before_derivative_data_exists():
    dates, close_map, futures_map, *_rest = _fixture()
    result = quant_index.build_self_sentiment_map(
        dates,
        close_map,
        futures_map,
        {},
        {},
        {},
        {},
    )
    payload = result[(dates[-1], "中证1000")]
    assert payload["self_sentiment_score"] is not None
    assert payload["self_sentiment_core_score"] is not None
    assert payload["self_sentiment_derivative_score"] is None


def test_self_sentiment_includes_30d_financing_net_buy_percentile():
    fixture = _fixture()
    dates = fixture[0]
    margin_sum_map = {
        trade_date: {"margin_financing_net_buy_sum_30d": float(index)}
        for index, trade_date in enumerate(dates)
    }

    result = quant_index.build_self_sentiment_map(
        *fixture,
        margin_financing_net_buy_sum_map=margin_sum_map,
    )
    payload = result[(dates[-1], "中证1000")]
    components = payload["self_sentiment_components_json"]

    assert components["version"] == "v4"
    assert components["raw_values"]["margin_financing_net_buy_30d"] == len(dates) - 1
    assert components["scores"]["margin_financing_net_buy_30d"] > 99
    assert components["component_count"] == 10


def test_self_sentiment_includes_vix_term_structure_and_downside_skew():
    fixture = _fixture()
    result = quant_index.build_self_sentiment_map(*fixture)
    payload = result[(fixture[0][-1], "中证1000")]
    components = payload["self_sentiment_components_json"]

    assert components["raw_values"]["vix_term_structure"] is not None
    assert components["raw_values"]["downside_skew_25d"] is not None
    assert components["scores"]["vix_term_structure"] is not None
    assert components["scores"]["downside_skew_25d"] is not None


def test_recompute_does_not_restore_invalid_current_raw_value_from_history():
    fixture = _fixture()
    latest_date = fixture[0][-1]
    index_name = "中证1000"
    vix_map = dict(fixture[6])
    vix_map.pop((latest_date, index_name))
    history_rows = [
        {
            "trade_date": latest_date,
            "index_name": index_name,
            "self_sentiment_components_json": {
                "raw_values": {
                    "vix_term_structure": -80.0,
                    "downside_skew_25d": -135.0,
                }
            },
        }
    ]

    result = quant_index.build_self_sentiment_map(
        fixture[0],
        fixture[1],
        fixture[2],
        fixture[3],
        fixture[4],
        fixture[5],
        vix_map,
        history_rows=history_rows,
        output_start_date=latest_date,
        output_end_date=latest_date,
    )
    raw_values = result[(latest_date, index_name)]["self_sentiment_components_json"]["raw_values"]

    assert raw_values["vix_term_structure"] is None
    assert raw_values["downside_skew_25d"] is None


def test_price_and_volume_put_call_do_not_affect_derivative_score():
    fixture = _fixture()
    baseline = quant_index.build_self_sentiment_map(*fixture)
    dates = fixture[0]
    latest_date = dates[-1]

    changed_price_map = {
        key: {**value, "option_pc_current_month": value["option_pc_current_month"] * 100}
        for key, value in fixture[3].items()
    }
    changed_flow_map = {
        key: {**value, "option_volume_pc_ratio": value["option_volume_pc_ratio"] * 100}
        for key, value in fixture[4].items()
    }
    changed_exchange_map = {
        key: {
            source_key: {
                **payload,
                "option_pc_current_month": payload["option_pc_current_month"] * 100,
                "option_volume_pc_ratio": payload["option_volume_pc_ratio"] * 100,
            }
            for source_key, payload in value.items()
        }
        for key, value in fixture[5].items()
    }
    changed = quant_index.build_self_sentiment_map(
        fixture[0],
        fixture[1],
        fixture[2],
        changed_price_map,
        changed_flow_map,
        changed_exchange_map,
        fixture[6],
    )

    for index_name in quant_index.CORE_INDEX_NAMES:
        assert (
            changed[(latest_date, index_name)]["self_sentiment_derivative_score"]
            == baseline[(latest_date, index_name)]["self_sentiment_derivative_score"]
        )
