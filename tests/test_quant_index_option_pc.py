import pytest

from akshare_project.collectors.quant_index import (
    build_exchange_option_pc_map,
    build_index_option_flow_pc_map,
    build_index_option_pc_map,
    build_option_flow_pc_payload_for_product,
    build_option_pc_payload_for_product,
    interpolate_option_price,
    select_option_pc_contract_months,
    third_friday_of_contract_month,
)


def _option_row(
    trade_date,
    product_prefix,
    contract_month,
    option_type,
    strike_price,
    close_price,
    volume=None,
    turnover=None,
):
    return {
        "trade_date": trade_date,
        "product_prefix": product_prefix,
        "contract_month": contract_month,
        "option_type": option_type,
        "strike_price": strike_price,
        "close_price": close_price,
        "volume": volume,
        "turnover": turnover,
    }


def test_put_and_call_interpolate_with_signed_slope():
    put_price = interpolate_option_price({8100: 23, 8200: 44}, 8132)
    call_price = interpolate_option_price({8100: 44, 8200: 23}, 8132)

    assert put_price == pytest.approx(29.72)
    assert call_price == pytest.approx(37.28)


def test_interpolation_uses_exact_strike_and_does_not_extrapolate():
    assert interpolate_option_price({8100: 23, 8200: 44}, 8200) == pytest.approx(44)
    assert interpolate_option_price({8100: 23, 8200: 44}, 8300) is None
    assert interpolate_option_price({8100: 23}, 8132) is None


def test_contract_month_selection_uses_current_next_and_two_quarters():
    selected = select_option_pc_contract_months(["2605", "2606", "2607", "2609", "2612", "2703"])

    assert selected == {
        "current_month": "2605",
        "next_month": "2606",
        "quarter_1": "2609",
        "quarter_2": "2612",
    }


def test_contract_expiry_uses_third_friday():
    assert third_friday_of_contract_month("2604").isoformat() == "2026-04-17"
    assert third_friday_of_contract_month("2605").isoformat() == "2026-05-15"


def test_contract_month_selection_keeps_expiring_month_before_expiry():
    selected = select_option_pc_contract_months(
        ["2604", "2605", "2606", "2609", "2612", "2703"],
        trade_date="2026-04-16",
    )

    assert selected == {
        "current_month": "2604",
        "next_month": "2605",
        "quarter_1": "2606",
        "quarter_2": "2609",
    }


def test_contract_month_selection_skips_expiring_month_on_expiry_day():
    selected = select_option_pc_contract_months(
        ["2604", "2605", "2606", "2609", "2612", "2703"],
        trade_date="2026-04-17",
    )

    assert selected == {
        "current_month": "2605",
        "next_month": "2606",
        "quarter_1": "2609",
        "quarter_2": "2612",
    }


def test_product_payload_returns_null_when_call_is_zero():
    rows = [
        _option_row("2026-05-08", "MO", "2605", "PUT", 8100, 23),
        _option_row("2026-05-08", "MO", "2605", "PUT", 8200, 44),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8100, 0),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8200, 0),
    ]

    payload = build_option_pc_payload_for_product("2026-05-08", "MO", 8132, rows)

    assert payload["option_pc_current_month"] is None
    assert payload["option_pc_current_month_contract_month"] == "2605"


def test_product_payload_uses_next_contract_on_expiry_day():
    rows = [
        _option_row("2026-04-17", "MO", "2604", "PUT", 8100, 999),
        _option_row("2026-04-17", "MO", "2604", "PUT", 8200, 999),
        _option_row("2026-04-17", "MO", "2604", "CALL", 8100, 1),
        _option_row("2026-04-17", "MO", "2604", "CALL", 8200, 1),
        _option_row("2026-04-17", "MO", "2605", "PUT", 8100, 23),
        _option_row("2026-04-17", "MO", "2605", "PUT", 8200, 44),
        _option_row("2026-04-17", "MO", "2605", "CALL", 8100, 44),
        _option_row("2026-04-17", "MO", "2605", "CALL", 8200, 23),
    ]

    payload = build_option_pc_payload_for_product("2026-04-17", "MO", 8132, rows)

    assert payload["option_pc_current_month_contract_month"] == "2605"
    assert payload["option_pc_current_month"] == pytest.approx(29.72 / 37.28)


def test_product_payload_uses_single_day_mo_close_override():
    rows = [
        _option_row("2024-09-30", "MO", "2410", "PUT", 5600, 120),
        _option_row("2024-09-30", "MO", "2410", "PUT", 5700, 180.2),
        _option_row("2024-09-30", "MO", "2410", "CALL", 5600, 570),
        _option_row("2024-09-30", "MO", "2410", "CALL", 5700, 510),
    ]

    payload = build_option_pc_payload_for_product("2024-09-30", "MO", 5708.83, rows)

    assert payload["option_pc_current_month_contract_month"] == "2410"
    assert payload["option_pc_current_month"] == pytest.approx(180.2 / 510)
    assert payload["option_pc_current_month_special_flag"] == 1
    assert payload["option_pc_current_month_special_note"] == "中证1000 MO2410 使用特殊点位 5700 计算"


def test_product_payload_uses_2025_04_07_mo_2505_override_for_next_month():
    rows = [
        _option_row("2025-04-07", "MO", "2504", "PUT", 5400, 220),
        _option_row("2025-04-07", "MO", "2504", "PUT", 5500, 300),
        _option_row("2025-04-07", "MO", "2504", "CALL", 5400, 180),
        _option_row("2025-04-07", "MO", "2504", "CALL", 5500, 146),
        _option_row("2025-04-07", "MO", "2505", "PUT", 5500, 400),
        _option_row("2025-04-07", "MO", "2505", "PUT", 5600, 460),
        _option_row("2025-04-07", "MO", "2505", "CALL", 5500, 278.8),
        _option_row("2025-04-07", "MO", "2505", "CALL", 5600, 207.4),
    ]

    payload = build_option_pc_payload_for_product("2025-04-07", "MO", 5496.44, rows)

    assert payload["option_pc_next_month_contract_month"] == "2505"
    assert payload["option_pc_next_month"] == pytest.approx(400 / 278.8)
    assert payload["option_pc_next_month_special_flag"] == 1
    assert payload["option_pc_next_month_special_note"] == "中证1000 MO2505 使用特殊点位 5500 计算"


def test_product_payload_regular_date_has_no_special_marker():
    rows = [
        _option_row("2026-05-08", "MO", "2605", "PUT", 8100, 23),
        _option_row("2026-05-08", "MO", "2605", "PUT", 8200, 44),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8100, 44),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8200, 23),
    ]

    payload = build_option_pc_payload_for_product("2026-05-08", "MO", 8132, rows)

    assert payload["option_pc_current_month"] == pytest.approx(29.72 / 37.28)
    assert payload["option_pc_current_month_special_flag"] == 0
    assert payload["option_pc_current_month_special_note"] is None


def test_index_option_pc_map_builds_core_indexes_and_shanghai_average_only():
    rows = [
        _option_row("2026-05-08", "HO", "2605", "PUT", 3000, 10),
        _option_row("2026-05-08", "HO", "2605", "PUT", 3100, 20),
        _option_row("2026-05-08", "HO", "2605", "CALL", 3000, 30),
        _option_row("2026-05-08", "HO", "2605", "CALL", 3100, 20),
        _option_row("2026-05-08", "IO", "2605", "PUT", 4000, 20),
        _option_row("2026-05-08", "IO", "2605", "PUT", 4100, 30),
        _option_row("2026-05-08", "IO", "2605", "CALL", 4000, 40),
        _option_row("2026-05-08", "IO", "2605", "CALL", 4100, 30),
        _option_row("2026-05-08", "MO", "2605", "PUT", 8100, 23),
        _option_row("2026-05-08", "MO", "2605", "PUT", 8200, 44),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8100, 44),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8200, 23),
    ]
    index_close_map = {
        ("2026-05-08", "上证50"): 3050,
        ("2026-05-08", "沪深300"): 4050,
        ("2026-05-08", "中证1000"): 8132,
        ("2026-05-08", "中证500"): 6100,
    }

    result = build_index_option_pc_map(rows, index_close_map)

    ho_ratio = 15 / 25
    io_ratio = 25 / 35
    mo_ratio = 29.72 / 37.28
    assert result[("2026-05-08", "上证50")]["option_pc_current_month"] == pytest.approx(ho_ratio)
    assert result[("2026-05-08", "沪深300")]["option_pc_current_month"] == pytest.approx(io_ratio)
    assert result[("2026-05-08", "中证1000")]["option_pc_current_month"] == pytest.approx(mo_ratio)
    assert result[("2026-05-08", "上证指数")]["option_pc_current_month"] == pytest.approx(
        (ho_ratio + io_ratio + mo_ratio) / 3
    )
    assert ("2026-05-08", "中证500") not in result


def test_shanghai_average_carries_special_marker_from_core_index():
    rows = [
        _option_row("2025-04-07", "HO", "2505", "PUT", 3000, 10),
        _option_row("2025-04-07", "HO", "2505", "PUT", 3100, 20),
        _option_row("2025-04-07", "HO", "2505", "CALL", 3000, 30),
        _option_row("2025-04-07", "HO", "2505", "CALL", 3100, 20),
        _option_row("2025-04-07", "MO", "2505", "PUT", 5500, 400),
        _option_row("2025-04-07", "MO", "2505", "PUT", 5600, 460),
        _option_row("2025-04-07", "MO", "2505", "CALL", 5500, 278.8),
        _option_row("2025-04-07", "MO", "2505", "CALL", 5600, 207.4),
    ]
    index_close_map = {
        ("2025-04-07", "上证50"): 3050,
        ("2025-04-07", "中证1000"): 5496.44,
    }

    result = build_index_option_pc_map(rows, index_close_map)

    assert result[("2025-04-07", "上证指数")]["option_pc_current_month_special_flag"] == 1
    assert (
        "中证1000 MO2505 使用特殊点位 5500 计算"
        in result[("2025-04-07", "上证指数")]["option_pc_current_month_special_note"]
    )


def test_product_flow_pc_aggregates_volume_and_turnover():
    rows = [
        _option_row("2026-05-08", "MO", "2605", "PUT", 8100, 23, volume=10, turnover=200),
        _option_row("2026-05-08", "MO", "2605", "PUT", 8200, 44, volume=30, turnover=600),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8100, 44, volume=20, turnover=500),
        _option_row("2026-05-08", "MO", "2606", "CALL", 8200, 23, volume=60, turnover=1500),
    ]

    payload = build_option_flow_pc_payload_for_product("2026-05-08", "MO", rows)

    assert payload["option_volume_pc_ratio"] == pytest.approx(40 / 80)
    assert payload["option_turnover_pc_ratio"] == pytest.approx(800 / 2000)


def test_product_flow_pc_skips_expiring_contract_on_expiry_day():
    rows = [
        _option_row("2026-04-17", "MO", "2604", "PUT", 8100, 23, volume=999, turnover=999),
        _option_row("2026-04-17", "MO", "2604", "CALL", 8100, 44, volume=1, turnover=1),
        _option_row("2026-04-17", "MO", "2605", "PUT", 8100, 23, volume=40, turnover=800),
        _option_row("2026-04-17", "MO", "2605", "CALL", 8100, 44, volume=80, turnover=2000),
    ]

    payload = build_option_flow_pc_payload_for_product("2026-04-17", "MO", rows)

    assert payload["option_volume_pc_ratio"] == pytest.approx(0.5)
    assert payload["option_turnover_pc_ratio"] == pytest.approx(0.4)


def test_product_flow_pc_returns_null_when_call_sum_missing_or_zero():
    rows = [
        _option_row("2026-05-08", "MO", "2605", "PUT", 8100, 23, volume=10, turnover=200),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8100, 44, volume=0, turnover=0),
    ]

    payload = build_option_flow_pc_payload_for_product("2026-05-08", "MO", rows)

    assert payload["option_volume_pc_ratio"] is None
    assert payload["option_turnover_pc_ratio"] is None


def test_index_option_flow_pc_map_builds_core_indexes_and_shanghai_average_only():
    rows = [
        _option_row("2026-05-08", "HO", "2605", "PUT", 3000, 10, volume=10, turnover=100),
        _option_row("2026-05-08", "HO", "2605", "CALL", 3000, 30, volume=20, turnover=200),
        _option_row("2026-05-08", "IO", "2605", "PUT", 4000, 20, volume=30, turnover=600),
        _option_row("2026-05-08", "IO", "2605", "CALL", 4000, 40, volume=60, turnover=1200),
        _option_row("2026-05-08", "MO", "2605", "PUT", 8100, 23, volume=40, turnover=400),
        _option_row("2026-05-08", "MO", "2605", "CALL", 8100, 44, volume=80, turnover=1600),
    ]

    result = build_index_option_flow_pc_map(rows)

    assert result[("2026-05-08", "上证50")]["option_volume_pc_ratio"] == pytest.approx(0.5)
    assert result[("2026-05-08", "沪深300")]["option_turnover_pc_ratio"] == pytest.approx(0.5)
    assert result[("2026-05-08", "中证1000")]["option_turnover_pc_ratio"] == pytest.approx(0.25)
    assert result[("2026-05-08", "上证指数")]["option_volume_pc_ratio"] == pytest.approx((0.5 + 0.5 + 0.5) / 3)
    assert result[("2026-05-08", "上证指数")]["option_turnover_pc_ratio"] == pytest.approx((0.5 + 0.5 + 0.25) / 3)
    assert ("2026-05-08", "中证500") not in result


def _exchange_option_row(
    exchange,
    underlying_code,
    contract_code,
    contract_trade_code,
    contract_month,
    option_type,
    strike_price,
    close_price,
    *,
    trade_date="2026-05-08",
    last_trade_date="2026-05-27",
    volume=10,
    turnover=100,
):
    return {
        "trade_date": trade_date,
        "exchange": exchange,
        "underlying_code": underlying_code,
        "contract_code": contract_code,
        "contract_trade_code": contract_trade_code,
        "contract_month": contract_month,
        "option_type": option_type,
        "strike_price": strike_price,
        "close_price": close_price,
        "last_trade_date": last_trade_date,
        "volume": volume,
        "turnover": turnover,
    }


def test_exchange_option_series_uses_etf_close_and_keeps_products_separate():
    rows = [
        _exchange_option_row("SSE", "510300", "1001", "510300P2605M004000", "2605", "PUT", 4.0, 0.10),
        _exchange_option_row("SSE", "510300", "1002", "510300P2605M004200", "2605", "PUT", 4.2, 0.30),
        _exchange_option_row("SSE", "510300", "1003", "510300C2605M004000", "2605", "CALL", 4.0, 0.30),
        _exchange_option_row("SSE", "510300", "1004", "510300C2605M004200", "2605", "CALL", 4.2, 0.10),
        _exchange_option_row("SZSE", "159919", "9001", "159919P2605M004000", "2605", "PUT", 4.0, 0.20),
        _exchange_option_row("SZSE", "159919", "9002", "159919P2605M004200", "2605", "PUT", 4.2, 0.40),
        _exchange_option_row("SZSE", "159919", "9003", "159919C2605M004000", "2605", "CALL", 4.0, 0.40),
        _exchange_option_row("SZSE", "159919", "9004", "159919C2605M004200", "2605", "CALL", 4.2, 0.20),
    ]

    result = build_exchange_option_pc_map(
        rows,
        {
            ("2026-05-08", "510300"): 4.1,
            ("2026-05-08", "159919"): 4.1,
        },
    )

    sources = result[("2026-05-08", "沪深300")]
    assert set(sources) == {"sse:510300", "szse:159919"}
    assert sources["sse:510300"]["option_pc_current_month"] == pytest.approx(1)
    assert sources["szse:159919"]["option_pc_current_month"] == pytest.approx(1)
    assert sources["sse:510300"]["product_code"] == "510300"
    assert sources["szse:159919"]["exchange_label"] == "深交所"


def test_exchange_option_price_excludes_adjusted_contract_but_flow_includes_it():
    rows = [
        _exchange_option_row("SSE", "510050", "1001", "510050P2605M003000", "2605", "PUT", 3.0, 0.10, volume=10, turnover=100),
        _exchange_option_row("SSE", "510050", "1002", "510050P2605M003200", "2605", "PUT", 3.2, 0.30, volume=10, turnover=100),
        _exchange_option_row("SSE", "510050", "1003", "510050C2605M003000", "2605", "CALL", 3.0, 0.30, volume=20, turnover=200),
        _exchange_option_row("SSE", "510050", "1004", "510050C2605M003200", "2605", "CALL", 3.2, 0.10, volume=20, turnover=200),
        _exchange_option_row("SSE", "510050", "1005", "510050P2605A003100", "2605", "PUT", 3.1, 99, volume=30, turnover=300),
    ]

    payload = build_exchange_option_pc_map(
        rows,
        {("2026-05-08", "510050"): 3.1},
    )[("2026-05-08", "上证50")]["sse:510050"]

    assert payload["option_pc_current_month"] == pytest.approx(1)
    assert payload["option_volume_pc_ratio"] == pytest.approx(50 / 40)
    assert payload["option_turnover_pc_ratio"] == pytest.approx(500 / 400)


def test_exchange_option_uses_real_last_trade_date_and_does_not_extrapolate():
    rows = [
        _exchange_option_row(
            "SSE",
            "588000",
            "1001",
            "588000P2605M001000",
            "2605",
            "PUT",
            1.0,
            0.10,
            trade_date="2026-05-27",
            last_trade_date="2026-05-27",
        ),
        _exchange_option_row(
            "SSE",
            "588000",
            "1002",
            "588000C2605M001000",
            "2605",
            "CALL",
            1.0,
            0.10,
            trade_date="2026-05-27",
            last_trade_date="2026-05-27",
        ),
    ]

    payload = build_exchange_option_pc_map(
        rows,
        {("2026-05-27", "588000"): 1.2},
    )[("2026-05-27", "科创50")]["sse:588000"]

    assert payload["option_pc_current_month"] is None
    assert payload["option_volume_pc_ratio"] is None
