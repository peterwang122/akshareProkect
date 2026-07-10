from akshare_project.collectors.quant_index import (
    build_option_vix_map,
    build_option_vix_payload,
    interpolate_risk_free_rate,
)


def _term_rows(contract_month, multiplier=1.0, **extra):
    rows = []
    for strike, call_price, put_price in (
        (90, 12, 1),
        (100, 6, 5),
        (110, 2, 11),
    ):
        for option_type, close_price in (
            ("CALL", call_price),
            ("PUT", put_price),
        ):
            rows.append(
                {
                    "option_type": option_type,
                    "strike_price": strike,
                    "open_price": close_price * multiplier,
                    "close_price": close_price,
                    "settle_price": close_price,
                    "pre_settle_price": close_price,
                    "contract_month": contract_month,
                    **extra,
                }
            )
    return rows


def test_risk_free_rate_is_linearly_interpolated_by_calendar_days():
    assert interpolate_risk_free_rate({7: 0.01, 30: 0.02}, 7) == 0.01
    assert interpolate_risk_free_rate({7: 0.01, 30: 0.02}, 30) == 0.02
    assert interpolate_risk_free_rate({7: 0.01, 30: 0.02}, 18.5) == 0.015


def test_vix_payload_calculates_independent_open_and_close_values():
    rows = _term_rows("2608", multiplier=1.2)
    payload = build_option_vix_payload(
        "2026-07-06",
        "CFFEX",
        "IO",
        rows,
        "2026-07-03",
        {7: 0.015, 30: 0.016, 90: 0.017},
    )

    assert payload is not None
    assert payload["vix_open"] > payload["vix_close"] > 0
    assert payload["vix_high"] == payload["vix_open"]
    assert payload["vix_low"] == payload["vix_close"]
    assert payload["calculation_method"] == "ivix_30d_option_open_and_close"


def test_open_vix_is_not_backfilled_from_close_prices():
    rows = _term_rows("2608", multiplier=1.0)
    for row in rows:
        row["open_price"] = None

    payload = build_option_vix_payload(
        "2026-07-06",
        "CFFEX",
        "IO",
        rows,
        "2026-07-03",
        {7: 0.015, 30: 0.016, 90: 0.017},
    )

    assert payload is not None
    assert payload["vix_open"] is None
    assert payload["vix_close"] > 0
    assert payload["vix_high"] == payload["vix_close"]
    assert payload["vix_low"] == payload["vix_close"]


def test_vix_map_keeps_cffex_and_exchange_products_separate():
    rate_rows = [
        {
            "trade_date": "2026-07-03",
            "tenor_days": tenor_days,
            "rate_decimal": rate,
        }
        for tenor_days, rate in ((7, 0.015), (30, 0.016), (90, 0.017))
    ]
    cffex_rows = [
        {
            **row,
            "trade_date": "2026-07-03",
            "product_prefix": "IO",
        }
        for row in _term_rows("2608", multiplier=1.1)
    ]
    exchange_rows = [
        {
            **row,
            "trade_date": "2026-07-03",
            "exchange": "SSE",
            "underlying_code": "510300",
            "last_trade_date": "2026-08-26",
            "contract_trade_code": f"510300C2608M{row['strike_price']}",
        }
        for row in _term_rows("2608", multiplier=1.2)
    ]

    result = build_option_vix_map(cffex_rows, exchange_rows, rate_rows)

    assert set(result[("2026-07-03", "沪深300")]) == {
        "cffex:IO",
        "sse:510300",
    }
    assert (
        result[("2026-07-03", "沪深300")]["cffex:IO"]["vix_open"]
        != result[("2026-07-03", "沪深300")]["sse:510300"]["vix_open"]
    )
