from akshare_project.collectors.quant_index import (
    black_76_option_price,
    build_option_vix_map,
    build_option_vix_payload,
    interpolate_risk_free_rate,
    merge_option_vix_minute_ohlc,
)


def _skew_term_rows(contract_month, days_to_expiry, risk_free_rate, put_vol, call_vol):
    time_to_expiry = days_to_expiry / 365
    rows = []
    for strike in (85, 90, 95, 100, 105, 110, 115):
        volatility = put_vol if strike < 100 else call_vol if strike > 100 else 0.25
        for option_type in ("CALL", "PUT"):
            price = black_76_option_price(
                100,
                strike,
                time_to_expiry,
                risk_free_rate,
                volatility,
                option_type,
            )
            rows.append(
                {
                    "option_type": option_type,
                    "strike_price": strike,
                    "open_price": price,
                    "close_price": price,
                    "settle_price": price,
                    "pre_settle_price": price,
                    "contract_month": contract_month,
                }
            )
    return rows


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


def test_vix_payload_exposes_term_structure_and_25d_downside_skew():
    rate = 0.016
    rows = [
        *_skew_term_rows("2608", 46, rate, put_vol=0.36, call_vol=0.20),
        *_skew_term_rows("2609", 74, rate, put_vol=0.32, call_vol=0.18),
    ]
    payload = build_option_vix_payload(
        "2026-07-06",
        "CFFEX",
        "IO",
        rows,
        "2026-07-03",
        {7: rate, 30: rate, 90: rate},
    )

    assert payload is not None
    assert payload["near_term_vix"] > 0
    assert payload["next_term_vix"] > 0
    assert payload["vix_term_structure"] == (
        payload["next_term_vix"] - payload["near_term_vix"]
    )
    assert payload["downside_skew_25d"] > 5
    assert payload["near_put_25d_implied_volatility"] > payload["near_call_25d_implied_volatility"]


def test_vix_term_structure_requires_sufficient_strike_coverage():
    payload = build_option_vix_payload(
        "2026-07-06",
        "CFFEX",
        "IO",
        [*_term_rows("2608"), *_term_rows("2609")],
        "2026-07-03",
        {7: 0.016, 30: 0.016, 90: 0.016},
    )

    assert payload is not None
    assert payload["near_term_vix"] is not None
    assert payload["next_term_vix"] is not None
    assert payload["vix_term_structure"] is None


def test_vix_payload_rejects_implausible_25d_skew():
    rate = 0.016
    rows = [
        *_skew_term_rows("2608", 46, rate, put_vol=1.20, call_vol=0.20),
        *_skew_term_rows("2609", 74, rate, put_vol=1.10, call_vol=0.20),
    ]
    payload = build_option_vix_payload(
        "2026-07-06",
        "CFFEX",
        "IO",
        rows,
        "2026-07-03",
        {7: rate, 30: rate, 90: rate},
    )

    assert payload is not None
    assert payload["downside_skew_25d"] is None


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


def test_minute_ohlc_overrides_daily_two_point_vix_range():
    daily_map = {
        ("2026-07-13", "沪深300"): {
            "cffex:IO": {
                "source_key": "cffex:IO",
                "vix_open": 20,
                "vix_high": 22,
                "vix_low": 20,
                "vix_close": 22,
            }
        }
    }

    result = merge_option_vix_minute_ohlc(
        daily_map,
        [
            {
                "trade_date": "2026-07-13",
                "source_key": "cffex:IO",
                "index_name": "沪深300",
                "exchange": "CFFEX",
                "product_code": "IO",
                "vix_open": 21,
                "vix_high": 29,
                "vix_low": 18,
                "vix_close": 24,
                "minute_count": 242,
                "mid_quote_count": 242,
                "near_contract_month": "2608",
                "near_expire_date": "2026-08-21",
                "price_basis": "mid_quote",
            }
        ],
    )

    payload = result[("2026-07-13", "沪深300")]["cffex:IO"]
    assert payload["vix_open"] == 21
    assert payload["vix_high"] == 29
    assert payload["vix_low"] == 18
    assert payload["vix_close"] == 24
    assert payload["minute_count"] == 242


def test_last_trade_only_minute_vix_does_not_override_daily_candle():
    daily_payload = {
        "source_key": "sse:510500",
        "vix_open": 26,
        "vix_high": 28,
        "vix_low": 25,
        "vix_close": 27,
    }
    daily_map = {
        ("2026-07-08", "中证500"): {"sse:510500": daily_payload.copy()}
    }

    result = merge_option_vix_minute_ohlc(
        daily_map,
        [
            {
                "trade_date": "2026-07-08",
                "index_name": "中证500",
                "exchange": "SSE",
                "product_code": "510500",
                "vix_open": 27,
                "vix_high": 58,
                "vix_low": 26,
                "vix_close": 28,
                "minute_count": 241,
                "mid_quote_count": 0,
                "price_basis": "last_trade",
            }
        ],
    )

    assert result[("2026-07-08", "中证500")]["sse:510500"] == daily_payload


def test_incomplete_minute_session_does_not_override_daily_candle():
    daily_payload = {
        "source_key": "cffex:IO",
        "vix_open": 20,
        "vix_high": 22,
        "vix_low": 19,
        "vix_close": 21,
    }
    daily_map = {
        ("2026-07-13", "沪深300"): {"cffex:IO": daily_payload.copy()}
    }

    result = merge_option_vix_minute_ohlc(
        daily_map,
        [
            {
                "trade_date": "2026-07-13",
                "index_name": "沪深300",
                "exchange": "CFFEX",
                "product_code": "IO",
                "vix_open": 24,
                "vix_high": 27,
                "vix_low": 23,
                "vix_close": 25,
                "minute_count": 100,
                "mid_quote_count": 100,
                "price_basis": "mid_quote",
            }
        ],
    )

    assert result[("2026-07-13", "沪深300")]["cffex:IO"] == daily_payload
