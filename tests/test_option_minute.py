import asyncio
from datetime import datetime, timedelta

import pytest

from akshare_project.collectors import option_minute
from akshare_project.services import stock_temp_service


def _term_rows(expiry, contract_month, multiplier=1.0, basis="mid_quote"):
    rows = []
    for strike, call_price, put_price in (
        (90, 12, 1),
        (100, 6, 5),
        (110, 2, 11),
    ):
        for option_type, price in (("CALL", call_price), ("PUT", put_price)):
            rows.append(
                {
                    "exchange": "CFFEX",
                    "underlying_code": "IO",
                    "option_type": option_type,
                    "contract_month": contract_month,
                    "strike_price": strike,
                    "expire_date": expiry,
                    "bar_time": "2026-07-06 10:00:00",
                    "mid_price": price * multiplier if basis == "mid_quote" else None,
                    "close_price": price * multiplier,
                    "bid1_price": price * multiplier - 0.1 if basis == "mid_quote" else None,
                }
            )
    return rows


def test_quote_midpoint_accepts_zero_bid_and_rejects_crossed_quote():
    assert option_minute.quote_midpoint(1, 3) == 2
    assert option_minute.quote_midpoint(0, 2) == 1
    assert option_minute.quote_midpoint(3, 2) is None


def test_parse_sina_five_day_payload_reads_dict_rows_and_forward_fills_date():
    metadata = {
        "exchange": "SSE",
        "contract_code": "10000001",
        "contract_trade_code": "510050C2608M03000",
        "underlying_code": "510050",
        "option_type": "CALL",
        "contract_month": "2608",
        "strike_price": 3,
        "expire_date": "2026-08-26",
    }
    payload = {
        "result": {
            "data": [
                [
                    {"i": "09:30:00", "p": "0.1000", "v": "2", "a": "0.1000", "d": "2026-07-06"},
                    {"i": "09:31:00", "p": "0.1100", "v": "3", "a": "0.1060"},
                ]
            ]
        }
    }

    rows = option_minute.parse_sina_five_day_payload(payload, metadata)

    assert [row["bar_time"] for row in rows] == [
        "2026-07-06 09:30:00",
        "2026-07-06 09:31:00",
    ]
    assert rows[1]["close_price"] == pytest.approx(0.11)
    assert rows[1]["minute_volume"] == 3


def test_exchange_snapshot_uses_polling_minute_for_the_whole_chain():
    metadata = {
        "exchange": "SSE",
        "contract_code": "10000001",
        "contract_trade_code": "510050C2608M03000",
        "underlying_code": "510050",
        "option_type": "CALL",
        "contract_month": "2608",
        "strike_price": 3,
        "expire_date": "2026-08-26",
    }
    rows = option_minute.build_exchange_snapshot_rows(
        [metadata],
        {
            "10000001": {
                "quote_time": "2026-07-13 13:08:00",
                "close_price": "0.1",
                "bid1_price": "0.09",
                "ask1_price": "0.11",
            }
        },
        "2026-07-13 13:17:00",
    )

    assert rows[0]["bar_time"] == "2026-07-13 13:17:00"
    assert rows[0]["raw_json"]["source_quote_time"] == "2026-07-13 13:08:00"


def test_live_snapshot_cannot_be_backdated():
    with pytest.raises(ValueError, match="cannot be backdated"):
        asyncio.run(option_minute.collect_snapshot("2020-01-02 10:00:00"))


def test_cffex_snapshot_parser_keeps_bid_ask_and_normalizes_contract_code():
    payload = {
        "result": {
            "data": {
                "up": [[2, 10, 11, 12, 3, 100, 0, 4000, "io2608C4000"]],
                "down": [[4, 8, 9, 10, 5, 120, 0, "io2608P4000"]],
            }
        }
    }

    rows = option_minute.parse_cffex_option_payload(
        payload,
        "IO",
        "2608",
        datetime(2026, 8, 21).date(),
        datetime(2026, 7, 6, 10, 0),
    )

    assert rows[0]["contract_code"] == "IO2608-C-4000"
    assert rows[0]["mid_price"] == 11
    assert rows[1]["contract_code"] == "IO2608-P-4000"
    assert rows[1]["mid_price"] == 9


def test_minute_vix_uses_exact_intraday_time_and_mid_quotes():
    rows = [
        *_term_rows("2026-07-31", "2607", multiplier=1.0),
        *_term_rows("2026-08-31", "2608", multiplier=1.1),
    ]
    rate_rows = [
        {
            "trade_date": "2026-07-03",
            "tenor_days": days,
            "rate_decimal": value,
        }
        for days, value in ((7, 0.015), (30, 0.016), (90, 0.017))
    ]

    result = option_minute.build_minute_vix_rows(rows, rate_rows)

    assert len(result) == 1
    assert result[0]["source_key"] == "cffex:IO"
    assert result[0]["vix_value"] > 0
    assert result[0]["price_basis"] == "mid_quote"
    assert result[0]["near_contract_month"] == "2607"
    assert result[0]["next_contract_month"] == "2608"


def test_zero_bid_tail_is_excluded_after_two_consecutive_strikes():
    rows = _term_rows("2026-08-31", "2608")
    rows.extend(
        [
            {
                **rows[0],
                "option_type": "PUT",
                "strike_price": strike,
                "mid_price": 0.05,
                "bid1_price": 0,
            }
            for strike in (80, 70, 60)
        ]
    )

    term = option_minute.calculate_minute_term_variance(
        rows,
        "2026-07-06 10:00:00",
        0.016,
    )

    assert term is not None
    assert term["strike_count"] == 3


def test_daily_session_collects_live_bars_until_close_then_finalizes():
    class Clock:
        current = datetime(2026, 7, 13, 14, 59, 58)

        @classmethod
        def now(cls):
            return cls.current

        @classmethod
        async def sleep(cls, seconds):
            cls.current += timedelta(seconds=seconds)

    captured_bars = []

    async def snapshot_handler(bar_time):
        captured_bars.append(bar_time)
        return {
            "minute_rows": 100,
            "vix_rows": 10,
            "source_counts": {"cffex:IO": 30},
        }

    async def finalize_handler(target_date):
        assert target_date == "2026-07-13"
        return {"status": "SUCCESS", "target_date": target_date, "coverage": []}

    result = asyncio.run(
        option_minute.run_daily_session(
            "2026-07-13",
            now_provider=Clock.now,
            sleep_handler=Clock.sleep,
            snapshot_handler=snapshot_handler,
            finalize_handler=finalize_handler,
        )
    )

    assert [item.strftime("%H:%M") for item in captured_bars] == ["14:59", "15:00"]
    assert result["session_mode"] == "live_trading_session"
    assert result["snapshot_bars"] == 2
    assert result["live_minute_rows"] == 200
    assert result["live_vix_rows"] == 20
    assert result["live_source_counts"] == {"cffex:IO": 60}


def test_daily_session_after_close_only_runs_finalize():
    finalized = []

    async def finalize_handler(target_date):
        finalized.append(target_date)
        return {"status": "SUCCESS", "target_date": target_date}

    result = asyncio.run(
        option_minute.run_daily_session(
            "2026-07-13",
            now_provider=lambda: datetime(2026, 7, 13, 15, 5),
            finalize_handler=finalize_handler,
        )
    )

    assert finalized == ["2026-07-13"]
    assert result["session_mode"] == "post_close_finalize"
    assert result["snapshot_bars"] == 0


def test_stock_temp_route_runs_the_trading_session(monkeypatch):
    calls = []

    async def fake_session(target_date=None):
        calls.append(target_date)
        return {"status": "SUCCESS", "target_date": target_date}

    monkeypatch.setattr(option_minute, "run_daily_session", fake_session)
    route = stock_temp_service.build_daily_routes()["/collect-option-minute-daily"]

    result = asyncio.run(route.handler(target_date="2026-07-13"))

    assert result == {"status": "SUCCESS", "target_date": "2026-07-13"}
    assert calls == ["2026-07-13"]
