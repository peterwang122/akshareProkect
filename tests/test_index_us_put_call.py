import asyncio

import pandas as pd
import pytest

from akshare_project.collectors import index as index_collector


def _put_call_row(trade_date, total=0.9):
    return {
        "trade_date": trade_date,
        "total_put_call_ratio": total,
        "index_put_call_ratio": 1.1,
        "equity_put_call_ratio": 0.6,
        "etf_put_call_ratio": 1.0,
        "data_source": index_collector.US_PUT_CALL_SOURCE,
    }


def test_build_us_put_call_ratio_row_from_daily_options_json_parses_all_scopes():
    payload = {
        "ratios": [
            {"name": "TOTAL PUT/CALL RATIO", "value": "0.87"},
            {"name": "INDEX PUT/CALL RATIO", "value": "1.12"},
            {"name": "EQUITY PUT/CALL RATIO", "value": "0.54"},
            {"name": "EXCHANGE TRADED PRODUCTS PUT/CALL RATIO", "value": "0.98"},
        ]
    }

    row = index_collector.build_us_put_call_ratio_row_from_daily_options_json(payload, "2026-05-04")

    assert row == {
        "trade_date": "2026-05-04",
        "total_put_call_ratio": 0.87,
        "index_put_call_ratio": 1.12,
        "equity_put_call_ratio": 0.54,
        "etf_put_call_ratio": 0.98,
        "data_source": index_collector.US_PUT_CALL_SOURCE,
    }


def test_fetch_us_put_call_daily_json_rows_for_dates_deduplicates_and_sorts(monkeypatch):
    requested_dates = []

    def fake_fetch(trade_date):
        requested_dates.append(trade_date)
        return {
            "ratios": [
                {"name": "TOTAL PUT/CALL RATIO", "value": "0.90"},
            ]
        }

    monkeypatch.setattr(index_collector, "fetch_us_put_call_daily_options_json", fake_fetch)

    rows, skipped, failures = asyncio.run(
        index_collector.fetch_us_put_call_daily_json_rows_for_dates(
            ["2026-05-05", "2026-05-04", "2026-05-05"],
            concurrency=1,
        )
    )

    assert requested_dates == ["2026-05-04", "2026-05-05"]
    assert [row["trade_date"] for row in rows] == ["2026-05-04", "2026-05-05"]
    assert skipped == 0
    assert failures == []


def test_sync_daily_us_put_call_ratio_repairs_all_missing_dates(monkeypatch):
    today = index_collector.datetime.now().date().strftime("%Y-%m-%d")
    recent_weekdays = index_collector.build_weekday_date_strings(
        (index_collector.datetime.now().date() - index_collector.timedelta(days=14)).strftime("%Y-%m-%d"),
        today,
    )
    latest_weekday = recent_weekdays[-1]
    historical_gap = "2026-05-04"
    requested_dates = []

    class FakeDbTools:
        def __init__(self):
            self.upserted_rows = []

        async def get_index_us_put_call_missing_trade_dates(self, start_date, end_date, limit):
            assert start_date == index_collector.US_PUT_CALL_DAILY_JSON_START_DATE
            assert end_date == today
            assert limit == 256
            return [historical_gap]

        async def upsert_index_us_put_call_ratio_daily(self, rows):
            self.upserted_rows = rows
            return len(rows)

    async def fake_fetch(candidate_dates, concurrency):
        requested_dates.extend(candidate_dates)
        assert concurrency == 4
        return [_put_call_row(historical_gap, 0.87), _put_call_row(latest_weekday, 0.91)], 0, []

    monkeypatch.setattr(index_collector, "fetch_us_put_call_daily_json_rows_for_dates", fake_fetch)
    db_tools = FakeDbTools()

    upserted = asyncio.run(index_collector.sync_daily_us_put_call_ratio(db_tools))

    assert historical_gap in requested_dates
    assert latest_weekday in requested_dates
    assert upserted == 2
    assert [row["trade_date"] for row in db_tools.upserted_rows] == [historical_gap, latest_weekday]


def test_parse_optionomics_option_premium_html_keeps_display_precision():
    html = """
      <span class="eyebrow">Live board · August 28, 2026</span>
      <strong>$16639.8<small>M</small></strong>
      <span>$10180.5M in calls</span>
      <span>$6459.3M in puts</span>
    """

    row = index_collector.parse_optionomics_option_premium_html(html)

    assert row["trade_date"] == "2026-08-28"
    assert row["total_premium_million_usd"] == 16639.8
    assert row["call_premium_million_usd"] == 10180.5
    assert row["put_premium_million_usd"] == 6459.3
    assert row["premium_put_call_ratio"] == 0.634478
    assert row["rounding_unit_million_usd"] == 0.1
    assert row["value_basis"] == "source_display_rounded_0.1m_usd"
    assert row["raw_json"]["put_display"] == "$6459.3M"


def test_parse_optionomics_option_premium_html_rejects_inconsistent_total():
    html = """
      <span>Live board · August 28, 2026</span>
      <strong>$10000.0<small>M</small></strong>
      <span>$7000.0M in calls</span>
      <span>$4000.0M in puts</span>
    """

    with pytest.raises(ValueError, match="does not match"):
        index_collector.parse_optionomics_option_premium_html(html)


def test_sync_daily_us_option_premium_upserts_page_date(monkeypatch):
    date_display = index_collector.datetime.now().strftime("%B %d, %Y")
    html = f"""
      <span>Live board · {date_display}</span>
      <strong>$12000.0<small>M</small></strong>
      <span>$8000.0M in calls</span>
      <span>$4000.0M in puts</span>
    """

    class FakeDbTools:
        def __init__(self):
            self.upserted_rows = []

        async def upsert_index_us_option_premium_daily(self, rows):
            self.upserted_rows = rows
            return len(rows)

    monkeypatch.setattr(index_collector, "fetch_us_option_premium_html", lambda: html)
    db_tools = FakeDbTools()

    upserted = asyncio.run(index_collector.sync_daily_us_option_premium(db_tools))

    assert upserted == 1
    assert len(db_tools.upserted_rows) == 1
    assert db_tools.upserted_rows[0]["premium_put_call_ratio"] == 0.5


def _nasdaq_chain_payload(expiration="September 18, 2026"):
    return {
        "data": {
            "lastTrade": "LAST TRADE: $505.00 (AS OF AUG 28, 2026)",
            "table": {
                "rows": [
                    {"expirygroup": expiration},
                    {
                        "expiryDate": "Sep 18",
                        "strike": "500.00",
                        "c_Last": "12.00",
                        "c_Volume": "1,200",
                        "c_Openinterest": "5,000",
                        "p_Last": "7.00",
                        "p_Volume": "900",
                        "p_Openinterest": "4,000",
                    },
                    {
                        "expiryDate": "Sep 18",
                        "strike": "510.00",
                        "c_Last": "8.00",
                        "c_Volume": "1,100",
                        "c_Openinterest": "4,500",
                        "p_Last": "11.00",
                        "p_Volume": "800",
                        "p_Openinterest": "3,500",
                    },
                    {
                        "expiryDate": "Sep 18",
                        "strike": "515.00",
                        "c_Last": "6.00",
                        "c_Volume": "0",
                        "p_Last": "14.00",
                        "p_Volume": "0",
                    },
                ]
            },
        }
    }


def test_fetch_nasdaq_option_chain_encodes_query_in_url(monkeypatch):
    captured = {}

    def fake_get_json(url, headers=None, timeout=30):
        captured.update({"url": url, "headers": headers, "timeout": timeout})
        return {"data": {}}

    monkeypatch.setattr(index_collector, "http_get_json", fake_get_json)

    payload = index_collector.fetch_nasdaq_us_option_chain(
        "spy",
        from_date="2026-09-18",
        to_date="2026-09-18",
    )

    assert payload == {"data": {}}
    assert captured["url"].startswith(
        "https://api.nasdaq.com/api/quote/SPY/option-chain?"
    )
    assert "assetclass=etf" in captured["url"]
    assert "limit=5000" in captured["url"]
    assert "fromdate=2026-09-18" in captured["url"]
    assert "todate=2026-09-18" in captured["url"]
    assert captured["headers"]["Referer"].endswith("/spy/option-chain")


def test_nasdaq_option_chain_parser_keeps_real_traded_last_only():
    payload = _nasdaq_chain_payload()

    rows = index_collector.build_us_option_price_rows_from_nasdaq_payload(
        payload,
        "SPY",
        expiration="2026-09-18",
    )
    selected = index_collector.select_adjacent_us_option_price_rows(rows, 505.0)

    assert len(rows) == 4
    assert len(selected) == 4
    assert {row["option_type"] for row in selected} == {"CALL", "PUT"}
    assert {row["strike_price"] for row in selected} == {500.0, 510.0}
    assert all(row["data_source"] == index_collector.US_OPTION_PRICE_PC_LIVE_SOURCE for row in selected)
    assert all(row["value_basis"] == "last_trade_with_positive_daily_volume" for row in selected)


def test_us_option_expiration_selection_ignores_weeklies():
    selected = index_collector.select_us_option_pc_expirations(
        [
            "2026-09-04",
            "2026-09-17",
            "2026-09-18",
            "2026-10-16",
            "2026-11-20",
            "2026-12-18",
            "2027-03-19",
        ],
        "2026-08-28",
    )

    assert {key: value.isoformat() for key, value in selected.items()} == {
        "current_month": "2026-09-18",
        "next_month": "2026-10-16",
        "quarter_1": "2026-12-18",
        "quarter_2": "2027-03-19",
    }


def test_history_frame_uses_unadjusted_underlying_close_and_positive_volume():
    option_rows = []
    for expiration in ("2024-09-20", "2024-10-18", "2024-12-20", "2025-03-21"):
        for option_type in ("call", "put"):
            for strike, last, volume in ((495.0, 11.0, 10), (505.0, 9.0, 20), (510.0, 8.0, 0)):
                option_rows.append({
                    "contract_id": f"SPY-{expiration}-{option_type}-{strike}",
                    "symbol": "SPY",
                    "expiration": expiration,
                    "strike": strike,
                    "type": option_type,
                    "last": last,
                    "volume": volume,
                    "open_interest": 100,
                    "date": "2024-08-30",
                })
    options = pd.DataFrame(option_rows)
    underlying = pd.DataFrame([
        {
            "date": "2024-08-30",
            "close": 500.0,
            "adjusted_close": 400.0,
        }
    ])

    rows = index_collector.build_us_option_price_rows_from_history_frames(
        options,
        underlying,
        "SPY",
    )

    assert len(rows) == 16
    assert all(row["underlying_close"] == 500.0 for row in rows)
    assert all(row["volume"] > 0 for row in rows)
    assert all(row["strike_price"] in {495.0, 505.0} for row in rows)
    assert all(row["data_source"] == index_collector.US_OPTION_PRICE_PC_HISTORY_SOURCE for row in rows)
