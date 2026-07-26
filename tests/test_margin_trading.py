import asyncio
from datetime import date

import pytest

from akshare_project.collectors import margin_trading, quant_index


def test_parse_sse_payload_keeps_official_cny_units():
    rows = margin_trading.parse_sse_payload(
        {
            "result": [
                {
                    "opDate": "20260716",
                    "rzye": 1430821453142,
                    "rzmre": 106047371889,
                    "rzche": 120008846949,
                    "rqylje": 13223339987,
                    "rzrqjyzl": 1444044793129,
                    "rqmcl": 60215736,
                    "rqyl": 2430299813,
                }
            ]
        }
    )
    assert rows[0]["trade_date"] == "2026-07-16"
    assert rows[0]["financing_balance"] == 1430821453142
    assert rows[0]["securities_lending_balance"] == 13223339987


def test_parse_szse_payload_converts_yi_cny_to_cny():
    payload = [
        {
            "metadata": {"tabkey": "tab1", "subname": "2026-07-16"},
            "data": [
                {
                    "jrrzmr": "1,014.15",
                    "jrrzye": "13,982.04",
                    "jrrjmc": "0.40",
                    "jrrjyl": "8.79",
                    "jrrjye": "72.31",
                    "jrrzrjye": "14,054.35",
                }
            ],
        }
    ]
    row = margin_trading.parse_szse_payload(payload, "2026-07-16")
    assert row["financing_buy_amount"] == pytest.approx(101_415_000_000)
    assert row["financing_balance"] == pytest.approx(1_398_204_000_000)
    assert row["securities_lending_balance"] == pytest.approx(7_231_000_000)


def test_parse_bse_payload_uses_raw_yuan_values():
    payload = [
        [
            {
                "rzmre": 460725057,
                "rzye": 8675158673,
                "rqmcl": 0,
                "rqyl": 400,
                "rqye": 8180,
                "rzrqye": 8675166853,
            }
        ],
        "2026-07-16",
    ]
    row = margin_trading.parse_bse_payload(payload, "2026-07-16")
    assert row["financing_buy_amount"] == 460725057
    assert row["financing_balance"] == 8675158673
    assert row["margin_balance"] == 8675166853


def test_parse_bse_jsonp_accepts_official_single_quoted_date():
    payload = margin_trading.parse_jsonp(
        'callback([[{"rzye":8675158673}], \'2026-07-16\'])'
    )
    assert payload == [[{"rzye": 8675158673}], "2026-07-16"]


def test_exchange_coverage_changes_from_bse_launch_date():
    assert margin_trading.expected_exchanges("2023-02-10") == {"SSE", "SZSE"}
    assert margin_trading.expected_exchanges("2023-02-13") == {
        "SSE",
        "SZSE",
        "BSE",
    }


def test_dashboard_writes_margin_leverage_ratio_for_all_cn_indices():
    rows = quant_index.build_dashboard_rows(
        trade_dates=["2026-07-16"],
        index_code_map={},
        emotion_map={},
        index_close_map={},
        futures_close_map={},
        breadth_map={},
        margin_trading_map={
            "2026-07-16": {
                "margin_financing_balance": 2_829_025_453_142,
                "margin_securities_lending_balance": 20_454_339_987,
                "margin_total_balance": 2_849_479_793_129,
                "margin_financing_net_buy_amount": -28_586_475_046,
                "margin_leverage_ratio_pct": 2.991798,
            }
        },
    )

    assert rows
    assert all(row["margin_leverage_ratio_pct"] == 2.991798 for row in rows)


def test_source_date_mismatch_is_not_accepted():
    payload = [
        {
            "metadata": {"tabkey": "tab1", "subname": "2026-07-15"},
            "data": [{"jrrzye": "1.00"}],
        }
    ]
    assert margin_trading.parse_szse_payload(payload, "2026-07-16") is None


def test_fetch_sse_rows_reads_every_reported_page(monkeypatch):
    payloads = {
        "1": {
            "pageHelp": {"pageCount": 2},
            "result": [{"opDate": "20260716", "rzye": 2}],
        },
        "2": {
            "pageHelp": {"pageCount": 2},
            "result": [{"opDate": "20100331", "rzye": 1}],
        },
    }
    requested_pages = []

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class Session:
        def get(self, _url, params, **_kwargs):
            page_no = params["pageHelp.pageNo"]
            requested_pages.append(page_no)
            return Response(payloads[page_no])

    monkeypatch.setattr(margin_trading, "direct_session", lambda: Session())

    rows = margin_trading.fetch_sse_rows_sync("2010-03-31", "2026-07-16")

    assert requested_pages == ["1", "2"]
    assert [row["trade_date"] for row in rows] == ["2010-03-31", "2026-07-16"]


def test_sync_daily_refreshes_target_dashboard_after_all_exchanges_arrive(monkeypatch):
    target = date(2026, 7, 22)
    refreshed_dates = []

    class FakeDb:
        async def init_pool(self):
            return None

        async def ensure_margin_trading_daily_table(self):
            return None

        async def upsert_margin_trading_daily_rows(self, rows):
            return len(rows)

        async def recompute_margin_trading_net_buy(self, _start, _end):
            return None

        async def get_margin_trading_coverage_summary(self, _start, _end):
            return [{"trade_date": target, "exchanges": "BSE,SSE,SZSE"}]

        async def close(self):
            return None

    async def fake_to_thread(func, *args):
        return func(*args)

    async def fake_exchange_dates(exchange, _dates, concurrency=3):
        return ([{"exchange": exchange, "trade_date": target.isoformat()}], [])

    async def fake_refresh(_db, trade_dates):
        refreshed_dates.extend(trade_dates)
        return 6

    monkeypatch.setattr(margin_trading, "DbTools", FakeDb)
    monkeypatch.setattr(
        margin_trading,
        "_load_trade_dates",
        lambda _db, _start, _end: asyncio.sleep(0, result=[target]),
    )
    monkeypatch.setattr(margin_trading, "fetch_sse_rows_sync", lambda _start, _end: [])
    monkeypatch.setattr(margin_trading.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(margin_trading, "_fetch_exchange_dates", fake_exchange_dates)
    monkeypatch.setattr(quant_index, "refresh_trade_dates", fake_refresh)

    result = asyncio.run(margin_trading.sync_daily(target))

    assert result["status"] == "SUCCESS"
    assert result["dashboard_affected"] == 6
    assert refreshed_dates == [target]
