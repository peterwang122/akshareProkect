from datetime import date

import pandas as pd
import pytest

from akshare_project.collectors import macro
from akshare_project.services import stock_temp_service


def test_parse_sse_market_cap_uses_main_a_and_star_only():
    payload = {
        "result": [
            {"PRODUCT_CODE": "01", "TOTAL_VALUE": "510178.55", "NEGO_VALUE": "490242.18"},
            {"PRODUCT_CODE": "02", "TOTAL_VALUE": "998.63", "NEGO_VALUE": "699.19"},
            {"PRODUCT_CODE": "03", "TOTAL_VALUE": "158630.96", "NEGO_VALUE": "130464.26"},
            {"PRODUCT_CODE": "17", "TOTAL_VALUE": "669808.14", "NEGO_VALUE": "621405.63"},
        ]
    }

    row = macro.parse_sse_daily_payload(payload, "2026-07-10")

    assert row["total_market_cap_cny"] == pytest.approx(66_880_951_000_000)
    assert row["circulating_market_cap_cny"] == pytest.approx(62_070_644_000_000)


def test_parse_szse_market_cap_accepts_columns_with_units():
    frame = pd.DataFrame(
        [
            ["主板A股", "26,510,860,148,121.76", "23,322,977,428,323.18"],
            ["主板B股", "36,689,217,319.00", "36,673,256,714.11"],
            ["创业板A股", "20,323,790,391,894.67", "16,446,083,996,178.30"],
        ],
        columns=["证券类别", "总市值(元)", "流通市值(元)"],
    )

    row = macro.parse_szse_summary_frame(frame, "2026-07-10")

    assert row["total_market_cap_cny"] == pytest.approx(46_834_650_540_016.43)
    assert row["circulating_market_cap_cny"] == pytest.approx(39_769_061_424_501.48)


def test_chinabond_history_response_maps_ten_year(monkeypatch):
    class Response:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return [{"workTime": "2026-07-10", "tenYear": "1.74", "qxmc": "中债国债收益率曲线"}]

    class Session:
        @staticmethod
        def post(url, **kwargs):
            assert url == macro.CHINABOND_YIELD_URL
            assert kwargs["params"]["gjqx"] == "10"
            return Response()

    monkeypatch.setattr(macro, "direct_session", Session)

    rows = macro.fetch_chinamoney_rows_sync("2026-07-10", "2026-07-10")

    assert rows[0]["maturity_yield_pct"] == 1.74
    assert rows[0]["data_source"] == "chinabond_mof_government_curve"


def test_build_macro_rows_uses_released_gdp_and_deposit_lag():
    trade_dates = [date(2026, 4, 10), date(2026, 4, 21)]
    valuations = [
        {"trade_date": target, "index_code": code, "pe_ttm": pe}
        for target in trade_dates
        for code, pe in (("000300", 10), ("000852", 20))
    ]
    yields = [
        {"trade_date": target, "maturity_yield_pct": 2}
        for target in trade_dates
    ]
    market_caps = [
        {"trade_date": target, "exchange": exchange, "total_market_cap_cny": value}
        for target in trade_dates
        for exchange, value in (("SSE", 50), ("SZSE", 40), ("BSE", 10))
    ]
    gdp = [
        {"period_end": "2025-06-30", "release_date": "2025-07-15", "nominal_gdp_cny": 20},
        {"period_end": "2025-09-30", "release_date": "2025-10-15", "nominal_gdp_cny": 20},
        {"period_end": "2025-12-31", "release_date": "2026-01-15", "nominal_gdp_cny": 20},
        {"period_end": "2026-03-31", "release_date": "2026-04-17", "nominal_gdp_cny": 40},
    ]
    deposits = [
        {"period_end": "2026-03-31", "household_deposit_cny": 200},
    ]

    rows = macro.build_macro_indicator_rows(
        trade_dates, valuations, yields, market_caps, gdp, deposits,
    )

    assert rows[0]["gdp_period_end"] == "2025-12-31"
    assert rows[0]["household_deposit_cny"] is None
    assert rows[1]["gdp_period_end"] == "2026-03-31"
    assert rows[1]["household_deposit_cny"] == 200
    assert rows[1]["hs300_equity_bond_spread_pp"] == pytest.approx(8)
    assert rows[1]["csi1000_equity_bond_spread_pp"] == pytest.approx(3)
    assert rows[1]["buffett_indicator_pct"] == pytest.approx(100)
    assert rows[1]["household_deposit_market_cap_ratio_pct"] == pytest.approx(200)
    assert rows[1]["market_cap_source"] == "exchange_official"
    assert rows[1]["market_cap_adjustment_factor"] == 1
    assert rows[1]["gdp_source"] == "nbs_trailing_4q"


def test_build_macro_rows_bridges_aggregate_history_and_prefers_official():
    trade_dates = [date(2020, 1, 2), date(2022, 1, 4)]
    market_caps = [
        {
            "trade_date": "2020-01-02",
            "exchange": "A_AGGREGATE",
            "total_market_cap_cny": 100,
            "reference_gdp_cny": 50,
        },
        {
            "trade_date": "2022-01-04",
            "exchange": "A_AGGREGATE",
            "total_market_cap_cny": 100,
            "reference_gdp_cny": 60,
        },
        *[
            {
                "trade_date": "2022-01-04",
                "exchange": exchange,
                "total_market_cap_cny": value,
            }
            for exchange, value in (("SSE", 50), ("SZSE", 30), ("BSE", 10))
        ],
    ]

    rows = macro.build_macro_indicator_rows(
        trade_dates, [], [], market_caps, [], [],
    )

    assert rows[0]["a_share_total_market_cap_cny"] == pytest.approx(90)
    assert rows[0]["buffett_indicator_pct"] == pytest.approx(180)
    assert rows[0]["market_cap_source"] == "legulegu_adjusted_to_exchange_official"
    assert rows[0]["market_cap_adjustment_factor"] == pytest.approx(0.9)
    assert rows[0]["gdp_source"] == "legulegu_nbs_annual_reference"
    assert rows[1]["a_share_total_market_cap_cny"] == pytest.approx(90)
    assert rows[1]["market_cap_source"] == "exchange_official"


def test_csi1000_spread_starts_on_official_launch_date():
    trade_dates = [date(2014, 10, 16), date(2014, 10, 17)]
    valuations = [
        {"trade_date": target, "index_code": "000852", "pe_ttm": 50}
        for target in trade_dates
    ]
    yields = [
        {"trade_date": target, "maturity_yield_pct": 4}
        for target in trade_dates
    ]

    rows = macro.build_macro_indicator_rows(
        trade_dates, valuations, yields, [], [], [],
    )

    assert rows[0]["csi1000_pe_ttm"] is None
    assert rows[0]["csi1000_equity_bond_spread_pp"] is None
    assert rows[1]["csi1000_pe_ttm"] == 50
    assert rows[1]["csi1000_equity_bond_spread_pp"] == pytest.approx(-2)


def test_parse_pbc_legacy_savings_deposit_table():
    html = """
    <table>
      <tr><td>项目 Item</td><td>2014.01</td><td>2014.02</td></tr>
      <tr><td>储蓄存款 Savings Deposits</td><td>100</td><td>110</td></tr>
      <tr><td>活期储蓄 Demand Savings Deposits</td><td>40</td><td>45</td></tr>
      <tr><td>定期储蓄 Time Savings Deposits</td><td>60</td><td>65</td></tr>
    </table>
    """

    rows = macro.parse_pbc_household_deposit_table(html, "https://example.test/2014")

    assert [row["period_end"] for row in rows] == ["2014-01-31", "2014-02-28"]
    assert rows[0]["household_deposit_cny"] == 10_000_000_000
    assert rows[0]["demand_deposit_cny"] == 4_000_000_000
    assert rows[0]["time_other_deposit_cny"] == 6_000_000_000
    assert rows[0]["data_source"] == "pbc_legacy_savings_deposit"


def test_fetch_legulegu_macro_rows_keeps_reference_gdp(monkeypatch):
    class Response:
        def __init__(self, payload=None):
            self.text = '<meta name="_csrf" content="token">'
            self._payload = payload

        @staticmethod
        def raise_for_status():
            return None

        def json(self):
            return self._payload

    class Session:
        @staticmethod
        def get(url, **kwargs):
            if url == macro.LEGULEGU_MACRO_PAGE_URL:
                return Response()
            assert kwargs["params"]["token"] == "lg-token"
            return Response({"data": [{
                "date": "2005-04-08", "marketCap": 300000, "gdp": 160000,
            }, {
                "date": "2020-01-05", "marketCap": 600000, "gdp": 900000,
            }]})

    monkeypatch.setattr(macro, "direct_session", Session)
    monkeypatch.setattr(macro, "get_token_lg", lambda: "lg-token")

    rows = macro.fetch_legulegu_macro_rows_sync("2005-04-08", "2020-01-05")

    assert rows[0]["exchange"] == "A_AGGREGATE"
    assert rows[0]["total_market_cap_cny"] == 30_000_000_000_000
    assert rows[0]["reference_gdp_cny"] == 16_000_000_000_000
    assert rows[1]["trade_date"] == "2020-01-03"
    assert rows[1]["raw_json"]["date"] == "2020-01-05"


def test_stock_temp_service_registers_macro_daily_route():
    route = stock_temp_service.build_daily_routes()["/collect-cn-macro-daily"]

    assert route.task_name == "cn_macro_daily"
    assert route.direct_network is True
