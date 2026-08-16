import pandas as pd

from akshare_project.collectors import global_risk


def test_build_fred_asset_rows_skips_missing_values():
    rows = global_risk.build_fred_asset_rows(
        "observation_date,NASDAQSOX\n2025-01-02,5000.5\n2025-01-03,.\n",
        "SOX",
        "费城半导体指数",
        "NASDAQSOX",
    )

    assert len(rows) == 1
    assert rows[0]["trade_date"] == "2025-01-02"
    assert rows[0]["close_value"] == 5000.5
    assert rows[0]["available_at"].strftime("%Y-%m-%d %H:%M") == "2025-01-03 08:00"


def test_build_ishares_nav_rows_reads_historical_worksheet():
    payload = """
    <ss:Workbook xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
      <ss:Worksheet ss:Name="Historical"><ss:Table>
        <ss:Row><ss:Cell><ss:Data ss:Type="String">As Of</ss:Data></ss:Cell>
          <ss:Cell><ss:Data ss:Type="String">NAV per Share</ss:Data></ss:Cell></ss:Row>
        <ss:Row><ss:Cell><ss:Data ss:Type="String">Jan 03, 2025</ss:Data></ss:Cell>
          <ss:Cell><ss:Data ss:Type="Number">91.25</ss:Data></ss:Cell></ss:Row>
      </ss:Table></ss:Worksheet>
    </ss:Workbook>
    """

    rows = global_risk.build_ishares_nav_rows(
        payload, "IXN_NAV", "iShares全球科技ETF NAV", "239750"
    )

    assert len(rows) == 1
    assert rows[0]["trade_date"] == "2025-01-03"
    assert rows[0]["close_value"] == 91.25


def test_build_dataframe_asset_rows_preserves_ohlc():
    dataframe = pd.DataFrame([
        {"日期": "2025-01-02", "开盘": 4.1, "最高": 4.3, "最低": 4.0, "最新价": 4.2, "总量": 12}
    ])

    rows = global_risk.build_dataframe_asset_rows(
        dataframe, "COPPER_HG", "COMEX铜连续", "source", "https://example.com"
    )

    assert rows[0]["open_value"] == 4.1
    assert rows[0]["high_value"] == 4.3
    assert rows[0]["low_value"] == 4.0
    assert rows[0]["close_value"] == 4.2
    assert rows[0]["volume"] == 12


def test_build_csi_tech_index_rows_converts_trading_value_to_yuan():
    rows = global_risk.build_csi_tech_index_rows(
        [{
            "tradeDate": "20250102",
            "open": 100,
            "high": 103,
            "low": 99,
            "close": 102,
            "change": 2,
            "changePct": 2,
            "tradingVol": 12.5,
            "tradingValue": 345.67,
        }],
        "000993",
        "sh000993",
    )

    assert rows[0]["trade_date"] == "2025-01-02"
    assert rows[0]["turnover"] == 34_567_000_000
    assert rows[0]["data_source"] == "csindex_official_index_perf"


def test_build_peakstone_turnover_concentration_rows_reads_public_top5_series():
    payload = """
    <html><body>
      <script id="panel-data" type="application/json">
        {"concentration":{"dates":["20260813","20260814"],"values":[49.46,48.29]}}
      </script>
    </body></html>
    """

    rows = global_risk.build_peakstone_turnover_concentration_rows(payload)

    assert [row["trade_date"] for row in rows] == ["2026-08-13", "2026-08-14"]
    assert rows[-1]["top5_pct"] == 48.29
    assert rows[-1]["top5_data_source"] == "peakstone_top5_turnover_concentration_ma5"


def test_build_top1_turnover_concentration_rows_keeps_raw_and_ma5():
    source_rows = [
        {
            "trade_date": f"2026-08-{day:02d}",
            "stock_count": 5000,
            "top1_stock_count": 50,
            "total_turnover_amount": 1_000_000_000_000,
            "top1_turnover_amount": ratio * 10_000_000_000,
        }
        for day, ratio in zip(range(10, 15), (20, 21, 22, 23, 24))
    ]

    rows = global_risk.build_top1_turnover_concentration_rows(source_rows)

    assert rows[0]["top1_pct"] is None
    assert rows[-1]["top1_raw_pct"] == 24.0
    assert rows[-1]["top1_pct"] == 22.0
    assert rows[-1]["top1_stock_count"] == 50
