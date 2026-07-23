import pandas as pd

from akshare_project.collectors.fund_purchase_limit import (
    build_historical_fund_rows,
    build_fund_purchase_limit_rows,
    carry_forward_market_wide_pause_flags,
    classify_purchase_status,
    detect_market_wide_purchase_pause,
    is_eligible_a_share_equity_fund,
    normalize_fund_product_name,
)
from akshare_project.collectors.quant_index import build_dashboard_rows


def test_normalize_fund_product_name_merges_share_classes():
    assert normalize_fund_product_name("测试成长混合A") == "测试成长混合"
    assert normalize_fund_product_name("测试成长混合 C") == "测试成长混合"
    assert normalize_fund_product_name("测试成长混合人民币A") == "测试成长混合"


def test_classify_historical_purchase_statuses():
    assert classify_purchase_status("限制大额申购") == {
        "limited_flag": 1,
        "limited_large_flag": 1,
        "suspended_purchase_flag": 0,
    }
    assert classify_purchase_status("暂停大额申购") == {
        "limited_flag": 1,
        "limited_large_flag": 1,
        "suspended_purchase_flag": 0,
    }
    assert classify_purchase_status("暂停申购") == {
        "limited_flag": 0,
        "limited_large_flag": 0,
        "suspended_purchase_flag": 1,
    }
    assert classify_purchase_status("开放申购")["limited_flag"] == 0


def test_market_wide_pause_detection_uses_product_ratio():
    rows = [
        {
            "product_key": f"product-{index}",
            "suspended_purchase_flag": 1 if index < 10 else 0,
        }
        for index in range(100)
    ]
    result = detect_market_wide_purchase_pause(rows)

    assert result["detected"] is True
    assert result["total_products"] == 100
    assert result["suspended_products"] == 10
    assert result["suspended_pct"] == 10.0


def test_market_wide_pause_detection_catches_just_below_ten_percent():
    rows = [
        {
            "product_key": f"product-{index}",
            "suspended_purchase_flag": 1 if index < 9 else 0,
        }
        for index in range(100)
    ]

    assert detect_market_wide_purchase_pause(rows)["detected"] is True


def test_market_wide_pause_carries_only_previous_large_limit_state():
    rows = [
        {
            "fund_code": "000001",
            "product_key": "product-1",
            "limited_flag": 0,
            "limited_large_flag": 0,
            "suspended_purchase_flag": 1,
        },
        {
            "fund_code": "000002",
            "product_key": "product-2",
            "limited_flag": 0,
            "limited_large_flag": 0,
            "suspended_purchase_flag": 1,
        },
        {
            "fund_code": "000003",
            "product_key": "product-3",
            "limited_flag": 1,
            "limited_large_flag": 1,
            "suspended_purchase_flag": 0,
        },
    ]
    result = carry_forward_market_wide_pause_flags(
        rows,
        {"000001": 1, "000002": 0, "000003": 1},
        pause_ratio=0.5,
        min_products=1,
    )

    assert result["detected"] is True
    assert result["carried_rows"] == 1
    assert [row["limited_flag"] for row in rows] == [1, 0, 1]


def test_isolated_pause_does_not_carry_previous_limit_state():
    rows = [
        {
            "fund_code": "000001",
            "product_key": "product-1",
            "limited_flag": 0,
            "limited_large_flag": 0,
            "suspended_purchase_flag": 1,
        },
        {
            "fund_code": "000002",
            "product_key": "product-2",
            "limited_flag": 0,
            "limited_large_flag": 0,
            "suspended_purchase_flag": 0,
        },
    ]
    result = carry_forward_market_wide_pause_flags(
        rows,
        {"000001": 1},
        pause_ratio=0.75,
        min_products=1,
    )

    assert result["detected"] is False
    assert result["carried_rows"] == 0
    assert rows[0]["limited_flag"] == 0


def test_build_historical_rows_uses_real_status_and_skips_exchange_only():
    rows = build_historical_fund_rows(
        {
            "fund_code": "000190",
            "fund_name": "中银新回报混合A",
            "fund_type": "混合型-灵活",
        },
        [
            {"FSRQ": "2026-07-16", "SGZT": "限制大额申购", "SHZT": "开放赎回"},
            {"FSRQ": "2026-07-15", "SGZT": "开放申购", "SHZT": "开放赎回"},
            {"FSRQ": "2026-07-14", "SGZT": "场内交易", "SHZT": "场内交易"},
        ],
    )

    assert [row["trade_date"] for row in rows] == ["2026-07-16", "2026-07-15"]
    assert rows[0]["limited_flag"] == 1
    assert rows[1]["limited_flag"] == 0
    assert all(row["data_source"] == "eastmoney_f10_lsjz" for row in rows)


def test_a_share_equity_universe_excludes_non_a_share_funds():
    assert is_eligible_a_share_equity_fund("股票型", "测试成长股票A", "开放申购")
    assert not is_eligible_a_share_equity_fund("债券型-混合二级", "测试债券A", "限大额")
    assert not is_eligible_a_share_equity_fund("QDII", "测试全球股票", "暂停申购")
    assert not is_eligible_a_share_equity_fund("混合型-偏股", "测试沪港深混合", "限大额")
    assert not is_eligible_a_share_equity_fund("指数型-股票", "测试恒生指数A", "限大额")
    assert not is_eligible_a_share_equity_fund("指数型-股票", "测试ETF", "场内交易")
    assert not is_eligible_a_share_equity_fund(
        "指数型-股票",
        "测试ETF",
        "场内买入",
        "510050",
    )
    assert not is_eligible_a_share_equity_fund(
        "指数型-股票",
        "测试ETF",
        "暂停申购",
        "159919",
    )


def test_build_rows_keeps_raw_share_classes_and_marks_limit_status():
    snapshot = pd.DataFrame(
        [
            {
                "基金代码": "000001",
                "基金简称": "测试成长混合A",
                "2026-07-16-单位净值": "1.0",
                "申购状态": "限大额",
                "赎回状态": "开放赎回",
            },
            {
                "基金代码": "000002",
                "基金简称": "测试成长混合C",
                "2026-07-16-单位净值": "1.0",
                "申购状态": "开放申购",
                "赎回状态": "开放赎回",
            },
            {
                "基金代码": "000003",
                "基金简称": "测试货币A",
                "2026-07-16-单位净值": "1.0",
                "申购状态": "暂停申购",
                "赎回状态": "开放赎回",
            },
        ]
    )
    names = pd.DataFrame(
        [
            {"基金代码": "000001", "基金类型": "混合型-偏股"},
            {"基金代码": "000002", "基金类型": "混合型-偏股"},
            {"基金代码": "000003", "基金类型": "货币型-普通货币"},
        ]
    )

    source_date, rows = build_fund_purchase_limit_rows(snapshot, names)

    assert source_date.isoformat() == "2026-07-16"
    assert len(rows) == 2
    assert rows[0]["product_key"] == rows[1]["product_key"]
    assert rows[0]["limited_flag"] == 1
    assert rows[1]["limited_flag"] == 0
    assert '"申购状态": "限大额"' in rows[0]["raw_json"]


def test_dashboard_only_writes_fund_limit_metric_for_shanghai_index():
    rows = build_dashboard_rows(
        trade_dates=["2026-07-16"],
        index_code_map={},
        emotion_map={},
        index_close_map={},
        futures_close_map={},
        breadth_map={},
        fund_purchase_limit_map={
            "2026-07-16": {
                "fund_purchase_limit_count": 321,
                "fund_purchase_limit_total_count": 4321,
                "fund_purchase_limit_pct": 7.428836,
            }
        },
    )

    shanghai = next(item for item in rows if item["index_name"] == "上证指数")
    hs300 = next(item for item in rows if item["index_name"] == "沪深300")
    assert shanghai["fund_purchase_limit_count"] == 321
    assert shanghai["fund_purchase_limit_total_count"] == 4321
    assert shanghai["fund_purchase_limit_pct"] == 7.428836
    assert hs300["fund_purchase_limit_count"] is None
    assert hs300["fund_purchase_limit_total_count"] is None
    assert hs300["fund_purchase_limit_pct"] is None
