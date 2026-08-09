from akshare_project.collectors.index import (
    CSI_DIVIDEND_INDEX_CODE,
    CSI_DIVIDEND_INDEX_SOURCE,
    append_csi_dividend_index_basic_row,
    build_csi_dividend_index_daily_rows,
    drop_csi_index_perf_boundary_duplicate,
)


def test_append_csi_dividend_index_basic_row_adds_official_index():
    rows = append_csi_dividend_index_basic_row([
        {
            'index_code': 'sz399006',
            'simple_code': '399006',
            'market': 'sz',
            'index_name': '创业板指',
            'data_source': 'test',
        }
    ])

    by_code = {row['index_code']: row for row in rows}
    assert by_code[CSI_DIVIDEND_INDEX_CODE]['index_name'] == '中证红利'
    assert by_code[CSI_DIVIDEND_INDEX_CODE]['data_source'] == CSI_DIVIDEND_INDEX_SOURCE


def test_build_csi_dividend_index_daily_rows_maps_official_fields():
    rows = build_csi_dividend_index_daily_rows([
        {
            'tradeDate': '20260807',
            'open': 99.0,
            'high': 103.0,
            'low': 97.0,
            'close': 100.0,
            'change': 2.0,
            'changePct': 2.04,
            'tradingVol': 123456.0,
            'tradingValue': 599.69,
        }
    ])

    assert rows == [
        {
            'index_code': CSI_DIVIDEND_INDEX_CODE,
            'open_price': 99.0,
            'close_price': 100.0,
            'high_price': 103.0,
            'low_price': 97.0,
            'volume': 123456.0,
            'turnover': 59_969_000_000.0,
            'amplitude': 6.1224,
            'price_change_rate': 2.04,
            'price_change_amount': 2.0,
            'turnover_rate': None,
            'trade_date': '2026-08-07',
            'data_source': CSI_DIVIDEND_INDEX_SOURCE,
        }
    ]


def test_drop_csi_index_perf_boundary_duplicate_removes_synthetic_start_date():
    duplicate_snapshot = {
        'open': 996.38,
        'high': 996.38,
        'low': 979.59,
        'close': 981.56,
        'change': None,
        'changePct': -1.84,
        'tradingVol': 180302622,
        'tradingValue': 10.39,
    }
    rows = drop_csi_index_perf_boundary_duplicate(
        [
            {'tradeDate': '20050101', **duplicate_snapshot},
            {'tradeDate': '20050104', **duplicate_snapshot},
            {'tradeDate': '20050105', 'close': 986.54},
        ],
        '20050101',
    )

    assert [row['tradeDate'] for row in rows] == ['20050104', '20050105']
