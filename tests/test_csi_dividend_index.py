import asyncio

from akshare_project.collectors import index as index_collector
from akshare_project.collectors.index import (
    CSI_DIVIDEND_INDEX_CODE,
    CSI_DIVIDEND_INDEX_SOURCE,
    append_csi_dividend_index_basic_row,
    build_csi_dividend_index_daily_rows,
    drop_csi_index_perf_boundary_duplicate,
    sync_daily_csi_dividend_index,
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


def test_sync_daily_csi_dividend_reports_source_not_ready(monkeypatch):
    monkeypatch.setattr(
        index_collector,
        'fetch_csi_dividend_index_perf',
        lambda _start, _end: [{'tradeDate': '20260807', 'close': 5506.75}],
    )

    result = asyncio.run(sync_daily_csi_dividend_index('2026-08-10'))

    assert result['status'] == 'SOURCE_NOT_READY'
    assert result['target_date'] == '2026-08-10'
    assert result['latest_trade_date'] == '2026-08-07'


def test_sync_daily_csi_dividend_persists_target_date(monkeypatch):
    class FakeDbTools:
        instances = []

        def __init__(self):
            self.basic_rows = None
            self.daily_rows = None
            self.closed = False
            self.instances.append(self)

        async def init_pool(self):
            return None

        async def upsert_index_basic_info(self, rows):
            self.basic_rows = rows
            return len(rows)

        async def upsert_index_daily_snapshots(self, rows):
            self.daily_rows = rows
            return len(rows)

        async def close(self):
            self.closed = True

    monkeypatch.setattr(index_collector, 'DbTools', FakeDbTools)
    monkeypatch.setattr(
        index_collector,
        'fetch_csi_dividend_index_perf',
        lambda _start, _end: [
            {
                'tradeDate': '20260810',
                'open': 5496.74,
                'high': 5578.88,
                'low': 5492.15,
                'close': 5561.23,
                'change': 54.48,
                'changePct': 0.99,
                'tradingVol': 5214281528.0,
                'tradingValue': 569.97,
            }
        ],
    )

    result = asyncio.run(sync_daily_csi_dividend_index('2026-08-10'))

    assert result['status'] == 'SUCCESS'
    assert result['target_date'] == '2026-08-10'
    assert result['latest_trade_date'] == '2026-08-10'
    db = FakeDbTools.instances[0]
    assert db.basic_rows[0]['index_code'] == CSI_DIVIDEND_INDEX_CODE
    assert db.daily_rows[0]['index_code'] == CSI_DIVIDEND_INDEX_CODE
    assert db.closed is True


def test_stock_temp_exposes_csi_dividend_daily_route():
    from akshare_project.services import stock_temp_service

    route = stock_temp_service.build_daily_routes()['/collect-index-csi-dividend-daily']

    assert route.task_name == 'index_csi_dividend_daily'
    assert route.handler is sync_daily_csi_dividend_index
    assert route.direct_network is True
