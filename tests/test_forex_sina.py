import asyncio
from datetime import datetime, timedelta

import pandas as pd
import pytest

from akshare_project.collectors import forex


def _history_frame(symbol_code, dates):
    rows = []
    previous_close = None
    for offset, trade_date in enumerate(dates):
        open_price = 6.70 + offset * 0.01
        low_price = open_price - 0.02
        high_price = open_price + 0.03
        close_price = open_price + 0.01
        rows.append({
            forex.COL_CODE: symbol_code,
            forex.COL_NAME: forex.FOREX_SYMBOL_NAMES[symbol_code],
            forex.COL_DATE: trade_date,
            forex.COL_OPEN: open_price,
            forex.COL_LOW: low_price,
            forex.COL_HIGH: high_price,
            forex.COL_LATEST: close_price,
            forex.COL_AMPLITUDE: forex.calculate_amplitude(
                high_price,
                low_price,
                previous_close,
            ),
        })
        previous_close = close_price
    return pd.DataFrame(rows)


def test_parse_sina_history_response_maps_ohlc_order_and_amplitude():
    response_text = (
        "/* source */\n"
        'var _FX=("2026-08-06,6.7000,6.6800,6.7300,6.7100,|'
        '2026-08-07,6.7200,6.6900,6.7500,6.7400,");'
    )

    frame = forex.parse_sina_history_response(response_text, 'USDCNH')

    first_row = frame.to_dict('records')[0]
    assert pd.isna(first_row.pop(forex.COL_AMPLITUDE))
    assert first_row == {
        forex.COL_CODE: 'USDCNH',
        forex.COL_NAME: '美元兑离岸人民币',
        forex.COL_DATE: '2026-08-06',
        forex.COL_OPEN: 6.7,
        forex.COL_LOW: 6.68,
        forex.COL_HIGH: 6.73,
        forex.COL_LATEST: 6.71,
    }
    assert frame.iloc[1][forex.COL_AMPLITUDE] == pytest.approx(
        round((6.75 - 6.69) / 6.71 * 100, 6)
    )


def test_parse_sina_history_response_discards_invalid_ohlc_rows():
    response_text = (
        'var _FX=("2026-08-06,6.7000,6.7100,6.6900,6.7000,|'
        '2026-08-07,6.7200,6.6900,6.7500,6.7400,");'
    )

    frame = forex.parse_sina_history_response(response_text, 'USDCNH')

    assert frame[forex.COL_DATE].tolist() == ['2026-08-07']


def test_parse_sina_history_response_inverts_ohlc_without_breaking_extremes():
    response_text = 'var _FX=("2026-08-07,7.8000,7.7500,7.8500,7.7900,");'

    frame = forex.parse_sina_history_response(response_text, 'CNHEUR', invert=True)
    row = frame.iloc[0]

    assert row[forex.COL_OPEN] == round(1 / 7.8, 6)
    assert row[forex.COL_LATEST] == round(1 / 7.79, 6)
    assert row[forex.COL_LOW] == round(1 / 7.85, 6)
    assert row[forex.COL_HIGH] == round(1 / 7.75, 6)


def test_parse_sina_realtime_response_maps_current_ohlc_and_dxy():
    response_text = (
        'var hq_str_fx_susdcnh="18:00:01,6.7459,6.7463,6.7430,69,'
        '6.7421,6.7479,6.7410,6.7459,USDCNH,0.04,0.0029,0.001023,,,'
        ',,2026-08-10";\n'
        'var hq_str_DINIW="18:00:02,99.7375,99.7375,99.5996,2645,'
        '99.5692,99.8098,99.5453,99.7375,DXY,2026-08-10";'
    )

    frame = forex.parse_sina_realtime_response(response_text, ['USDCNH', 'UDI'])
    rows = {row[forex.COL_CODE]: row for row in frame.to_dict('records')}

    assert rows['USDCNH'][forex.COL_DATE] == '2026-08-10'
    assert rows['USDCNH'][forex.COL_OPEN] == 6.7421
    assert rows['USDCNH'][forex.COL_HIGH] == 6.7479
    assert rows['USDCNH'][forex.COL_LOW] == 6.741
    assert rows['USDCNH'][forex.COL_LATEST] == 6.7459
    assert rows['UDI'][forex.COL_OPEN] == 99.5692
    assert rows['UDI'][forex.COL_LATEST] == 99.7375


def test_parse_sina_realtime_response_rejects_missing_symbols():
    response_text = (
        'var hq_str_fx_susdcnh="18:00:01,6.7459,6.7463,6.7430,69,'
        '6.7421,6.7479,6.7410,6.7459,USDCNH,0.04,0.0029,0.001023,,,'
        ',,2026-08-10";'
    )

    with pytest.raises(ValueError, match='missing symbols: UDI'):
        forex.parse_sina_realtime_response(response_text, ['USDCNH', 'UDI'])


def test_sina_mapping_covers_daily_pairs_and_dxy():
    assert set(forex.DAILY_SYNC_SYMBOLS) <= set(forex.SINA_FOREX_SYMBOLS)
    assert forex.SINA_FOREX_SYMBOLS['UDI'] == 'DINIW'
    assert forex.SINA_FOREX_SYMBOLS['CNHEUR'] == 'fx_seurcnh'
    assert forex.SINA_INVERTED_SYMBOLS == {'CNHEUR'}


def test_build_rows_use_sina_source_labels():
    frame = _history_frame('USDCNH', ['2026-08-07'])

    pair_rows = forex.build_forex_daily_rows(frame, 'USDCNH')
    dxy_rows = forex.build_forex_daily_rows(
        frame,
        'UDI',
        forex.USD_INDEX_SYMBOL_NAME,
        data_source=forex.SINA_USD_INDEX_DATA_SOURCE,
    )

    assert pair_rows[0]['data_source'] == forex.SINA_FOREX_DATA_SOURCE
    assert dxy_rows[0]['data_source'] == forex.SINA_USD_INDEX_DATA_SOURCE


def test_daily_sync_excludes_unsettled_local_date(monkeypatch):
    today = datetime.now().date()
    dates = [
        (today - timedelta(days=2)).isoformat(),
        (today - timedelta(days=1)).isoformat(),
        today.isoformat(),
    ]

    class FakeDbTools:
        instances = []

        def __init__(self):
            self.basic_rows = []
            self.daily_rows = []
            self.closed = False
            self.instances.append(self)

        async def init_pool(self):
            return None

        async def upsert_forex_basic_info(self, rows):
            self.basic_rows = rows
            return len(rows)

        async def upsert_forex_daily_snapshots(self, rows):
            self.daily_rows = rows
            return len(rows)

        async def close(self):
            self.closed = True

    monkeypatch.setattr(forex, 'DbTools', FakeDbTools)
    monkeypatch.setattr(
        forex,
        'get_forex_history',
        lambda symbol_code: _history_frame(symbol_code, dates),
    )

    result = asyncio.run(forex.sync_daily_from_history(['USDCNH']))

    assert result == 2
    db = FakeDbTools.instances[0]
    assert [row['trade_date'] for row in db.daily_rows] == dates[:2]
    assert {row['data_source'] for row in db.daily_rows} == {
        forex.SINA_FOREX_DATA_SOURCE
    }
    assert db.closed is True


def test_intraday_sync_includes_current_local_date(monkeypatch):
    today = datetime.now().date()
    dates = [
        (today - timedelta(days=1)).isoformat(),
        today.isoformat(),
    ]

    class FakeDbTools:
        instances = []

        def __init__(self):
            self.daily_rows = []
            self.instances.append(self)

        async def init_pool(self):
            return None

        async def upsert_forex_basic_info(self, rows):
            return len(rows)

        async def upsert_forex_daily_snapshots(self, rows):
            self.daily_rows = rows
            return len(rows)

        async def close(self):
            return None

    monkeypatch.setattr(forex, 'DbTools', FakeDbTools)
    monkeypatch.setattr(
        forex,
        'get_forex_realtime',
        lambda symbol_codes: _history_frame(symbol_codes[0], dates),
    )

    result = asyncio.run(forex.sync_intraday_from_history(['USDCNH']))

    assert result == 1
    assert [row['trade_date'] for row in FakeDbTools.instances[0].daily_rows] == [dates[-1]]
    assert FakeDbTools.instances[0].daily_rows[0]['data_source'] == (
        forex.SINA_FOREX_INTRADAY_DATA_SOURCE
    )


def test_usd_index_intraday_includes_current_local_date(monkeypatch):
    today = datetime.now().date()
    dates = [
        (today - timedelta(days=1)).isoformat(),
        today.isoformat(),
    ]

    class FakeDbTools:
        instances = []

        def __init__(self):
            self.daily_rows = []
            self.instances.append(self)

        async def init_pool(self):
            return None

        async def upsert_forex_basic_info(self, rows):
            return len(rows)

        async def upsert_forex_daily_snapshots(self, rows):
            self.daily_rows = rows
            return len(rows)

        async def close(self):
            return None

    monkeypatch.setattr(forex, 'DbTools', FakeDbTools)
    monkeypatch.setattr(
        forex,
        'get_forex_realtime',
        lambda symbol_codes: _history_frame(symbol_codes[0], dates),
    )

    result = asyncio.run(forex.sync_usd_index_intraday())

    assert result == 1
    db_rows = FakeDbTools.instances[0].daily_rows
    assert [row['trade_date'] for row in db_rows] == [dates[-1]]
    assert {row['data_source'] for row in db_rows} == {
        forex.SINA_USD_INDEX_INTRADAY_DATA_SOURCE
    }


def test_unsupported_symbol_is_rejected_before_network_call():
    with pytest.raises(ValueError, match='does not support GBPCHF'):
        forex.fetch_sina_history_once('GBPCHF')
