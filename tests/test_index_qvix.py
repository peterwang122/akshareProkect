import asyncio

import pandas as pd

from akshare_project.collectors import index as index_collector
from akshare_project.collectors.index import build_qvix_daily_rows


def test_build_qvix_daily_rows_filters_weekends():
    rows = build_qvix_daily_rows(
        "50ETF_QVIX",
        pd.DataFrame(
            [
                {"date": "2026-05-15", "open": 18.16, "high": 18.21, "low": 17.43, "close": 17.48},
                {"date": "2026-05-16", "open": 18.45, "high": 18.70, "low": 17.68, "close": 17.82},
                {"date": "2026-05-18", "open": 18.45, "high": 18.70, "low": 17.68, "close": 17.82},
            ]
        ),
        "test",
    )

    assert [row["trade_date"] for row in rows] == ["2026-05-15", "2026-05-18"]


def test_build_qvix_daily_rows_normalizes_invalid_high_and_low():
    rows = build_qvix_daily_rows(
        "CYB_QVIX",
        pd.DataFrame(
            [
                {"date": "2026-05-08", "open": 26.59, "high": 28.31, "low": 27.00, "close": 31.00},
                {"date": "2026-05-11", "open": 29.94, "high": 32.91, "low": 33.70, "close": 32.37},
            ]
        ),
        "test",
    )

    assert rows[0]["high_price"] == 31.00
    assert rows[0]["low_price"] == 26.59
    assert rows[1]["high_price"] == 33.70
    assert rows[1]["low_price"] == 29.94


def test_process_qvix_index_upserts_existing_history(monkeypatch):
    daily_rows = [{"index_code": "50ETF_QVIX", "trade_date": "2026-01-05"}]

    monkeypatch.setattr(
        index_collector,
        "build_qvix_history_from_source",
        lambda *_args: pd.DataFrame([{"date": "2026-01-05"}]),
    )
    monkeypatch.setattr(
        index_collector,
        "build_qvix_daily_rows",
        lambda *_args: daily_rows,
    )

    class FakeDbTools:
        def __init__(self):
            self.rows = None

        async def upsert_index_qvix_daily_snapshots(self, rows):
            self.rows = rows
            return len(rows)

        async def batch_index_qvix_daily_data(self, _rows):
            raise AssertionError("history repair must not use insert-only method")

    db_tools = FakeDbTools()
    result = asyncio.run(
        index_collector.process_qvix_index(
            {
                "index_code": "50ETF_QVIX",
                "data_source": "test",
            },
            pd.DataFrame(),
            db_tools,
        )
    )

    assert result == 1
    assert db_tools.rows == daily_rows


def test_sync_daily_qvix_backfills_recent_source_rows(monkeypatch):
    lookback_rows = index_collector.QVIX_DAILY_RECENT_BACKFILL_ROWS
    daily_rows = [
        {"index_code": "50ETF_QVIX", "trade_date": f"2026-01-{day:02d}"}
        for day in range(1, lookback_rows + 6)
    ]
    fake_db_tools = None

    class FakeDbTools:
        def __init__(self):
            nonlocal fake_db_tools
            fake_db_tools = self
            self.rows = None

        async def init_pool(self):
            return None

        async def upsert_index_qvix_basic_info(self, rows):
            return len(rows)

        async def upsert_index_qvix_daily_snapshots(self, rows):
            self.rows = rows
            return len(rows)

        async def close(self):
            return None

    monkeypatch.setattr(index_collector, "DbTools", FakeDbTools)
    monkeypatch.setattr(index_collector, "fetch_qvix_daily_source", lambda: pd.DataFrame([{"date": "2026-01-20"}]))
    monkeypatch.setattr(
        index_collector,
        "QVIX_DEFINITIONS",
        [
            {
                "index_code": "50ETF_QVIX",
                "simple_code": "50ETF",
                "market": "cn",
                "index_name": "50ETF QVIX",
                "data_source": "test",
            }
        ],
    )
    monkeypatch.setattr(
        index_collector,
        "build_qvix_history_from_source",
        lambda *_args: pd.DataFrame([{"date": "2026-01-20"}]),
    )
    monkeypatch.setattr(
        index_collector,
        "build_qvix_daily_rows",
        lambda *_args: daily_rows,
    )

    result = asyncio.run(index_collector.sync_daily_qvix())

    assert result["upserted"] == lookback_rows
    assert fake_db_tools.rows == daily_rows[-lookback_rows:]
    assert result["trade_dates"] == [row["trade_date"] for row in daily_rows[-lookback_rows:]]
