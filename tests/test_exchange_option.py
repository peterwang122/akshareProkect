import asyncio

import pytest

from akshare_project.collectors import exchange_option


def test_parse_contract_trade_code_for_sse_and_szse():
    call = exchange_option.parse_contract_trade_code("510050C2607M02700")
    put = exchange_option.parse_contract_trade_code("159915P2412M001450")

    assert call == {
        "underlying_code": "510050",
        "option_type": "CALL",
        "contract_month": "2607",
        "strike_price": 2.7,
    }
    assert put == {
        "underlying_code": "159915",
        "option_type": "PUT",
        "contract_month": "2412",
        "strike_price": 1.45,
    }


def test_sse_stats_turnover_is_normalized_to_yuan():
    row = exchange_option._sse_stats_row(
        {
            "SECURITY_CODE": "510050",
            "SECURITY_ABBR": "上证50ETF华夏",
            "TRADE_DATE": "2026-07-02",
            "CONTRACT_VOLUME": "96",
            "TOTAL_MONEY": "69,171",
            "TOTAL_VOLUME": "1,321,975",
            "CALL_VOLUME": "725,523",
            "PUT_VOLUME": "596,452",
            "CP_RATE": "82.21",
            "LEAVES_QTY": "1,619,111",
            "LEAVES_CALL_QTY": "990,268",
            "LEAVES_PUT_QTY": "628,843",
        },
        "2026-07-02",
    )

    assert row["turnover_amount"] == 691_710_000
    assert row["put_call_volume_ratio"] == pytest.approx(0.8221)


def test_parse_sina_quote_payload_reads_daily_fields():
    payload = (
        'var hq_str_CON_OP_90007051="1,0.7927,0.8000,0.8469,1,28,0,'
        '3.1000,0.7920,0.7800,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'
        '0,0,0,0,0,0,2026-07-03 15:00:00,0,E0,EBS,159901,name,'
        '5.2,0.8965,0.7572,40,115026.00";'
    )

    result = exchange_option.parse_sina_quote_payload(payload)["90007051"]

    assert result["trade_date"] == "2026-07-03"
    assert result["bid1_volume"] == "1"
    assert result["bid1_price"] == "0.7927"
    assert result["ask1_price"] == "0.8469"
    assert result["ask1_volume"] == "1"
    assert result["open_price"] == "0.7800"
    assert result["close_price"] == "0.8000"
    assert result["high_price"] == "0.8965"
    assert result["low_price"] == "0.7572"
    assert result["volume"] == "40"
    assert result["turnover"] == "115026.00"


def test_fetch_sina_quotes_keeps_batch_separator_unescaped(monkeypatch):
    requested_urls = []

    class Response:
        text = (
            'var hq_str_CON_OP_10000001="1,0,1.0,0,0,0,0,3.0,0,0,'
            '0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'
            '2026-07-03 15:00:00,0,E0,EBS,510050,name,0,1.1,0.9,'
            '10,100.00";'
        )

        @staticmethod
        def raise_for_status():
            return None

    class Session:
        @staticmethod
        def get(url, **kwargs):
            requested_urls.append(url)
            assert "params" not in kwargs
            return Response()

    monkeypatch.setattr(exchange_option, "direct_session", Session)

    exchange_option.fetch_sina_quotes_sync(["10000001", "10000002"])

    assert requested_urls == [
        f"{exchange_option.SINA_QUOTE_URL}"
        "?list=CON_OP_10000001,CON_OP_10000002"
    ]


def test_validate_product_coverage_rejects_missing_product():
    rows = [
        {"exchange": exchange, "underlying_code": code}
        for exchange, products in exchange_option.EXCHANGE_OPTION_PRODUCTS.items()
        for code in products
        if code != "159901"
    ]

    with pytest.raises(RuntimeError, match="159901"):
        exchange_option.validate_product_coverage(rows, "2026-07-02")


def test_build_contract_info_keeps_listing_and_expiry_metadata():
    row = exchange_option.build_contract_info_row(
        exchange="SSE",
        contract_code="10000001",
        contract_trade_code="510050C1503M02200",
        contract_name="50ETF购3月2200",
        contract_unit="10000",
        listed_date="20150209",
        last_trade_date="20150325",
        exercise_date="20150325",
        expire_date="20150325",
        delivery_date="20150326",
        listing_reason="合约新挂",
    )

    assert row["underlying_code"] == "510050"
    assert row["option_type"] == "CALL"
    assert row["strike_price"] == 2.2
    assert row["listed_date"] == "2015-02-09"
    assert row["expire_date"] == "2015-03-25"
    assert row["contract_unit"] == 10000


def test_sina_history_explicit_null_is_a_completed_empty_history(monkeypatch):
    class Response:
        text = "/* redirect */\n(null);"

        @staticmethod
        def raise_for_status():
            return None

    class Session:
        @staticmethod
        def get(*args, **kwargs):
            return Response()

    monkeypatch.setattr(exchange_option, "direct_session", Session)

    assert exchange_option.fetch_sina_history_sync("SZSE", "90000001") == []


def test_parse_sse_official_history_includes_exact_turnover():
    rows = exchange_option.parse_sse_official_history_payload(
        {
            "kline": [
                [20260703, 0.2201, 0.2309, 0.2111, 0.2161, 199, 434672],
            ]
        },
        "10010971",
    )

    assert rows == [
        {
            "exchange": "SSE",
            "contract_code": "10010971",
            "trade_date": "2026-07-03",
            "open_price": 0.2201,
            "high_price": 0.2309,
            "low_price": 0.2111,
            "close_price": 0.2161,
            "volume": 199.0,
            "turnover": 434672.0,
            "data_source": "sse_official_dayk",
            "source_url": (
                f"{exchange_option.SSE_OFFICIAL_HISTORY_URL}/10010971"
            ),
            "raw_json": [
                20260703,
                0.2201,
                0.2309,
                0.2111,
                0.2161,
                199,
                434672,
            ],
        }
    ]


def test_parse_szse_official_history_includes_exact_turnover_and_zero_day():
    rows = exchange_option.parse_szse_official_history_payload(
        {
            "code": "0",
            "data": {
                "picupdata": [
                    [
                        "2026-07-03",
                        "0.5930",
                        "0.5930",
                        "0.5930",
                        "0.5930",
                        "0.0223",
                        "3.91",
                        1,
                        5930.0,
                    ],
                    [
                        "2026-07-04",
                        "0",
                        "0",
                        "0",
                        "0",
                        "0",
                        "0",
                        0,
                        0,
                    ],
                ]
            },
        },
        "90007641",
    )

    assert rows[0]["high_price"] == 0.593
    assert rows[0]["low_price"] == 0.593
    assert rows[0]["volume"] == 1
    assert rows[0]["turnover"] == 5930
    assert rows[1]["volume"] == 0
    assert rows[1]["turnover"] == 0


def test_history_rows_are_filtered_to_cn_trade_dates():
    rows = [
        {"trade_date": "2026-02-13", "close_price": 1},
        {"trade_date": "2026-02-14", "close_price": 2},
        {"trade_date": "invalid", "close_price": 3},
    ]

    assert exchange_option.filter_history_rows_to_trade_dates(
        rows,
        ["2026-02-13"],
    ) == [rows[0]]


def test_sync_daily_keeps_successful_exchange_rows_when_other_exchange_fails(
    monkeypatch,
):
    class FakeDb:
        def __init__(self):
            self.info_rows = []
            self.daily_rows = []
            self.stats_rows = []

        async def init_pool(self):
            return None

        async def ensure_exchange_option_tables(self):
            return None

        async def batch_exchange_option_contract_info(self, rows):
            self.info_rows.extend(rows)
            return len(rows)

        async def batch_exchange_option_contract_daily_data(self, rows):
            self.daily_rows.extend(rows)
            return len(rows)

        async def batch_exchange_option_daily_stats(self, rows):
            self.stats_rows.extend(rows)
            return len(rows)

        async def close(self):
            return None

    fake_db = FakeDb()
    monkeypatch.setattr(exchange_option, "DbTools", lambda: fake_db)

    sse_contracts = [
        exchange_option.build_contract_row(
            exchange="SSE",
            contract_code=str(10000000 + index),
            contract_trade_code=f"{underlying}C2607M03000",
            contract_name=f"{underlying} test",
            trade_date="2026-07-03",
        )
        for index, underlying in enumerate(
            exchange_option.EXCHANGE_OPTION_PRODUCTS["SSE"],
            start=1,
        )
    ]
    sse_stats = [
        {
            "exchange": "SSE",
            "underlying_code": underlying,
            "trade_date": "2026-07-03",
        }
        for underlying in exchange_option.EXCHANGE_OPTION_PRODUCTS["SSE"]
    ]

    monkeypatch.setattr(
        exchange_option,
        "fetch_sse_current_contract_rows_sync",
        lambda target: sse_contracts,
    )
    monkeypatch.setattr(
        exchange_option,
        "fetch_szse_current_contract_rows_sync",
        lambda target: (_ for _ in ()).throw(RuntimeError("SZSE unavailable")),
    )
    monkeypatch.setattr(
        exchange_option,
        "fetch_sse_stats_rows_sync",
        lambda target: sse_stats,
    )
    monkeypatch.setattr(
        exchange_option,
        "fetch_szse_stats_rows_sync",
        lambda target: (_ for _ in ()).throw(RuntimeError("SZSE unavailable")),
    )
    monkeypatch.setattr(
        exchange_option,
        "fetch_sina_quotes_sync",
        lambda codes: {
            code: {
                "trade_date": "2026-07-03",
                "close_price": "1.0",
            }
            for code in codes
        },
    )
    async def fake_official_target_rows(contract_rows, _target):
        return (
            {
                "SSE": [
                    {
                        **row,
                        "trade_date": "2026-07-03",
                        "close_price": 1,
                        "volume": 1,
                        "turnover": 1,
                    }
                    for row in contract_rows
                    if row.get("exchange") == "SSE"
                ],
                "SZSE": [],
            },
            {"SSE": [], "SZSE": []},
        )

    monkeypatch.setattr(
        exchange_option,
        "fetch_official_target_rows",
        fake_official_target_rows,
    )

    with pytest.raises(
        RuntimeError,
        match="已保留合约主表5行、合约日线5行、官方覆盖5行、统计5行",
    ):
        asyncio.run(exchange_option.sync_daily("2026-07-03"))

    assert len(fake_db.info_rows) == 5
    assert len(fake_db.daily_rows) == 10
    assert len(fake_db.stats_rows) == 5


def test_sync_daily_succeeds_when_product_stats_are_not_published(monkeypatch):
    class FakeDb:
        def __init__(self):
            self.daily_rows = []

        async def init_pool(self):
            return None

        async def ensure_exchange_option_tables(self):
            return None

        async def batch_exchange_option_contract_info(self, rows):
            return len(rows)

        async def batch_exchange_option_contract_daily_data(self, rows):
            self.daily_rows.extend(rows)
            return len(rows)

        async def batch_exchange_option_daily_stats(self, rows):
            return len(rows)

        async def close(self):
            return None

    fake_db = FakeDb()
    monkeypatch.setattr(exchange_option, "DbTools", lambda: fake_db)

    rows = {
        exchange: [
            exchange_option.build_contract_row(
                exchange=exchange,
                contract_code=f"{exchange}{index}",
                contract_trade_code=f"{underlying}C2607M03000",
                contract_name=f"{underlying} test",
                trade_date="2026-07-14",
            )
            for index, underlying in enumerate(products, start=1)
        ]
        for exchange, products in exchange_option.EXCHANGE_OPTION_PRODUCTS.items()
    }
    monkeypatch.setattr(
        exchange_option,
        "fetch_sse_current_contract_rows_sync",
        lambda target: rows["SSE"],
    )
    monkeypatch.setattr(
        exchange_option,
        "fetch_szse_current_contract_rows_sync",
        lambda target: rows["SZSE"],
    )
    monkeypatch.setattr(exchange_option, "fetch_sse_stats_rows_sync", lambda target: [])
    monkeypatch.setattr(exchange_option, "fetch_szse_stats_rows_sync", lambda target: [])
    monkeypatch.setattr(
        exchange_option,
        "fetch_sina_quotes_sync",
        lambda codes: {
            code: {"trade_date": "2026-07-14", "close_price": 1}
            for code in codes
        },
    )

    async def fake_official_target_rows(contract_rows, _target):
        return (
            {
                exchange: [
                    {
                        **row,
                        "trade_date": "2026-07-14",
                        "close_price": 1,
                        "volume": 1,
                        "turnover": 1,
                    }
                    for row in contract_rows
                    if row.get("exchange") == exchange
                ]
                for exchange in exchange_option.EXCHANGE_OPTION_PRODUCTS
            },
            {"SSE": [], "SZSE": []},
        )

    monkeypatch.setattr(
        exchange_option,
        "fetch_official_target_rows",
        fake_official_target_rows,
    )

    result = asyncio.run(exchange_option.sync_daily("2026-07-14"))

    assert result["status"] == "SUCCESS"
    assert result["stats_status"] == "source_pending"
    assert "期权产品覆盖不完整" in result["warnings"][0]
    assert len(fake_db.daily_rows) == 18


def test_sync_stats_daily_requires_complete_product_coverage(monkeypatch):
    class FakeDb:
        def __init__(self):
            self.stats_rows = []

        async def init_pool(self):
            return None

        async def ensure_exchange_option_tables(self):
            return None

        async def batch_exchange_option_daily_stats(self, rows):
            self.stats_rows.extend(rows)
            return len(rows)

        async def close(self):
            return None

    fake_db = FakeDb()
    monkeypatch.setattr(exchange_option, "DbTools", lambda: fake_db)
    monkeypatch.setattr(
        exchange_option,
        "fetch_sse_stats_rows_sync",
        lambda target: [
            {"exchange": "SSE", "underlying_code": code, "trade_date": "2026-07-14"}
            for code in exchange_option.EXCHANGE_OPTION_PRODUCTS["SSE"]
        ],
    )
    monkeypatch.setattr(exchange_option, "fetch_szse_stats_rows_sync", lambda target: [])

    with pytest.raises(RuntimeError, match="SZSE缺少"):
        asyncio.run(exchange_option.sync_stats_daily("2026-07-14"))

    assert len(fake_db.stats_rows) == 5
