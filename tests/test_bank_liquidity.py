import asyncio
from datetime import date, timedelta

import pytest
import requests

from akshare_project.collectors import bank_liquidity


def test_parse_chinamoney_frr_payload_keeps_official_values_and_publish_time():
    rows = bank_liquidity.parse_chinamoney_frr_payload(
        {
            "records": [
                {
                    "lfiProducDate": "2026-08-28",
                    "frValueMap": {
                        "FR001": "1.3700",
                        "FR007": "1.4100",
                        "FDR001": "1.3400",
                        "FDR007": "1.3800",
                    },
                }
            ]
        }
    )

    assert rows == [
        {
            "trade_date": "2026-08-28",
            "fr001_pct": 1.37,
            "fr007_pct": 1.41,
            "fdr001_pct": 1.34,
            "fdr007_pct": 1.38,
            "frr_source_date": "2026-08-28",
            "frr_available_at": "2026-08-28 11:30:00",
            "source_url_frr": bank_liquidity.CHINAMONEY_FRR_PAGE,
            "raw_frr_json": {
                "lfiProducDate": "2026-08-28",
                "frValueMap": {
                    "FR001": "1.3700",
                    "FR007": "1.4100",
                    "FDR001": "1.3400",
                    "FDR007": "1.3800",
                },
            },
        }
    ]


def test_request_with_retry_recovers_from_transient_connection_error(monkeypatch):
    calls = []

    class FakeSession:
        def request(self, method, url, **kwargs):
            calls.append((method, url, kwargs))
            if len(calls) == 1:
                raise requests.exceptions.ConnectionError("temporary disconnect")
            return self

        def raise_for_status(self):
            return None

    monkeypatch.setattr(bank_liquidity, "direct_session", lambda: FakeSession())
    monkeypatch.setattr(bank_liquidity.time, "sleep", lambda _seconds: None)

    response = bank_liquidity.request_with_retry("GET", "https://example.test/data")

    assert response is not None
    assert len(calls) == 2


def test_parse_closing_repo_payload_reads_dr_and_r_weighted_rates():
    payload = {
        "data": {"lastDate": "2026-08-28"},
        "records": [
            {"instrmntCd": "DR001", "wghtdAvgRepoRate": "1.3378"},
            {"instrmntCd": "DR007", "wghtdAvgRepoRate": "1.3859"},
        ],
    }

    row = bank_liquidity.parse_closing_repo_payload(payload, "DR")

    assert row["trade_date"] == "2026-08-28"
    assert row["dr001_weighted_pct"] == 1.3378
    assert row["dr007_weighted_pct"] == 1.3859
    assert row["closing_repo_available_at"] == "2026-08-28 22:00:00"


def test_parse_closing_repo_payload_rejects_date_only_weekend_response():
    assert bank_liquidity.parse_closing_repo_payload(
        {"data": {"lastDate": "2026-08-29"}, "records": []},
        "DR",
    ) is None


def test_parse_chinabond_history_html_reads_one_year_bank_and_government_yields():
    html = """
    <table id="gjqxData">
      <tr><td>中债国债收益率曲线</td><td>2026-08-28</td><td>-</td><td>-</td><td>1.2132</td></tr>
      <tr><td>中债商业银行普通债收益率曲线(AAA)</td><td>2026-08-28</td><td>-</td><td>-</td><td>1.4844</td></tr>
    </table>
    """

    rows = bank_liquidity.parse_chinabond_history_html(html)

    assert rows[0]["cgb_1y_yield_pct"] == 1.2132
    assert rows[0]["bank_bond_aaa_1y_yield_pct"] == 1.4844
    assert rows[0]["chinabond_available_at"] == "2026-08-28 17:30:00"


def test_parse_pbc_omo_article_supports_table_and_paragraph_operations():
    html = """
    <html><head>
      <meta name="ArticleTitle" content="公开市场业务交易公告 [2026]第168号">
      <meta name="PubDate" content="2026-08-28">
      <meta name="createDate" content="2026-08-28 09:00:00">
    </head><body><div id="zoom">
      <p>开展了200亿元7天期逆回购操作，具体情况如下：</p>
      <p>7天期逆回购操作情况</p>
      <table>
        <tr><th>期限</th><th>操作利率</th><th>投标量</th><th>中标量</th></tr>
        <tr><td>7天</td><td>1.40%</td><td>200亿元</td><td>200亿元</td></tr>
      </table>
      <p>同时，开展了3330亿元隔夜逆回购操作。</p>
    </div></body></html>
    """

    rows = bank_liquidity.parse_pbc_omo_article_html(
        html,
        "https://www.pbc.gov.cn/zhengcehuobisi/125475/2026082808585347002/index.html",
    )

    assert len(rows) == 2
    seven_day = next(row for row in rows if row["tenor_days"] == 7)
    overnight = next(row for row in rows if row["tenor_days"] == 1)
    assert seven_day["awarded_amount_cny"] == 20_000_000_000
    assert seven_day["operation_rate_pct"] == 1.4
    assert seven_day["maturity_date"] is None
    assert overnight["awarded_amount_cny"] == 333_000_000_000


def test_parse_pbc_omo_old_table_uses_headers_and_ignores_migration_create_date():
    html = """
    <html><head>
      <meta name="ArticleTitle" content="公开市场业务交易公告 [2016]第67号">
      <meta name="PubDate" content="2016-04-26">
      <meta name="createDate" content="2025-12-10 15:41:04">
    </head><body><div id="zoom">
      <p>人民银行以利率招标方式开展了逆回购操作。</p>
      <table>
        <tr><td>期限</td><td>交易量</td><td>中标利率</td></tr>
        <tr><td>7 天</td><td>1400 亿元</td><td>2.25%</td></tr>
      </table>
    </div></body></html>
    """

    rows = bank_liquidity.parse_pbc_omo_article_html(
        html,
        "https://www.pbc.gov.cn/zhengcehuobisi/125475/3053258/index.html",
    )

    assert rows[0]["awarded_amount_cny"] == 140_000_000_000
    assert rows[0]["operation_rate_pct"] == 2.25
    assert rows[0]["published_at"].strftime("%Y-%m-%d %H:%M") == "2016-04-26 09:20"


def test_parse_pbc_omo_explicit_no_reverse_repo_operation():
    html = """
    <html><head>
      <meta name="ArticleTitle" content="公开市场业务交易公告 [2018]第75号">
      <meta name="PubDate" content="2018-04-20">
    </head><body>
      <span id="shijian">2018-04-20 09:10:13</span>
      <div id="zoom">
        <p>人民银行开展中央国库现金管理商业银行定期存款操作800亿元，无逆回购操作。</p>
      </div>
    </body></html>
    """

    rows = bank_liquidity.parse_pbc_omo_article_html(
        html,
        "https://www.pbc.gov.cn/zhengcehuobisi/125475/3523741/index.html",
    )

    assert len(rows) == 1
    assert rows[0]["operation_date"] == "2018-04-20"
    assert rows[0]["awarded_amount_cny"] == 0
    assert rows[0]["no_operation"] is True
    assert rows[0]["published_at"].strftime("%Y-%m-%d %H:%M:%S") == "2018-04-20 09:10:13"


def test_parse_pbc_omo_explicit_zero_operation_volume():
    rows = bank_liquidity.parse_pbc_omo_article_html(
        """
        <html><body><div id="zoom">
          <p>文章来源：2024-08-07 09:20:30</p>
          <p>2024年8月7日逆回购操作量为零。</p>
        </div></body></html>
        """,
        "https://www.pbc.gov.cn/example/5425164/index.html",
        fallback_date="2024-08-07",
    )

    assert len(rows) == 1
    assert rows[0]["tool_type"] == "reverse_repo"
    assert rows[0]["no_operation"] is True
    assert rows[0]["awarded_amount_cny"] == 0


def test_parse_pbc_omo_keeps_mlf_out_of_reverse_repo():
    rows = bank_liquidity.parse_pbc_omo_article_html(
        """
        <html><body><div id="zoom">
          <p>文章来源：2024-07-15 09:20:30</p>
          <p>开展1290亿元逆回购操作。此外，开展1000亿元中期借贷便利（MLF）操作。</p>
          <p>逆回购操作情况</p>
          <table>
            <tr><td>期限</td><td>中标量</td><td>中标利率</td></tr>
            <tr><td>7天</td><td>1290亿元</td><td>1.80%</td></tr>
          </table>
          <p>MLF操作情况</p>
          <table>
            <tr><td>期限</td><td>操作量</td><td>中标利率</td></tr>
            <tr><td>1年</td><td>1000亿元</td><td>2.50%</td></tr>
          </table>
        </div></body></html>
        """,
        "https://www.pbc.gov.cn/example/5403927/index.html",
        fallback_date="2024-07-15",
    )

    assert [(row["tool_type"], row["tenor_days"]) for row in rows] == [
        ("reverse_repo", 7),
        ("mlf", 365),
    ]


def test_parse_pbc_central_bank_bill_article_is_not_reverse_repo():
    html = """
    <html><head>
      <meta name="ArticleTitle" content="公开市场业务交易公告 [2026]第165号">
      <meta name="PubDate" content="2026-08-25">
    </head><body><div id="zoom">
      <p>人民银行通过香港金融管理局发行央行票据。</p>
      <table>
        <tr><td>期次</td><td>发行量</td><td>期限</td><td>中标利率</td></tr>
        <tr><td>第七期央行票据</td><td>150亿元</td><td>3个月</td><td>1.30%</td></tr>
      </table>
    </div></body></html>
    """

    assert bank_liquidity.parse_pbc_omo_article_html(
        html,
        "https://www.pbc.gov.cn/zhengcehuobisi/125475/bill/index.html",
    ) == []


def test_resolve_operation_maturity_dates_uses_official_market_days():
    operations = [
        {
            "operation_date": "2026-08-28",
            "tenor_days": 1,
            "no_operation": False,
        },
        {
            "operation_date": "2026-08-28",
            "tenor_days": 7,
            "no_operation": False,
        },
    ]

    rows = bank_liquidity.resolve_operation_maturity_dates(
        operations,
        ["2026-08-28", "2026-08-31", "2026-09-01", "2026-09-04"],
    )

    assert rows[0]["maturity_date"] == "2026-08-31"
    assert rows[1]["maturity_date"] == "2026-09-04"


def test_append_open_market_fields_carries_policy_rate_from_before_history_start():
    rows = [
        {
            "trade_date": "2017-05-31",
            "fdr001_pct": 2.8,
            "fdr007_pct": 2.9,
        }
    ]
    operations = [
        {
            "operation_date": "2017-05-25",
            "tool_type": "reverse_repo",
            "tenor_days": 7,
            "operation_rate_pct": 2.45,
            "awarded_amount_cny": 10_000_000_000,
            "maturity_date": None,
            "no_operation": False,
            "published_at": "2017-05-25 09:20:00",
            "source_url": "https://www.pbc.gov.cn/policy-source/index.html",
        }
    ]

    result = bank_liquidity.append_open_market_fields(rows, operations)

    assert result[0]["reverse_repo_7d_policy_rate_pct"] == 2.45
    assert result[0]["reverse_repo_7d_policy_source_date"] == "2017-05-25"
    assert result[0]["reverse_repo_7d_policy_available_at"] == "2017-05-25 09:20:00"
    assert result[0]["source_url_reverse_repo_7d_policy"].endswith("policy-source/index.html")


def test_append_open_market_fields_keeps_original_source_until_policy_rate_changes():
    rows = [
        {"trade_date": "2026-08-27", "fdr001_pct": 1.3, "fdr007_pct": 1.4},
        {"trade_date": "2026-08-28", "fdr001_pct": 1.3, "fdr007_pct": 1.4},
    ]
    operations = [
        {
            "operation_date": "2026-01-02",
            "tool_type": "reverse_repo",
            "tenor_days": 7,
            "operation_rate_pct": 1.4,
            "awarded_amount_cny": 10_000_000_000,
            "maturity_date": None,
            "no_operation": False,
            "published_at": "2026-01-02 09:20:00",
            "source_url": "https://www.pbc.gov.cn/rate-change/index.html",
        },
        {
            "operation_date": "2026-08-28",
            "tool_type": "reverse_repo",
            "tenor_days": 7,
            "operation_rate_pct": 1.4,
            "awarded_amount_cny": 20_000_000_000,
            "maturity_date": None,
            "no_operation": False,
            "published_at": "2026-08-28 09:20:00",
            "source_url": "https://www.pbc.gov.cn/daily-operation/index.html",
        },
    ]

    result = bank_liquidity.append_open_market_fields(rows, operations)

    assert result[-1]["reverse_repo_7d_policy_rate_pct"] == 1.4
    assert result[-1]["reverse_repo_7d_policy_source_date"] == "2026-01-02"
    assert result[-1]["source_url_reverse_repo_7d_policy"].endswith("rate-change/index.html")
    assert result[-1]["pbc_source_date"] == "2026-08-28"


def test_parse_pbc_monthly_keeps_two_reverse_repo_subitems():
    html = """
    <html><head>
      <meta name="ArticleTitle" content="2026年7月中央银行各项工具流动性投放情况">
      <meta name="PubDate" content="2026-08-04">
    </head><body><div id="zoom"><table>
      <tr><th>类别</th><th>工具</th><th>投放</th><th>回笼</th><th>净投放</th></tr>
      <tr><td>公开市场业务</td><td>7天期逆回购</td><td>47395</td><td>49890</td><td>-2495</td></tr>
      <tr><td>公开市场业务</td><td>其他期限逆回购</td><td>42000</td><td>35000</td><td>7000</td></tr>
    </table></div></body></html>
    """

    rows = bank_liquidity.parse_pbc_monthly_article_html(
        html,
        "https://www.pbc.gov.cn/zhengcehuobisi/5727710/example/index.html",
    )

    assert len(rows) == 2
    assert {row["tool_name"] for row in rows} == {"7天期逆回购", "其他期限逆回购"}
    assert {row["tool_type"] for row in rows} == {"reverse_repo"}
    assert rows[0]["period_end"] == "2026-07-31"


def test_midrank_percentile_requires_252_prior_values_and_handles_ties():
    assert bank_liquidity.midrank_percentile([1.0] * 251, 1.0) is None
    result = bank_liquidity.midrank_percentile([1.0] * 126 + [2.0] * 126, 1.0)
    assert result == pytest.approx(25.0)


def test_compute_liquidity_scores_excludes_current_and_requires_all_factors():
    start = date(2025, 1, 1)
    rows = []
    for index in range(253):
        rows.append(
            {
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "fdr007_pct": 1.5 + index / 10_000,
                "reverse_repo_7d_policy_rate_pct": 1.4,
                "fdr001_pct": 1.45 + index / 10_000,
                "fr007_pct": 1.55 + index / 10_000,
                "bank_bond_aaa_1y_yield_pct": 1.8 + index / 10_000,
                "cgb_1y_yield_pct": 1.3,
            }
        )

    calculated = bank_liquidity.compute_liquidity_scores(rows)

    assert calculated[251]["liquidity_tightness_score"] is None
    assert calculated[252]["liquidity_tightness_score"] is not None
    assert all(
        factor["prior_sample_count"] == 252
        for factor in calculated[252]["components_json"]["factors"].values()
    )
    incomplete = dict(rows[-1])
    incomplete["trade_date"] = "2027-01-01"
    incomplete["bank_bond_aaa_1y_yield_pct"] = None
    assert bank_liquidity.compute_liquidity_scores(rows[:-1] + [incomplete])[-1][
        "liquidity_tightness_score"
    ] is None


def test_sync_pbc_operations_retries_transient_network_errors(monkeypatch):
    item = {
        "source_url": "https://www.pbc.gov.cn/example/index.html",
        "source_date": "2026-08-28",
    }
    calls = []

    class FakeDb:
        async def get_cn_pbc_open_market_operations(self, _start, _end):
            return []

        async def upsert_cn_pbc_open_market_operations(self, rows):
            return len(rows)

    def fake_fetch(_item):
        calls.append(_item)
        if len(calls) < 3:
            raise requests.exceptions.SSLError("temporary eof")
        return [{"source_url": _item["source_url"]}]

    async def no_wait(_seconds):
        return None

    monotonic_values = iter(range(0, 100, 3))
    monkeypatch.setattr(bank_liquidity, "pbc_items_between_sync", lambda *_args: [item])
    monkeypatch.setattr(bank_liquidity, "fetch_pbc_omo_article_sync", fake_fetch)
    monkeypatch.setattr(bank_liquidity.asyncio, "sleep", no_wait)
    monkeypatch.setattr(bank_liquidity.time, "monotonic", lambda: next(monotonic_values))

    written = asyncio.run(
        bank_liquidity.sync_pbc_operations(
            FakeDb(),
            date(2026, 8, 28),
            date(2026, 8, 28),
        )
    )

    assert written == 1
    assert len(calls) == 3
