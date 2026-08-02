from akshare_project.collectors import index
from akshare_project.services import stock_temp_service


def test_build_cn_market_fear_greed_rows_validates_and_sorts():
    payload = {
        "records": [
            {
                "trade_date": "2026-07-27",
                "index_value": 32.41,
                "status_label": "中性",
                "locked": False,
            },
            {
                "trade_date": "2026-07-24",
                "index_value": "16.1",
                "status_label": "恐惧",
                "locked": True,
            },
            {"trade_date": "2026-07-23", "index_value": 101},
        ]
    }

    rows = index.build_cn_market_fear_greed_rows(payload)

    assert [row["trade_date"] for row in rows] == ["2026-07-24", "2026-07-27"]
    assert rows[0]["fear_greed_value"] == 16.1
    assert rows[0]["sentiment_label"] == "恐惧"
    assert rows[0]["locked"] is True
    assert rows[1]["data_source"] == "miumiu_market_fear_greed"
    assert rows[1]["raw_json"]["index_value"] == 32.41


def test_fetch_cn_market_fear_greed_history_retries_stale_response(monkeypatch):
    class FakeResponse:
        def __init__(self, trade_date):
            self.trade_date = trade_date

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "records": [
                    {
                        "trade_date": self.trade_date,
                        "index_value": 25,
                        "status_label": "恐惧",
                    }
                ]
            }

    responses = iter([FakeResponse("2026-07-23"), FakeResponse("2026-07-30")])
    request_params = []

    def fake_get(*_args, **kwargs):
        request_params.append(kwargs)
        return next(responses)

    monkeypatch.setattr(index.requests, "get", fake_get)
    monkeypatch.setattr(index.time, "sleep", lambda *_args: None)

    payload = index.fetch_cn_market_fear_greed_history(
        expected_date="2026-07-30",
        max_attempts=2,
    )

    assert payload["records"][-1]["trade_date"] == "2026-07-30"
    assert len(request_params) == 2
    assert request_params[0]["params"]["_"] != request_params[1]["params"]["_"]
    assert request_params[0]["headers"]["Cache-Control"] == "no-cache"


def test_fetch_cn_market_fear_greed_history_keeps_best_retry(monkeypatch):
    class FakeResponse:
        def __init__(self, trade_date):
            self.trade_date = trade_date

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "records": [
                    {
                        "trade_date": self.trade_date,
                        "index_value": 25,
                        "status_label": "恐惧",
                    }
                ]
            }

    responses = iter([FakeResponse("2026-07-29"), FakeResponse("2026-07-23")])
    monkeypatch.setattr(index.requests, "get", lambda *_args, **_kwargs: next(responses))
    monkeypatch.setattr(index.time, "sleep", lambda *_args: None)

    payload = index.fetch_cn_market_fear_greed_history(
        expected_date="2026-07-30",
        max_attempts=2,
    )

    assert payload["records"][-1]["trade_date"] == "2026-07-29"


def test_stock_temp_service_registers_cn_market_fear_greed_route():
    route = stock_temp_service.build_daily_routes()["/collect-index-cn-market-fear-greed-daily"]

    assert route.task_name == "index_cn_market_fear_greed_daily"
    assert route.direct_network is True
