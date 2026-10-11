import asyncio
from contextlib import nullcontext
from datetime import date, datetime

import pytest

from akshare_project.services import stock_temp_service


def test_persistent_runner_starts_without_coze(monkeypatch):
    captured = {}

    class FakeLoop:
        def run_forever(self):
            captured["ran_loop"] = True

        def close(self):
            captured["closed_loop"] = True

    def fake_session(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(stock_temp_service.asyncio, "new_event_loop", FakeLoop)
    monkeypatch.setattr(stock_temp_service.asyncio, "set_event_loop", lambda _loop: None)
    monkeypatch.setattr(
        stock_temp_service.douyin_emotion, "PersistentDouyinBrowserSession", fake_session
    )
    runner = stock_temp_service.DouyinPersistentBrowserRunner()
    runner._thread_main()

    assert captured == {
        "headless": True,
        "show_coze": False,
        "ran_loop": True,
        "closed_loop": True,
    }
    assert runner._ready.is_set()


def test_persistent_runner_keeps_existing_browser_reuse_options():
    captured = {}

    class FakeSession:
        async def run(self, target_date, **kwargs):
            captured["target_date"] = target_date
            captured.update(kwargs)
            return {"status": "NO_UPDATE"}

    runner = stock_temp_service.DouyinPersistentBrowserRunner()
    runner._session = FakeSession()
    result = asyncio.run(runner._run("2026-10-08", True, "2026-10-08T22:00:00"))

    assert captured["target_date"] == date(2026, 10, 8)
    assert captured["keep_browser_open"] is True
    assert captured["close_at"] == datetime(
        2026, 10, 8, 22, tzinfo=stock_temp_service.douyin_emotion.SHANGHAI_TZ
    )
    assert result["status"] == "NO_UPDATE"


def _handler(monkeypatch, runner):
    monkeypatch.setattr(stock_temp_service, "DOUYIN_BROWSER_RUNNER", runner)
    monkeypatch.setattr(stock_temp_service, "without_proxy_env", nullcontext)
    monkeypatch.setattr(stock_temp_service, "print", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        stock_temp_service.douyin_local_asr, "runtime_readiness",
        lambda: {"extraction_engine": "local_turbo_asr", "asr_ready": True},
    )
    handler = object.__new__(stock_temp_service.StockTempHandler)
    response = {}
    handler._send_json = lambda code, payload: response.update(code=code, payload=payload)
    return handler, response


@pytest.mark.parametrize(
    "result_status",
    ["NO_UPDATE", "UPDATE_FOUND_RETRYABLE", "UPDATE_FOUND_FAILED", "PARTIAL", "SUCCESS", "RECOVERED", "ALREADY_PROCESSED"],
)
def test_legacy_route_returns_local_pipeline_result(monkeypatch, result_status):
    captured = {}

    class FakeRunner:
        def run(self, **kwargs):
            captured.update(kwargs)
            health = stock_temp_service.build_health_payload()
            assert health["active_daily_request_count"] == 1
            assert health["active_daily_requests"][0]["task_name"] == "douyin_coze_emotion_daily"
            return {"status": result_status, "extraction_method": "local_asr_turbo+local_parser"}

    handler, response = _handler(monkeypatch, FakeRunner())
    route = stock_temp_service.build_daily_routes()["/collect-douyin-coze-emotion-daily"]
    payload = {
        "target_date": "2026-10-08",
        "keep_browser_open": True,
        "browser_close_at": "2026-10-08T22:00:00+08:00",
    }
    handler._run_daily_route(route, payload)

    assert route.task_name == "douyin_coze_emotion_daily"
    assert route.direct_network is True
    assert captured == payload
    assert response["code"] == 200
    assert response["payload"]["result"]["status"] == result_status
    assert response["payload"]["task_name"] == route.task_name
    assert stock_temp_service.build_health_payload()["active_request_count"] == 0


@pytest.mark.parametrize(
    "failure",
    [
        stock_temp_service.douyin_emotion.AuthenticationRequiredError("抖音登录已失效"),
        RuntimeError("抖音情绪采集已有实例正在运行"),
    ],
)
def test_route_exposes_login_and_single_instance_failure(monkeypatch, failure):
    class FakeRunner:
        def run(self, **_kwargs):
            raise failure

    handler, response = _handler(monkeypatch, FakeRunner())
    route = stock_temp_service.build_daily_routes()["/collect-douyin-coze-emotion-daily"]
    handler._run_daily_route(route, {"target_date": "2026-10-08"})

    assert response["code"] == 500
    assert response["payload"]["status"] == "FAILED"
    assert response["payload"]["error"] == str(failure)
    assert "result" not in response["payload"]
    assert stock_temp_service.build_health_payload()["active_request_count"] == 0


def test_route_rejects_unrecognized_fields_before_browser_use(monkeypatch):
    class FakeRunner:
        def run(self, **_kwargs):
            pytest.fail("invalid requests must not start the browser")

    handler, response = _handler(monkeypatch, FakeRunner())
    route = stock_temp_service.build_daily_routes()["/collect-douyin-coze-emotion-daily"]
    handler._run_daily_route(route, {"media_url": "https://untrusted.example/video.mp4"})

    assert response["code"] == 400
    assert response["payload"]["status"] == "INVALID_REQUEST"
    assert "media_url" in response["payload"]["error"]


def test_health_reports_only_readiness_and_request_metadata(monkeypatch):
    monkeypatch.setattr(
        stock_temp_service.douyin_local_asr, "runtime_readiness",
        lambda: {"extraction_engine": "local_turbo_asr", "asr_ready": False},
    )
    with stock_temp_service.track_service_request("stock_hfq_request"):
        health = stock_temp_service.build_health_payload()
        assert health["active_request_count"] == 1
        assert health["active_daily_request_count"] == 0
        assert health["active_requests"][0]["task_name"] == "stock_hfq_request"
        assert health["douyin"] == {
            "extraction_engine": "local_turbo_asr", "asr_ready": False,
        }
    assert stock_temp_service.build_health_payload()["active_request_count"] == 0


def test_runner_cancels_background_task_on_total_timeout(monkeypatch):
    captured = {}

    class FakeFuture:
        def result(self, *, timeout):
            captured["timeout"] = timeout
            raise TimeoutError()

        def cancel(self):
            captured["cancelled"] = True

    def fake_submit(coroutine, loop):
        coroutine.close()
        return FakeFuture()

    runner = stock_temp_service.DouyinPersistentBrowserRunner()
    monkeypatch.setattr(runner, "_ensure_started", lambda: None)
    monkeypatch.setattr(stock_temp_service.asyncio, "run_coroutine_threadsafe", fake_submit)
    with pytest.raises(RuntimeError, match="抖音情绪采集总超时"):
        runner.run(target_date="2026-10-08")
    assert captured == {"timeout": 1800, "cancelled": True}
