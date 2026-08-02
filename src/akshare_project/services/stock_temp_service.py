import asyncio
import json
import threading
import time
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Awaitable, Callable, Dict

import requests

from akshare_project.collectors import (
    cffex,
    douyin_emotion,
    etf,
    exchange_option,
    excel_emotion,
    forex,
    fund_purchase_limit,
    futures,
    index,
    macro,
    margin_trading,
    option,
    option_minute,
    quant_index,
    risk_free_rate,
    stock,
)
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.network import without_proxy_env
from akshare_project.core.paths import ensure_runtime_layout, get_config_dir

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8786
LOGGER = get_logger("stock_temp_service")


AsyncHandler = Callable[[], Awaitable[object]]


@dataclass(frozen=True)
class DailyRoute:
    path: str
    task_name: str
    handler: AsyncHandler
    direct_network: bool = False


class DouyinPersistentBrowserRunner:
    def __init__(self):
        self._state_lock = threading.Lock()
        self._ready = threading.Event()
        self._thread = None
        self._loop = None
        self._session = None

    def _thread_main(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._session = douyin_emotion.PersistentDouyinBrowserSession(headless=True)
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            loop.close()

    def _ensure_started(self):
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._ready.clear()
            self._thread = threading.Thread(
                target=self._thread_main,
                name="douyin-persistent-browser",
                daemon=True,
            )
            self._thread.start()
        if not self._ready.wait(timeout=10):
            raise RuntimeError("抖音常驻浏览器后台线程启动超时")

    async def _run(self, target_date, keep_browser_open, browser_close_at):
        normalized_target_date = (
            date.fromisoformat(str(target_date))
            if target_date
            else datetime.now(douyin_emotion.SHANGHAI_TZ).date()
        )
        close_at = None
        if browser_close_at:
            close_at = datetime.fromisoformat(str(browser_close_at))
            if close_at.tzinfo is None:
                close_at = close_at.replace(tzinfo=douyin_emotion.SHANGHAI_TZ)
        return await self._session.run(
            normalized_target_date,
            keep_browser_open=bool(keep_browser_open),
            close_at=close_at,
        )

    def run(self, *, target_date=None, keep_browser_open=False, browser_close_at=None):
        self._ensure_started()
        future = asyncio.run_coroutine_threadsafe(
            self._run(target_date, keep_browser_open, browser_close_at),
            self._loop,
        )
        return future.result(timeout=1800)

    def close(self):
        with self._state_lock:
            thread = self._thread
            loop = self._loop
            session = self._session
        if thread is None or loop is None or not thread.is_alive():
            return
        try:
            asyncio.run_coroutine_threadsafe(session.close(), loop).result(timeout=30)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=10)
            with self._state_lock:
                self._thread = None
                self._loop = None
                self._session = None


DOUYIN_BROWSER_RUNNER = DouyinPersistentBrowserRunner()


def print(*args, **kwargs):
    echo_and_log(LOGGER, *args, **kwargs)


def load_service_config():
    config_path = get_config_dir() / "stock_temp_service.json"
    if not config_path.exists():
        return {"host": DEFAULT_HOST, "port": DEFAULT_PORT}
    with open(config_path, "r", encoding="utf-8") as file:
        payload = json.load(file)
    return {
        "host": str(payload.get("host", DEFAULT_HOST)).strip() or DEFAULT_HOST,
        "port": int(payload.get("port", DEFAULT_PORT)),
    }


def build_health_payload():
    config = load_service_config()
    return {
        "status": "ok",
        "service": "stock_temp_service",
        "host": config["host"],
        "port": config["port"],
        "thread": threading.current_thread().name,
    }


async def run_index_futures_handler_for_previous_trade_day(handler, market: str):
    previous_trade_date = await quant_index.resolve_market_previous_trade_date(market)
    if not previous_trade_date:
        raise RuntimeError(f"no previous trade date found for market={market}")
    result = await handler(trade_date=previous_trade_date)
    refresh_result = await quant_index.repair_market_previous_trade_day(market)
    return {
        "trade_date": previous_trade_date,
        "collection": result,
        "quant_index_refresh": refresh_result,
    }


async def run_hk_index_futures_handler(target_date=None):
    if target_date:
        if isinstance(target_date, datetime):
            resolved_trade_date = target_date.date()
        elif isinstance(target_date, date):
            resolved_trade_date = target_date
        else:
            resolved_trade_date = datetime.strptime(str(target_date), "%Y-%m-%d").date()
    else:
        previous_trade_date = await quant_index.resolve_market_previous_trade_date("hk")
        if not previous_trade_date:
            raise RuntimeError("no previous trade date found for market=hk")
        resolved_trade_date = datetime.strptime(str(previous_trade_date), "%Y-%m-%d").date()

    result = await futures.sync_hk_index_futures_daily(
        trade_date=resolved_trade_date,
        return_details=True,
    )
    if result.get("status") != "SUCCESS":
        result["quant_index_refresh"] = 0
        return result

    refresh_result = await quant_index.repair_market_previous_trade_day(
        "hk",
        reference_date=resolved_trade_date + timedelta(days=1),
    )
    result["quant_index_refresh"] = refresh_result
    return result


async def run_handler_for_previous_trade_day(handler, market: str):
    previous_trade_date = await quant_index.resolve_market_previous_trade_date(market)
    if not previous_trade_date:
        raise RuntimeError(f"no previous trade date found for market={market}")
    result = await handler(trade_date=previous_trade_date)
    return {
        "trade_date": previous_trade_date,
        "collection": result,
    }


def build_daily_routes() -> Dict[str, DailyRoute]:
    return {
        "/collect-index-us-daily": DailyRoute(
            path="/collect-index-us-daily",
            task_name="index_us_daily",
            handler=index.collect_us_indices_daily_for_service,
        ),
        "/collect-index-hk-daily": DailyRoute(
            path="/collect-index-hk-daily",
            task_name="index_hk_daily",
            handler=index.collect_hk_indices_daily_for_service,
        ),
        "/collect-stock-daily": DailyRoute(
            path="/collect-stock-daily",
            task_name="stock_daily",
            handler=stock.sync_daily,
            direct_network=True,
        ),
        "/collect-stock-exchange-official-daily": DailyRoute(
            path="/collect-stock-exchange-official-daily",
            task_name="stock_exchange_official_daily",
            handler=stock.sync_exchange_official_daily,
            direct_network=True,
        ),
        "/collect-index-cn-daily": DailyRoute(
            path="/collect-index-cn-daily",
            task_name="index_cn_daily",
            handler=index.sync_daily_from_spot,
            direct_network=True,
        ),
        "/collect-index-bj50-daily": DailyRoute(
            path="/collect-index-bj50-daily",
            task_name="index_bj50_daily",
            handler=index.sync_daily_special_index,
            direct_network=True,
        ),
        "/collect-cffex-daily": DailyRoute(
            path="/collect-cffex-daily",
            task_name="cffex_daily",
            handler=lambda: cffex.sync_latest_daily_data(headless=True),
            direct_network=True,
        ),
        "/collect-forex-daily": DailyRoute(
            path="/collect-forex-daily",
            task_name="forex_daily",
            handler=forex.sync_daily_from_history,
            direct_network=True,
        ),
        "/collect-usd-index-daily": DailyRoute(
            path="/collect-usd-index-daily",
            task_name="usd_index_daily",
            handler=forex.sync_usd_index_once,
        ),
        "/collect-futures-daily": DailyRoute(
            path="/collect-futures-daily",
            task_name="futures_daily",
            handler=futures.sync_today,
            direct_network=True,
        ),
        "/collect-us-index-futures-daily": DailyRoute(
            path="/collect-us-index-futures-daily",
            task_name="us_index_futures_daily",
            handler=lambda: run_index_futures_handler_for_previous_trade_day(futures.sync_us_index_futures_daily, "us"),
        ),
        "/collect-us-index-futures-official-daily": DailyRoute(
            path="/collect-us-index-futures-official-daily",
            task_name="us_index_futures_official_daily",
            handler=lambda: run_handler_for_previous_trade_day(
                futures.sync_us_index_futures_official_daily,
                "us",
            ),
        ),
        "/collect-hk-index-futures-daily": DailyRoute(
            path="/collect-hk-index-futures-daily",
            task_name="hk_index_futures_daily",
            handler=run_hk_index_futures_handler,
        ),
        "/collect-etf-daily": DailyRoute(
            path="/collect-etf-daily",
            task_name="etf_daily",
            handler=etf.sync_daily,
            direct_network=True,
        ),
        "/collect-option-daily": DailyRoute(
            path="/collect-option-daily",
            task_name="option_daily",
            handler=lambda: option.sync_daily(headless=True),
            direct_network=True,
        ),
        "/collect-exchange-option-daily": DailyRoute(
            path="/collect-exchange-option-daily",
            task_name="exchange_option_daily",
            handler=lambda target_date=None: exchange_option.sync_daily(target_date=target_date),
            direct_network=True,
        ),
        "/collect-exchange-option-stats-daily": DailyRoute(
            path="/collect-exchange-option-stats-daily",
            task_name="exchange_option_stats_daily",
            handler=lambda target_date=None: exchange_option.sync_stats_daily(
                target_date=target_date
            ),
            direct_network=True,
        ),
        "/collect-option-minute-daily": DailyRoute(
            path="/collect-option-minute-daily",
            task_name="option_minute_daily",
            handler=lambda target_date=None: option_minute.run_daily_session(
                target_date=target_date
            ),
            direct_network=True,
        ),
        "/collect-cn-risk-free-rate-daily": DailyRoute(
            path="/collect-cn-risk-free-rate-daily",
            task_name="cn_risk_free_rate_daily",
            handler=lambda target_date=None: risk_free_rate.sync_daily(
                target_date=target_date
            ),
            direct_network=True,
        ),
        "/collect-cn-macro-daily": DailyRoute(
            path="/collect-cn-macro-daily",
            task_name="cn_macro_daily",
            handler=lambda target_date=None: macro.sync_daily(target_date=target_date),
            direct_network=True,
        ),
        "/collect-margin-trading-daily": DailyRoute(
            path="/collect-margin-trading-daily",
            task_name="margin_trading_daily",
            handler=lambda target_date=None: margin_trading.sync_daily(
                target_date=target_date
            ),
            direct_network=True,
        ),
        "/collect-fund-purchase-limit-daily": DailyRoute(
            path="/collect-fund-purchase-limit-daily",
            task_name="fund_purchase_limit_daily",
            handler=lambda target_date=None: fund_purchase_limit.sync_daily(
                target_date=target_date
            ),
            direct_network=True,
        ),
        "/collect-quant-index-daily": DailyRoute(
            path="/collect-quant-index-daily",
            task_name="quant_index_daily",
            handler=quant_index.sync_daily,
            direct_network=True,
        ),
        "/collect-index-qvix-daily": DailyRoute(
            path="/collect-index-qvix-daily",
            task_name="index_qvix_daily",
            handler=index.sync_daily_qvix,
            direct_network=True,
        ),
        "/collect-index-news-sentiment-daily": DailyRoute(
            path="/collect-index-news-sentiment-daily",
            task_name="index_news_sentiment_daily",
            handler=index.sync_daily_news_sentiment_scope,
            direct_network=True,
        ),
        "/collect-index-cn-market-fear-greed-daily": DailyRoute(
            path="/collect-index-cn-market-fear-greed-daily",
            task_name="index_cn_market_fear_greed_daily",
            handler=lambda target_date=None: index.sync_daily_cn_market_fear_greed(
                target_date=target_date
            ),
            direct_network=True,
        ),
        "/import-emotion-excel": DailyRoute(
            path="/import-emotion-excel",
            task_name="excel_emotion_import",
            handler=lambda: excel_emotion.import_excel("情绪指标.xlsx"),
        ),
        "/collect-douyin-coze-emotion-daily": DailyRoute(
            path="/collect-douyin-coze-emotion-daily",
            task_name="douyin_coze_emotion_daily",
            handler=lambda target_date=None: douyin_emotion.sync_daily(target_date=target_date),
            direct_network=True,
        ),
        "/collect-index-us-vix-daily": DailyRoute(
            path="/collect-index-us-vix-daily",
            task_name="index_us_vix_daily",
            handler=index.sync_daily_us_vix_only,
        ),
        "/collect-index-us-fear-greed-daily": DailyRoute(
            path="/collect-index-us-fear-greed-daily",
            task_name="index_us_fear_greed_daily",
            handler=index.sync_daily_us_fear_greed_only,
        ),
        "/collect-index-us-hedge-proxy-daily": DailyRoute(
            path="/collect-index-us-hedge-proxy-daily",
            task_name="index_us_hedge_proxy_daily",
            handler=index.sync_daily_us_hedge_proxy,
        ),
        "/collect-index-us-put-call-ratio-daily": DailyRoute(
            path="/collect-index-us-put-call-ratio-daily",
            task_name="index_us_put_call_ratio_daily",
            handler=index.sync_daily_us_put_call_ratio_only,
        ),
        "/collect-index-us-treasury-yield-daily": DailyRoute(
            path="/collect-index-us-treasury-yield-daily",
            task_name="index_us_treasury_yield_daily",
            handler=index.sync_daily_us_treasury_yield_only,
        ),
        "/collect-index-us-credit-spread-daily": DailyRoute(
            path="/collect-index-us-credit-spread-daily",
            task_name="index_us_credit_spread_daily",
            handler=index.sync_daily_us_credit_spread_only,
        ),
    }


DAILY_ROUTES = build_daily_routes()


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_daily_success_payload(route: DailyRoute, started_at: str, finished_at: str, duration_seconds: float, result):
    return {
        "status": "SUCCESS",
        "task_name": route.task_name,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 3),
        "result": result,
    }


def build_daily_failed_payload(route: DailyRoute, started_at: str, finished_at: str, duration_seconds: float, error: str):
    return {
        "status": "FAILED",
        "task_name": route.task_name,
        "error": error,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 3),
    }


class StockTempHandler(BaseHTTPRequestHandler):
    server_version = "StockTempService/1.0"

    def log_message(self, format, *args):
        return

    def _send_json(self, status_code, payload):
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_payload(self):
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            content_length = 0

        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._send_json(400, {"status": "INVALID_REQUEST", "error": "request body must be valid JSON"})
            return None

        if not isinstance(payload, dict):
            self._send_json(400, {"status": "INVALID_REQUEST", "error": "request body must be a JSON object"})
            return None

        return payload

    def _run_daily_route(self, route: DailyRoute, payload: dict):
        payload_task_names = {
            "stock_exchange_official_daily",
            "douyin_coze_emotion_daily",
            "exchange_option_daily",
            "exchange_option_stats_daily",
            "option_minute_daily",
            "cn_risk_free_rate_daily",
            "cn_macro_daily",
            "margin_trading_daily",
            "fund_purchase_limit_daily",
            "index_cn_market_fear_greed_daily",
            "hk_index_futures_daily",
        }
        if payload and route.task_name not in payload_task_names:
            self._send_json(
                400,
                {
                    "status": "INVALID_REQUEST",
                    "error": "daily endpoints currently accept only an empty JSON object",
                },
            )
            return

        started_at = now_text()
        started_monotonic = time.perf_counter()
        try:
            context = without_proxy_env() if route.direct_network else nullcontext()
            with context:
                if route.task_name in payload_task_names:
                    allowed_keys = {"target_date"}
                    if route.task_name == "douyin_coze_emotion_daily":
                        allowed_keys.update({"keep_browser_open", "browser_close_at"})
                    unexpected_keys = sorted(set(payload) - allowed_keys)
                    if unexpected_keys:
                        self._send_json(
                            400,
                            {
                                "status": "INVALID_REQUEST",
                                "error": f"unsupported {route.task_name} fields: {unexpected_keys}",
                            },
                        )
                        return
                    if route.task_name == "douyin_coze_emotion_daily":
                        result = DOUYIN_BROWSER_RUNNER.run(
                            target_date=payload.get("target_date"),
                            keep_browser_open=payload.get("keep_browser_open", False),
                            browser_close_at=payload.get("browser_close_at"),
                        )
                    else:
                        result = asyncio.run(route.handler(target_date=payload.get("target_date")))
                else:
                    result = asyncio.run(route.handler())
        except Exception as exc:
            finished_at = now_text()
            duration_seconds = time.perf_counter() - started_monotonic
            print(f"daily collect failed [{route.task_name}]: {exc}")
            self._send_json(
                500,
                build_daily_failed_payload(
                    route=route,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration_seconds,
                    error=str(exc),
                ),
            )
            return

        finished_at = now_text()
        duration_seconds = time.perf_counter() - started_monotonic
        self._send_json(
            200,
            build_daily_success_payload(
                route=route,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration_seconds,
                result=result,
            ),
        )

    def do_GET(self):
        if self.path.rstrip("/") == "/health":
            self._send_json(200, build_health_payload())
            return
        self._send_json(404, {"status": "NOT_FOUND", "error": "unsupported path"})

    def do_POST(self):
        normalized_path = self.path.rstrip("/")
        if normalized_path not in {"/collect", "/collect-forex"} and normalized_path not in DAILY_ROUTES:
            self._send_json(404, {"status": "NOT_FOUND", "error": "unsupported path"})
            return

        payload = self._read_json_payload()
        if payload is None:
            return

        if normalized_path in DAILY_ROUTES:
            self._run_daily_route(DAILY_ROUTES[normalized_path], payload)
            return

        if normalized_path == "/collect-forex":
            try:
                result = asyncio.run(
                    forex.collect_symbol_history_for_request(
                        symbol_code=payload.get("symbol_code"),
                    )
                )
                self._send_json(200, result)
            except ValueError as exc:
                self._send_json(400, {"status": "INVALID_REQUEST", "error": str(exc)})
            except Exception as exc:
                print(f"forex collect failed: {exc}")
                self._send_json(500, {"status": "FAILED", "error": str(exc)})
            return

        try:
            with without_proxy_env():
                result = asyncio.run(
                    stock.collect_hfq_for_request(
                        stock_code=payload.get("stock_code"),
                        start_date=payload.get("start_date"),
                        end_date=payload.get("end_date"),
                    )
                )
            self._send_json(200, result)
        except ValueError as exc:
            self._send_json(400, {"status": "INVALID_REQUEST", "error": str(exc)})
        except Exception as exc:
            print(f"stock temp collect failed: {exc}")
            self._send_json(500, {"status": "FAILED", "error": str(exc)})


def run_stock_temp_service():
    ensure_runtime_layout()
    config = load_service_config()
    server = ThreadingHTTPServer((config["host"], int(config["port"])), StockTempHandler)
    print(f"stock temp service started at http://{config['host']}:{config['port']}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("stock temp service stopped by keyboard interrupt")
    finally:
        DOUYIN_BROWSER_RUNNER.close()
        server.server_close()


def run_healthcheck():
    config = load_service_config()
    base_url = f"http://{config['host']}:{config['port']}"
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        response.raise_for_status()
        payload = response.json()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    except Exception as exc:
        raise RuntimeError(
            "stock temp service is unavailable. "
            "Please start it first with: python stock_temp_service.py serve"
        ) from exc
