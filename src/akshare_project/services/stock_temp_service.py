import asyncio
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Awaitable, Callable, Dict

import requests

from akshare_project.collectors import cffex, etf, forex, futures, index, option, quant_index, stock
from akshare_project.core.logging_utils import echo_and_log, get_logger
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
        ),
        "/collect-index-cn-daily": DailyRoute(
            path="/collect-index-cn-daily",
            task_name="index_cn_daily",
            handler=index.sync_daily_from_spot,
        ),
        "/collect-index-bj50-daily": DailyRoute(
            path="/collect-index-bj50-daily",
            task_name="index_bj50_daily",
            handler=index.sync_daily_special_index,
        ),
        "/collect-cffex-daily": DailyRoute(
            path="/collect-cffex-daily",
            task_name="cffex_daily",
            handler=lambda: cffex.sync_latest_daily_data(headless=True),
        ),
        "/collect-forex-daily": DailyRoute(
            path="/collect-forex-daily",
            task_name="forex_daily",
            handler=forex.sync_daily_from_history,
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
            handler=lambda: run_index_futures_handler_for_previous_trade_day(futures.sync_hk_index_futures_daily, "hk"),
        ),
        "/collect-etf-daily": DailyRoute(
            path="/collect-etf-daily",
            task_name="etf_daily",
            handler=etf.sync_daily,
        ),
        "/collect-option-daily": DailyRoute(
            path="/collect-option-daily",
            task_name="option_daily",
            handler=lambda: option.sync_daily(headless=True),
        ),
        "/collect-quant-index-daily": DailyRoute(
            path="/collect-quant-index-daily",
            task_name="quant_index_daily",
            handler=quant_index.sync_daily,
        ),
        "/collect-index-qvix-daily": DailyRoute(
            path="/collect-index-qvix-daily",
            task_name="index_qvix_daily",
            handler=index.sync_daily_qvix,
        ),
        "/collect-index-news-sentiment-daily": DailyRoute(
            path="/collect-index-news-sentiment-daily",
            task_name="index_news_sentiment_daily",
            handler=index.sync_daily_news_sentiment_scope,
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
        if payload:
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
