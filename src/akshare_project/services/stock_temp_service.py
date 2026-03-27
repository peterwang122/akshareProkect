import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

from akshare_project.collectors import stock
from akshare_project.core.logging_utils import echo_and_log, get_logger
from akshare_project.core.paths import ensure_runtime_layout, get_config_dir

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8786
LOGGER = get_logger("stock_temp_service")


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

    def do_GET(self):
        if self.path.rstrip("/") == "/health":
            self._send_json(200, build_health_payload())
            return
        self._send_json(404, {"status": "NOT_FOUND", "error": "unsupported path"})

    def do_POST(self):
        if self.path.rstrip("/") != "/collect":
            self._send_json(404, {"status": "NOT_FOUND", "error": "unsupported path"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            content_length = 0

        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._send_json(400, {"status": "INVALID_REQUEST", "error": "request body must be valid JSON"})
            return

        try:
            result = asyncio.run(
                stock.collect_qfq_for_request(
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
