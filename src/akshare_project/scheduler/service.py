import json
import os
import random
import subprocess
import threading
import time
import builtins
import logging
from datetime import datetime, timedelta
from http import HTTPStatus
from http.client import RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import requests
import pandas as pd

from akshare_project.core.logging_utils import get_logger
from akshare_project.core.paths import ensure_runtime_layout
from akshare_project.scheduler.config import load_scheduler_config
from akshare_project.scheduler.registry import get_function_spec
from akshare_project.scheduler.serialization import serialize_result
from akshare_project.scheduler.store import SchedulerStore

LOGGER = get_logger("ak_scheduler")
LOG_PREFIX = "[AK-SCHEDULER]"
SERVICE_BUILD = "2026-03-29-empty-guard-v3"
def log(message, level="info"):
    normalized_level = str(level or "info").strip().lower()
    level_name = normalized_level.upper()
    levelno = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "success": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }.get(normalized_level, logging.INFO)
    text = f"{LOG_PREFIX}[{level_name}] {message}"
    if normalized_level == "error":
        builtins.print(f"\033[31m{text}\033[0m")
    elif normalized_level == "success":
        builtins.print(f"\033[32m{text}\033[0m")
    elif normalized_level == "warning":
        builtins.print(f"\033[33m{text}\033[0m")
    else:
        builtins.print(text)
    LOGGER.log(levelno, message)


def compact_json(value, limit=240):
    try:
        text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        text = str(value)
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def is_empty_dataframe_payload(result_type, result_json):
    if str(result_type or "").strip().lower() != "dataframe":
        return False
    if not result_json:
        return False
    try:
        payload = json.loads(result_json)
    except Exception:
        return False
    columns = payload.get("columns")
    records = payload.get("records")
    return isinstance(records, list) and len(records) == 0 and (columns is None or isinstance(columns, list))


def summarize_job(job):
    if not job:
        return "job=<none>"
    return (
        f"id={job.get('id')} "
        f"function={job.get('function_name')} "
        f"source={job.get('source_group')} "
        f"status={job.get('status')} "
        f"attempt={job.get('attempt_count')} "
        f"parent={job.get('parent_job_id')} "
        f"root={job.get('root_job_id')} "
        f"workflow={job.get('workflow_name') or '-'} "
        f"caller={job.get('caller_name') or '-'}"
    )


def classify_exception(exc):
    text = str(exc or "")
    lowered = text.lower()
    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout, RemoteDisconnected)):
        return "network"
    if "429" in lowered or "403" in lowered or "captcha" in lowered or "滑块" in text or "验证" in text:
        return "anti_bot"
    if isinstance(exc, ValueError):
        return "data"
    return "unexpected"


def load_policy(config, source_group):
    policy = dict((config.get("source_policies") or {}).get(source_group, {}))
    policy.setdefault("min_interval_seconds", 2.0)
    policy.setdefault("max_attempts", 4)
    policy.setdefault("initial_backoff_seconds", 2)
    policy.setdefault("backoff_cap_seconds", 900)
    policy.setdefault("jitter_seconds", 5)
    policy.setdefault("enable_circuit_breaker", False)
    policy.setdefault("breaker_threshold", 3)
    policy.setdefault("breaker_cooldown_seconds", 300)
    policy.setdefault("retryable_categories", ["network", "anti_bot", "unexpected"])
    policy.setdefault("breaker_categories", ["anti_bot", "network"])
    return policy


class SchedulerRuntimeState:
    def __init__(self):
        self._lock = threading.Lock()
        self.source_state = {
            "eastmoney": {"last_dispatch_at": None, "cooldown_until": None, "consecutive_breaker_hits": 0},
            "sina": {"last_dispatch_at": None, "cooldown_until": None, "consecutive_breaker_hits": 0},
            "ths": {"last_dispatch_at": None, "cooldown_until": None, "consecutive_breaker_hits": 0},
        }

    def get_source_state(self, source_group):
        with self._lock:
            return dict(self.source_state.get(source_group, {}))

    def mark_dispatch(self, source_group):
        with self._lock:
            self.source_state[source_group]["last_dispatch_at"] = datetime.now()

    def mark_success(self, source_group):
        with self._lock:
            self.source_state[source_group]["consecutive_breaker_hits"] = 0

    def mark_failure(self, source_group, policy, error_category):
        with self._lock:
            state = self.source_state[source_group]
            if error_category in policy.get("breaker_categories", []):
                state["consecutive_breaker_hits"] += 1
            else:
                state["consecutive_breaker_hits"] = 0
            if (
                policy.get("enable_circuit_breaker")
                and state["consecutive_breaker_hits"] >= int(policy.get("breaker_threshold", 3))
            ):
                state["cooldown_until"] = datetime.now() + timedelta(seconds=int(policy.get("breaker_cooldown_seconds", 300)))
                state["consecutive_breaker_hits"] = 0

    def cooldown_until(self, source_group):
        with self._lock:
            return self.source_state[source_group]["cooldown_until"]

    def health_payload(self):
        with self._lock:
            return {
                source: {
                    "last_dispatch_at": state["last_dispatch_at"].isoformat() if state["last_dispatch_at"] else None,
                    "cooldown_until": state["cooldown_until"].isoformat() if state["cooldown_until"] else None,
                    "consecutive_breaker_hits": state["consecutive_breaker_hits"],
                }
                for source, state in self.source_state.items()
            }


class SchedulerService:
    def __init__(self):
        ensure_runtime_layout()
        self.config = load_scheduler_config()
        self.store = SchedulerStore()
        self.state = SchedulerRuntimeState()
        self.stop_event = threading.Event()
        self.threads = []
        self.started_at = None
        self.process_id = os.getpid()

    def mark_started(self):
        self.started_at = datetime.now()

    def submit_job(self, payload):
        if not str(payload.get("request_key", "")).strip():
            raise ValueError("request_key is required")
        spec = get_function_spec(payload.get("function_name"))
        if spec is None:
            raise ValueError(f"unsupported function_name: {payload.get('function_name')}")
        if payload.get("source_group") and payload.get("source_group") != spec.source_group:
            raise ValueError(f"source_group mismatch for {payload.get('function_name')}")
        payload["source_group"] = spec.source_group
        job = self.store.submit_job(payload)
        queue_entry = (
            "requeued-empty-cache"
            if job.get("_dedupe_requeued_empty_success")
            else ("reused" if job.get("_dedupe_reused") else "new")
        )
        log(
            "job submitted: "
            f"{summarize_job(job)} "
            f"queue_entry={queue_entry} "
            f"request_key={payload.get('request_key')} "
            f"args={compact_json(payload.get('args') or [])} "
            f"kwargs={compact_json(payload.get('kwargs') or {})}"
        )
        return job

    def get_job(self, job_id):
        job = self.store.get_job(job_id)
        if not job:
            return None
        return self._decode_job_fields(job)

    def get_jobs(self, job_ids):
        jobs = self.store.get_jobs(job_ids)
        return [self._decode_job_fields(job) for job in jobs]

    def _decode_job_fields(self, job):
        decoded = dict(job)
        for field in ("args_json", "kwargs_json"):
            if decoded.get(field):
                try:
                    decoded[field] = json.loads(decoded[field])
                except Exception:
                    pass
        return decoded

    def build_health_payload(self):
        queue_stats = self.store.get_queue_stats()
        queue_depth = 0
        running_jobs = 0
        for source_stats in queue_stats.values():
            queue_depth += int(source_stats.get("PENDING", 0)) + int(source_stats.get("WAITING_PARENT", 0))
            running_jobs += int(source_stats.get("RUNNING", 0))
        return {
            "service_status": "running",
            "build": SERVICE_BUILD,
            "pid": self.process_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "queue_depth": queue_depth,
            "running_jobs": running_jobs,
            "queue_stats": queue_stats,
            "sources": self.state.health_payload(),
        }

    def start_background_threads(self):
        for source_group in ("eastmoney", "sina", "ths"):
            thread = threading.Thread(target=self.worker_loop, args=(source_group,), daemon=True)
            thread.start()
            self.threads.append(thread)

        for target in (self.recovery_loop, self.cleanup_loop, self.reconcile_loop):
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            self.threads.append(thread)

    def recovery_loop(self):
        while not self.stop_event.is_set():
            try:
                recovered = self.store.recover_stale_jobs(self.config.get("lease_seconds", 300))
                if recovered:
                    log(f"recovered stale jobs: {recovered}")
            except Exception as exc:
                log(f"recovery loop failed: {exc}", level="error")
            self.stop_event.wait(max(30.0, float(self.config.get("poll_interval_seconds", 1.0))))

    def cleanup_loop(self):
        while not self.stop_event.is_set():
            try:
                deleted = self.store.cleanup_old_results(self.config.get("result_retention_hours", 24))
                if deleted:
                    log(f"cleaned old scheduler jobs: {deleted}")
            except Exception as exc:
                log(f"cleanup loop failed: {exc}", level="error")
            self.stop_event.wait(300.0)

    def reconcile_loop(self):
        while not self.stop_event.is_set():
            try:
                changed = self.store.reconcile_waiting_children(
                    cancel_on_parent_failure=bool(self.config.get("cancel_children_on_parent_failure", True))
                )
                if changed:
                    log(f"reconciled waiting children: {changed}")
            except Exception as exc:
                log(f"reconcile loop failed: {exc}", level="error")
            self.stop_event.wait(max(5.0, float(self.config.get("poll_interval_seconds", 1.0))))

    def wait_for_rate_limit(self, source_group, policy):
        last_dispatch_at = self.state.get_source_state(source_group).get("last_dispatch_at")
        if not last_dispatch_at:
            return
        min_interval = float(policy.get("min_interval_seconds", 2.0))
        elapsed = (datetime.now() - last_dispatch_at).total_seconds()
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    def execute_job(self, job, policy):
        spec = get_function_spec(job["function_name"])
        if spec is None:
            raise ValueError(f"unsupported function_name: {job['function_name']}")

        args = json.loads(job.get("args_json") or "[]")
        kwargs = json.loads(job.get("kwargs_json") or "{}")
        result = spec.callable(*args, **kwargs)
        if spec.function_name == "stock_zh_a_hist_tx" and isinstance(result, pd.DataFrame) and result.empty:
            # Prevent caching empty payload (no rows) as SUCCESS.
            raise RuntimeError("stock_zh_a_hist_tx returned empty dataframe rows")
        return result

    def worker_loop(self, source_group):
        policy = load_policy(self.config, source_group)
        poll_interval = float(self.config.get("poll_interval_seconds", 1.0))
        while not self.stop_event.is_set():
            cooldown_until = self.state.cooldown_until(source_group)
            if cooldown_until and cooldown_until > datetime.now():
                self.stop_event.wait(min(poll_interval, (cooldown_until - datetime.now()).total_seconds()))
                continue

            try:
                job = self.store.lease_next_job(source_group, self.config.get("lease_seconds", 300))
            except Exception as exc:
                log(f"{source_group} lease failed: {exc}", level="error")
                self.stop_event.wait(poll_interval)
                continue

            if not job:
                self.stop_event.wait(poll_interval)
                continue

            self.wait_for_rate_limit(source_group, policy)
            self.state.mark_dispatch(source_group)

            try:
                result = self.execute_job(job, policy)
                result_type, result_json = serialize_result(result)
                if (
                    job.get("function_name") == "stock_zh_a_hist_tx"
                    and is_empty_dataframe_payload(result_type, result_json)
                ):
                    # Double guard: even if upstream returned an odd object,
                    # do not allow empty-row cache to become SUCCESS.
                    raise RuntimeError("stock_zh_a_hist_tx serialized to empty dataframe rows")
                try:
                    self.store.mark_success(job["id"], result_type, result_json)
                except Exception as state_exc:
                    log(
                        f"{source_group} mark_success failed: {summarize_job(job)} error={state_exc}",
                        level="error",
                    )
                    self.stop_event.wait(poll_interval)
                    continue
                self.state.mark_success(source_group)
                log(
                    f"{source_group} job success: {summarize_job(job)} "
                    f"result_type={result_type}",
                    level="success",
                )
            except Exception as exc:
                error_category = classify_exception(exc)
                error_message = str(exc)
                self.state.mark_failure(source_group, policy, error_category)

                attempt_count = int(job.get("attempt_count") or 0)
                max_attempts = int(policy.get("max_attempts", 4))
                retryable = error_category in set(policy.get("retryable_categories", []))
                if retryable and attempt_count < max_attempts:
                    wait_seconds = min(
                        float(policy.get("backoff_cap_seconds", 900)),
                        float(policy.get("initial_backoff_seconds", 120)) * (2 ** max(0, attempt_count - 1)),
                    )
                    wait_seconds += random.uniform(0, float(policy.get("jitter_seconds", 0)))
                    next_run_at = datetime.now() + timedelta(seconds=wait_seconds)
                    try:
                        self.store.mark_retry(job["id"], error_category, error_message, next_run_at)
                    except Exception as state_exc:
                        log(
                            f"{source_group} mark_retry failed: {summarize_job(job)} error={state_exc}",
                            level="error",
                        )
                        self.stop_event.wait(poll_interval)
                        continue
                    log(
                        f"{source_group} job retry scheduled: {summarize_job(job)} "
                        f"category={error_category} wait={wait_seconds:.1f}s error={error_message}",
                        level="warning",
                    )
                else:
                    try:
                        self.store.mark_failed(job["id"], error_category, error_message)
                    except Exception as state_exc:
                        log(
                            f"{source_group} mark_failed failed: {summarize_job(job)} error={state_exc}",
                            level="error",
                        )
                        self.stop_event.wait(poll_interval)
                        continue
                    log(
                        f"{source_group} job failed: {summarize_job(job)} "
                        f"category={error_category} error={error_message}",
                        level="error",
                    )


SERVICE_INSTANCE = SchedulerService()


class SchedulerRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload, status=HTTPStatus.OK):
        encoded = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/jobs/query":
            try:
                length = int(self.headers.get("Content-Length", "0") or 0)
                body = self.rfile.read(length).decode("utf-8") if length else "{}"
                payload = json.loads(body or "{}")
                ids = payload.get("ids") or []
                jobs = SERVICE_INSTANCE.get_jobs(ids)
                self._send_json({"jobs": jobs})
            except Exception as exc:
                log(
                    f"http request failed: method=POST path={parsed.path} error={exc}",
                    level="error",
                )
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        if parsed.path != "/jobs":
            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(length).decode("utf-8") if length else "{}"
            payload = json.loads(body or "{}")
            job = SERVICE_INSTANCE.submit_job(payload)
            self._send_json({
                "id": job["id"],
                "status": job["status"],
                "root_job_id": job.get("root_job_id") or job["id"],
                "deduped": bool(job.get("_dedupe_reused")),
            })
        except Exception as exc:
            log(
                f"http request failed: method=POST path={parsed.path} error={exc}",
                level="error",
            )
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(SERVICE_INSTANCE.build_health_payload())
            return

        if parsed.path.startswith("/jobs/"):
            try:
                job_id = int(parsed.path.split("/")[-1])
            except ValueError:
                log(
                    f"http request failed: method=GET path={parsed.path} error=invalid job id",
                    level="error",
                )
                self._send_json({"error": "invalid job id"}, status=HTTPStatus.BAD_REQUEST)
                return
            job = SERVICE_INSTANCE.get_job(job_id)
            if not job:
                log(
                    f"http request failed: method=GET path={parsed.path} error=job not found",
                    level="error",
                )
                self._send_json({"error": "job not found"}, status=HTTPStatus.NOT_FOUND)
                return
            self._send_json(job)
            return

        self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format, *args):
        return


def inspect_listening_port_owner(port):
    try:
        result = subprocess.run(
            ["netstat", "-ano", "-p", "TCP"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception:
        return None

    target_suffix = f":{int(port)}"
    for raw_line in (result.stdout or "").splitlines():
        line = raw_line.strip()
        if not line or "LISTENING" not in line.upper():
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        local_address = parts[1]
        state = parts[3].upper()
        pid_text = parts[4]
        if state != "LISTENING" or not local_address.endswith(target_suffix):
            continue
        owner = {
            "pid": int(pid_text) if str(pid_text).isdigit() else None,
            "image_name": None,
        }
        if owner["pid"] is not None:
            try:
                task = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {owner['pid']}", "/FO", "CSV", "/NH"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                task_line = (task.stdout or "").strip().splitlines()
                if task_line:
                    first_line = task_line[0].strip().strip('"')
                    if first_line and not first_line.startswith("INFO:"):
                        owner["image_name"] = first_line.split('","')[0].strip('"')
            except Exception:
                pass
        return owner
    return None


def create_scheduler_server(host, port):
    try:
        return ThreadingHTTPServer((host, int(port)), SchedulerRequestHandler)
    except OSError as exc:
        if getattr(exc, "winerror", None) not in {10048} and exc.errno not in {48, 98}:
            raise
        owner = inspect_listening_port_owner(port)
        if owner:
            image_name = owner.get("image_name") or "unknown"
            raise RuntimeError(
                f"AK scheduler service can not start because {host}:{port} is already in use "
                f"by pid={owner.get('pid')} image={image_name}"
            ) from exc
        raise RuntimeError(
            f"AK scheduler service can not start because {host}:{port} is already in use"
        ) from exc


def run_scheduler_service():
    config = SERVICE_INSTANCE.config
    host = config.get("host", "127.0.0.1")
    port = int(config.get("port", 8765))
    server = create_scheduler_server(host, port)
    server.daemon_threads = True
    SERVICE_INSTANCE.mark_started()
    SERVICE_INSTANCE.start_background_threads()
    log(
        f"AK scheduler service started at http://{host}:{port} "
        f"build={SERVICE_BUILD}"
    )
    try:
        server.serve_forever()
    finally:
        SERVICE_INSTANCE.stop_event.set()
        server.server_close()


def run_healthcheck():
    config = load_scheduler_config()
    try:
        response = requests.get(
            f"http://{config.get('host', '127.0.0.1')}:{int(config.get('port', 8765))}/health",
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            "AK scheduler service is unavailable. "
            "Please start it first with: python ak_scheduler_service.py serve"
        ) from exc
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))


def run_scheduler_doctor(limit=20):
    config = load_scheduler_config()
    host = config.get('host', '127.0.0.1')
    port = int(config.get('port', 8765))
    base_url = f"http://{config.get('host', '127.0.0.1')}:{int(config.get('port', 8765))}"
    health = None
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        response.raise_for_status()
        health = response.json()
    except requests.RequestException:
        health = {
            "service_status": "unavailable",
            "build": None,
            "pid": None,
            "started_at": None,
        }

    store = SchedulerStore()
    empty_count = None
    recent_empty = []
    db_error = None
    try:
        empty_count = store.count_empty_stock_hist_successes()
        recent_empty = store.get_recent_empty_stock_hist_successes(limit=int(limit))
    except Exception as exc:
        db_error = str(exc)
    payload = {
        "expected_build": SERVICE_BUILD,
        "health": health,
        "port_owner": inspect_listening_port_owner(port),
        "host": host,
        "port": port,
        "empty_stock_hist_success_count": empty_count,
        "recent_empty_stock_hist_successes": recent_empty,
        "db_error": db_error,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
