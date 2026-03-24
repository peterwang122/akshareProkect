import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

import requests

from akshare_project.scheduler.config import load_scheduler_config
from akshare_project.scheduler.registry import get_function_spec, resolve_callable_spec
from akshare_project.scheduler.serialization import deserialize_result


@dataclass(frozen=True)
class SchedulerContext:
    parent_job_id: int | None = None
    root_job_id: int | None = None
    workflow_name: str | None = None


@dataclass(frozen=True)
class SchedulerCallResult:
    value: Any
    job_id: int
    root_job_id: int | None
    status: str


def _json_safe(value):
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _build_base_url() -> str:
    config = load_scheduler_config()
    return f"http://{config.get('host', '127.0.0.1')}:{int(config.get('port', 8765))}"


def _raise_service_unavailable(exc: Exception) -> None:
    raise RuntimeError(
        "AK scheduler service is unavailable. "
        "Please start it first with: python ak_scheduler_service.py serve"
    ) from exc


def _submit_job(
    function_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    scheduler_context: SchedulerContext | None = None,
    caller_name: str | None = None,
    request_key: str | None = None,
) -> dict:
    base_url = _build_base_url()
    payload = {
        "function_name": function_name,
        "source_group": get_function_spec(function_name).source_group,
        "args": _json_safe(list(args)),
        "kwargs": _json_safe(kwargs),
        "request_key": str(request_key or f"{function_name}:{uuid.uuid4().hex}"),
        "caller_name": str(caller_name or function_name).strip(),
    }
    if scheduler_context is not None:
        if scheduler_context.parent_job_id is not None:
            payload["parent_job_id"] = int(scheduler_context.parent_job_id)
        if scheduler_context.root_job_id is not None:
            payload["root_job_id"] = int(scheduler_context.root_job_id)
        if scheduler_context.workflow_name:
            payload["workflow_name"] = str(scheduler_context.workflow_name).strip()

    try:
        response = requests.post(f"{base_url}/jobs", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        _raise_service_unavailable(exc)


def _poll_job(job_id: int, timeout_seconds: float) -> dict:
    base_url = _build_base_url()
    started = time.time()
    config = load_scheduler_config()
    poll_interval = max(0.5, float(config.get("client_poll_interval_seconds", 1.0)))

    while True:
        try:
            response = requests.get(f"{base_url}/jobs/{int(job_id)}", timeout=30)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            _raise_service_unavailable(exc)

        status = str(payload.get("status", "")).strip().upper()
        if status == "SUCCESS":
            return payload
        if status in {"FAILED", "CANCELLED"}:
            error_category = payload.get("error_category")
            error_message = payload.get("error_message")
            raise RuntimeError(
                f"AK scheduler job {job_id} {status.lower()} "
                f"[{error_category or 'unknown'}]: {error_message or 'unknown error'}"
            )

        if time.time() - started > timeout_seconds:
            raise TimeoutError(f"AK scheduler job {job_id} timed out after {timeout_seconds:.0f}s")
        time.sleep(poll_interval)


def call_registered_function(
    func_or_name,
    *args,
    scheduler_context: SchedulerContext | None = None,
    caller_name: str | None = None,
    request_key: str | None = None,
    **kwargs,
) -> SchedulerCallResult:
    spec = (
        get_function_spec(func_or_name)
        if isinstance(func_or_name, str)
        else resolve_callable_spec(func_or_name)
    )
    if spec is None:
        raise ValueError(f"function is not registered for AK scheduler: {func_or_name}")

    config = load_scheduler_config()
    submitted = _submit_job(
        spec.function_name,
        args,
        kwargs,
        scheduler_context=scheduler_context,
        caller_name=caller_name or spec.function_name,
        request_key=request_key,
    )
    polled = _poll_job(int(submitted["id"]), float(config.get("client_timeout_seconds", 7200)))
    value = deserialize_result(polled.get("result_type"), polled.get("result_json"))
    return SchedulerCallResult(
        value=value,
        job_id=int(polled["id"]),
        root_job_id=(
            int(polled["root_job_id"])
            if polled.get("root_job_id") not in (None, "")
            else None
        ),
        status=str(polled.get("status", "SUCCESS")).strip().upper() or "SUCCESS",
    )
