import json
import re
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


@dataclass(frozen=True)
class SchedulerJobHandle:
    job_id: int
    root_job_id: int | None
    status: str


@dataclass(frozen=True)
class SchedulerJobSnapshot:
    job_id: int
    root_job_id: int | None
    status: str
    function_name: str | None
    source_group: str | None
    parent_job_id: int | None
    workflow_name: str | None
    result_type: str | None
    result_json: str | None
    error_category: str | None
    error_message: str | None


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


def _raise_service_response_error(response: requests.Response) -> None:
    try:
        payload = response.json()
        detail = payload.get("error") or payload
    except Exception:
        detail = response.text

    detail_text = str(detail)
    unsupported_match = re.search(r"unsupported function_name:\s*([A-Za-z0-9_]+)", detail_text)
    if unsupported_match:
        function_name = unsupported_match.group(1).strip()
        if get_function_spec(function_name) is not None:
            raise RuntimeError(
                "AK scheduler service is running an outdated function registry. "
                f"It does not recognize '{function_name}'. "
                "Please restart it with: python ak_scheduler_service.py serve"
            )
    raise RuntimeError(
        f"AK scheduler service request failed: HTTP {response.status_code}; {detail}"
    )


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
        if response.status_code >= 400:
            _raise_service_response_error(response)
        return response.json()
    except RuntimeError:
        raise
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


def _query_jobs(job_ids: list[int]) -> list[dict]:
    base_url = _build_base_url()
    payload = {"ids": [int(job_id) for job_id in job_ids]}
    try:
        response = requests.post(f"{base_url}/jobs/query", json=payload, timeout=30)
        if response.status_code >= 400:
            _raise_service_response_error(response)
        data = response.json()
        return list(data.get("jobs") or [])
    except RuntimeError:
        raise
    except requests.RequestException as exc:
        _raise_service_unavailable(exc)


def _snapshot_from_payload(payload: dict) -> SchedulerJobSnapshot:
    return SchedulerJobSnapshot(
        job_id=int(payload["id"]),
        root_job_id=(
            int(payload["root_job_id"])
            if payload.get("root_job_id") not in (None, "")
            else None
        ),
        status=str(payload.get("status", "")).strip().upper(),
        function_name=(str(payload.get("function_name")).strip() if payload.get("function_name") else None),
        source_group=(str(payload.get("source_group")).strip() if payload.get("source_group") else None),
        parent_job_id=(
            int(payload["parent_job_id"])
            if payload.get("parent_job_id") not in (None, "")
            else None
        ),
        workflow_name=(str(payload.get("workflow_name")).strip() if payload.get("workflow_name") else None),
        result_type=(str(payload.get("result_type")).strip() if payload.get("result_type") else None),
        result_json=payload.get("result_json"),
        error_category=(str(payload.get("error_category")).strip() if payload.get("error_category") else None),
        error_message=(str(payload.get("error_message")).strip() if payload.get("error_message") else None),
    )


def submit_registered_job(
    func_or_name,
    *args,
    scheduler_context: SchedulerContext | None = None,
    caller_name: str | None = None,
    request_key: str | None = None,
    **kwargs,
) -> SchedulerJobHandle:
    spec = (
        get_function_spec(func_or_name)
        if isinstance(func_or_name, str)
        else resolve_callable_spec(func_or_name)
    )
    if spec is None:
        raise ValueError(f"function is not registered for AK scheduler: {func_or_name}")

    submitted = _submit_job(
        spec.function_name,
        args,
        kwargs,
        scheduler_context=scheduler_context,
        caller_name=caller_name or spec.function_name,
        request_key=request_key,
    )
    return SchedulerJobHandle(
        job_id=int(submitted["id"]),
        root_job_id=(
            int(submitted["root_job_id"])
            if submitted.get("root_job_id") not in (None, "")
            else None
        ),
        status=str(submitted.get("status", "")).strip().upper() or "PENDING",
    )


def get_job_snapshot(job_id: int) -> SchedulerJobSnapshot:
    base_url = _build_base_url()
    try:
        response = requests.get(f"{base_url}/jobs/{int(job_id)}", timeout=30)
        if response.status_code >= 400:
            _raise_service_response_error(response)
        payload = response.json()
    except RuntimeError:
        raise
    except requests.RequestException as exc:
        _raise_service_unavailable(exc)
    return _snapshot_from_payload(payload)


def get_job_snapshots(job_ids: list[int]) -> list[SchedulerJobSnapshot]:
    if not job_ids:
        return []
    return [_snapshot_from_payload(item) for item in _query_jobs(job_ids)]


def decode_job_result(snapshot: SchedulerJobSnapshot):
    return deserialize_result(snapshot.result_type, snapshot.result_json)


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
    handle = submit_registered_job(
        spec.function_name,
        *args,
        scheduler_context=scheduler_context,
        caller_name=caller_name or spec.function_name,
        request_key=request_key,
        **kwargs,
    )
    polled = _poll_job(int(handle.job_id), float(config.get("client_timeout_seconds", 7200)))
    value = deserialize_result(polled.get("result_type"), polled.get("result_json"))
    return SchedulerCallResult(
        value=value,
        job_id=int(handle.job_id),
        root_job_id=handle.root_job_id,
        status=str(polled.get("status", "SUCCESS")).strip().upper() or "SUCCESS",
    )
