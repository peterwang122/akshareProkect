import json
from pathlib import Path

from akshare_project.services.stock_temp_service import (
    build_daily_routes,
    build_daily_success_payload,
)


GOLDEN_PATH = Path(__file__).parent / "golden" / "quant_index_daily_contract_v1.json"


def _load_golden(path=GOLDEN_PATH):
    return json.loads(path.read_text(encoding="utf-8"))


def test_akshare_produces_quant_index_daily_golden_contract():
    golden = _load_golden()
    request = golden["request"]
    expected_response = golden["provider_response"]
    route = build_daily_routes()[request["endpoint"]]

    assert route.path == request["endpoint"]
    assert route.task_name == request["collector_key"]
    assert request["payload"] == {}

    response = build_daily_success_payload(
        route=route,
        started_at=expected_response["started_at"],
        finished_at=expected_response["finished_at"],
        duration_seconds=1.2344,
        result=expected_response["result"],
    )

    assert response == expected_response


def test_sibling_fit_golden_sample_does_not_drift():
    sibling_path = (
        Path(__file__).resolve().parents[2]
        / "FIT"
        / "backend"
        / "tests"
        / "golden"
        / GOLDEN_PATH.name
    )
    if sibling_path.exists():
        assert _load_golden(sibling_path) == _load_golden()
