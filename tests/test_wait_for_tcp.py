import importlib.util
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "wait_for_tcp.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("ak_wait_for_tcp", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_wait_for_tcp_executes_command_after_dependencies_are_ready(monkeypatch):
    module = _load_module()
    calls = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    def fake_execv(program, command):
        calls.append((program, command))
        raise RuntimeError("exec called")

    monkeypatch.setattr(module.socket, "create_connection", lambda *_args, **_kwargs: Connection())
    monkeypatch.setattr(module.os, "execv", fake_execv)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(SCRIPT_PATH), "127.0.0.1:3306", "127.0.0.1:6379", "--", "/bin/echo", "ready"],
    )

    with pytest.raises(RuntimeError, match="exec called"):
        module.main()

    assert calls == [("/bin/echo", ["/bin/echo", "ready"])]


@pytest.mark.parametrize("endpoint", ["3306", "localhost:nope", "localhost:70000"])
def test_wait_for_tcp_rejects_invalid_endpoints(endpoint):
    module = _load_module()
    with pytest.raises(Exception):
        module._parse_endpoint(endpoint)
