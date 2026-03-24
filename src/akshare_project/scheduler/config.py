import json
from pathlib import Path

from akshare_project.core.paths import get_config_dir


DEFAULT_SCHEDULER_CONFIG = {
    "host": "127.0.0.1",
    "port": 8765,
    "poll_interval_seconds": 1.0,
    "result_retention_hours": 24,
    "lease_seconds": 300,
    "client_poll_interval_seconds": 1.0,
    "client_timeout_seconds": 7200,
    "cancel_children_on_parent_failure": True,
    "source_policies": {},
}


def get_scheduler_config_path() -> Path:
    return get_config_dir() / "ak_scheduler.json"


def load_scheduler_config() -> dict:
    config = dict(DEFAULT_SCHEDULER_CONFIG)
    config_path = get_scheduler_config_path()
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as file:
            loaded = json.load(file)
        config.update(loaded or {})

    config["port"] = int(config.get("port", DEFAULT_SCHEDULER_CONFIG["port"]))
    config["poll_interval_seconds"] = float(config.get("poll_interval_seconds", 1.0))
    config["result_retention_hours"] = float(config.get("result_retention_hours", 24))
    config["lease_seconds"] = int(config.get("lease_seconds", 300))
    config["client_poll_interval_seconds"] = float(config.get("client_poll_interval_seconds", 1.0))
    config["client_timeout_seconds"] = float(config.get("client_timeout_seconds", 7200))
    config["cancel_children_on_parent_failure"] = bool(config.get("cancel_children_on_parent_failure", True))
    return config
