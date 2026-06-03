import json
import os
from pathlib import Path

from akshare_project.core.paths import get_config_dir


DEFAULT_DB_INFO = {
    "host": "127.0.0.1",
    "user": "fit",
    "passwd": "fitpass",
    "port": 3306,
    "db": "stock_info",
    "charset": "utf8mb4",
    "use_unicode": True,
    "timezone": "+08:00",
}


ENV_OVERRIDES = {
    "host": ("AK_DB_HOST", "DB_HOST", "MYSQL_HOST"),
    "user": ("AK_DB_USER", "DB_USER", "MYSQL_USER"),
    "passwd": ("AK_DB_PASSWORD", "AK_DB_PASS", "DB_PASSWORD", "DB_PASS", "MYSQL_PASSWORD"),
    "port": ("AK_DB_PORT", "DB_PORT", "MYSQL_PORT"),
    "db": ("AK_DB_NAME", "DB_NAME", "MYSQL_DATABASE"),
    "database": ("AK_DB_NAME", "DB_NAME", "MYSQL_DATABASE"),
    "charset": ("AK_DB_CHARSET", "DB_CHARSET", "MYSQL_CHARSET"),
    "timezone": ("AK_DB_TIMEZONE", "DB_TIMEZONE", "MYSQL_TIMEZONE"),
}


def _first_env(names: tuple[str, ...]) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def load_db_info(config_path: str | Path | None = None) -> dict:
    path = Path(config_path) if config_path is not None else get_config_dir() / "db_info.json"
    db_info = dict(DEFAULT_DB_INFO)
    if path.exists():
        with path.open("r", encoding="utf-8") as file:
            db_info.update(json.load(file) or {})

    for key, env_names in ENV_OVERRIDES.items():
        value = _first_env(env_names)
        if value is None:
            continue
        db_info[key] = int(value) if key == "port" else value

    if db_info.get("database") and not db_info.get("db"):
        db_info["db"] = db_info["database"]
    if db_info.get("db") and not db_info.get("database"):
        db_info["database"] = db_info["db"]

    db_info["port"] = int(db_info.get("port", 3306))
    db_info["charset"] = str(db_info.get("charset") or "utf8mb4")
    db_info["timezone"] = str(db_info.get("timezone") or "+08:00").strip() or "+08:00"
    return db_info
