from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"
PACKAGE_ROOT = SRC_ROOT / "akshare_project"
RUNTIME_ROOT = REPO_ROOT / "runtime"
LOGS_DIR = RUNTIME_ROOT / "logs"
STATE_DIR = RUNTIME_ROOT / "state"
CACHE_DIR = RUNTIME_ROOT / "cache"
ARTIFACTS_DIR = RUNTIME_ROOT / "artifacts"
DATA_INPUT_DIR = REPO_ROOT / "data" / "input"
DOCS_DIR = REPO_ROOT / "docs"
CONFIG_DIR = REPO_ROOT / "config"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_runtime_layout() -> None:
    for path in (LOGS_DIR, STATE_DIR, CACHE_DIR, ARTIFACTS_DIR, DATA_INPUT_DIR, DOCS_DIR):
        ensure_dir(path)


def get_repo_root() -> Path:
    return REPO_ROOT


def get_config_dir() -> Path:
    return CONFIG_DIR


def get_logs_dir() -> Path:
    return ensure_dir(LOGS_DIR)


def get_log_path(module_name: str) -> Path:
    safe_name = str(module_name).strip().replace(" ", "_")
    return get_logs_dir() / f"{safe_name}.log"


def get_state_dir() -> Path:
    return ensure_dir(STATE_DIR)


def get_state_path(name: str, suffix: str = "progress") -> Path:
    safe_name = str(name).strip().replace(" ", "_")
    safe_suffix = str(suffix).strip().replace(".", "_")
    return get_state_dir() / f"{safe_name}.{safe_suffix}"


def get_cache_dir(name: str | None = None) -> Path:
    base = ensure_dir(CACHE_DIR)
    return ensure_dir(base / name) if name else base


def get_artifacts_dir(name: str | None = None) -> Path:
    base = ensure_dir(ARTIFACTS_DIR)
    return ensure_dir(base / name) if name else base


def get_input_dir() -> Path:
    return ensure_dir(DATA_INPUT_DIR)


def get_input_path(filename: str, fallback_to_root: bool = True) -> Path:
    candidate = get_input_dir() / filename
    if candidate.exists():
        return candidate

    if fallback_to_root:
        root_candidate = REPO_ROOT / filename
        if root_candidate.exists():
            return root_candidate

    return candidate


def get_docs_dir() -> Path:
    return ensure_dir(DOCS_DIR)
