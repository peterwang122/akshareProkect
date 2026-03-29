import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from akshare_project.core.paths import ensure_runtime_layout  # noqa: E402
from akshare_project.scheduler.service import run_healthcheck, run_scheduler_doctor, run_scheduler_service  # noqa: E402


def main():
    ensure_runtime_layout()
    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "serve"

    if command == "serve":
        run_scheduler_service()
        return
    if command == "health":
        run_healthcheck()
        return
    if command == "doctor":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        run_scheduler_doctor(limit=limit)
        return

    raise ValueError("usage: python ak_scheduler_service.py [serve|health|doctor [limit]]")


if __name__ == "__main__":
    main()
