import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = Path(sys.executable)


def run(args: list[str]) -> int:
    return subprocess.run(args, cwd=REPO_ROOT, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Cross-platform akshareProkect service helper.")
    parser.add_argument(
        "command",
        choices=[
            "scheduler",
            "stock-temp",
            "runner-daily",
            "scheduler-health",
            "stock-temp-health",
            "scheduler-doctor",
            "install-playwright",
        ],
    )
    parser.add_argument("args", nargs="*")
    parsed = parser.parse_args()

    if parsed.command == "scheduler":
        return run([str(PYTHON), "ak_scheduler_service.py", "serve"])
    if parsed.command == "stock-temp":
        return run([str(PYTHON), "stock_temp_service.py", "serve"])
    if parsed.command == "runner-daily":
        return run([str(PYTHON), "run.py", "runner", "daily"])
    if parsed.command == "scheduler-health":
        return run([str(PYTHON), "ak_scheduler_service.py", "health"])
    if parsed.command == "stock-temp-health":
        return run([str(PYTHON), "stock_temp_service.py", "health"])
    if parsed.command == "scheduler-doctor":
        return run([str(PYTHON), "ak_scheduler_service.py", "doctor", *(parsed.args or [])])
    if parsed.command == "install-playwright":
        return run([str(PYTHON), "-m", "playwright", "install", "chromium"])

    parser.error(f"unsupported command: {parsed.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
