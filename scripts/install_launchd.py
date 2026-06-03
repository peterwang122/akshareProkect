import argparse
import plistlib
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = Path(sys.executable)
LAUNCH_AGENTS = Path.home() / "Library" / "LaunchAgents"


SERVICES = {
    "scheduler": {
        "label": "com.akshareProkect.scheduler",
        "args": [str(PYTHON), str(REPO_ROOT / "ak_scheduler_service.py"), "serve"],
        "log": "ak_scheduler_launchd",
    },
    "stock-temp": {
        "label": "com.akshareProkect.stock-temp",
        "args": [str(PYTHON), str(REPO_ROOT / "stock_temp_service.py"), "serve"],
        "log": "stock_temp_service_launchd",
    },
}


def plist_payload(service: dict) -> dict:
    log_dir = REPO_ROOT / "runtime" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return {
        "Label": service["label"],
        "ProgramArguments": service["args"],
        "WorkingDirectory": str(REPO_ROOT),
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(log_dir / f"{service['log']}.out.log"),
        "StandardErrorPath": str(log_dir / f"{service['log']}.err.log"),
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
        },
    }


def install(service_name: str) -> Path:
    service = SERVICES[service_name]
    LAUNCH_AGENTS.mkdir(parents=True, exist_ok=True)
    plist_path = LAUNCH_AGENTS / f"{service['label']}.plist"
    with plist_path.open("wb") as file:
        plistlib.dump(plist_payload(service), file)
    subprocess.run(["launchctl", "unload", str(plist_path)], check=False)
    subprocess.run(["launchctl", "load", str(plist_path)], check=True)
    return plist_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Install macOS launchd services for akshareProkect.")
    parser.add_argument("services", nargs="*", choices=sorted(SERVICES), default=sorted(SERVICES))
    args = parser.parse_args()
    for service_name in args.services:
        plist_path = install(service_name)
        print(f"installed {service_name}: {plist_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
