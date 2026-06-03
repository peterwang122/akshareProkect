import argparse
import getpass
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIT_ROOT = REPO_ROOT.parent / "FIT"


def env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return str(value).strip() if value is not None and str(value).strip() else default


def docker_compose_prefix(compose_file: Path) -> list[str]:
    return ["docker", "compose", "-f", str(compose_file)]


def resolve_host_mysql_tool(args, tool: str) -> str:
    if tool == "mysql" and args.host_mysql:
        return str(args.host_mysql)
    if tool == "mysqldump" and args.host_mysqldump:
        return str(args.host_mysqldump)
    if args.host_mysql_bin_dir:
        candidate = args.host_mysql_bin_dir / tool
        if candidate.exists():
            return str(candidate)

    candidate = shutil.which(tool)
    if candidate:
        return candidate

    mac_candidate = Path("/usr/local/mysql/bin") / tool
    if mac_candidate.exists():
        return str(mac_candidate)

    raise SystemExit(
        f"Can not find host MySQL tool: {tool}. "
        "Install MySQL client tools, set --host-mysql-bin-dir, "
        "or use --remote-client docker if the Docker network can reach the remote database."
    )


def mysql_client_command(args, endpoint: dict, tool: str) -> tuple[list[str], dict]:
    env_vars = os.environ.copy()
    env_vars["MYSQL_PWD"] = endpoint["password"]

    if endpoint.get("client") == "host":
        return [resolve_host_mysql_tool(args, tool)], env_vars

    command = docker_compose_prefix(args.compose_file)
    command += ["exec", "-T", "-e", f"MYSQL_PWD={endpoint['password']}", args.mysql_service, tool]
    return command, env_vars


def mysql_args(host: str, port: int, user: str, database: str | None = None) -> list[str]:
    args = ["-h", host, "-P", str(port), "-u", user, "--default-character-set=utf8mb4"]
    if database:
        args.append(database)
    return args


def run_mysql_statement(args, target: dict, statement: str) -> None:
    command, env_vars = mysql_client_command(args, target, "mysql")
    command += mysql_args(target["host"], target["port"], target["user"])
    command += ["-e", statement]
    subprocess.run(command, check=True, env=env_vars)


def dump_database(args, source: dict, dump_path: Path) -> None:
    dump_path.parent.mkdir(parents=True, exist_ok=True)
    command, env_vars = mysql_client_command(args, source, "mysqldump")
    command += mysql_args(source["host"], source["port"], source["user"])
    command += [
        "--single-transaction",
        "--routines",
        "--triggers",
        "--events",
        "--hex-blob",
        "--column-statistics=0",
        source["database"],
    ]
    with dump_path.open("wb") as output:
        subprocess.run(command, stdout=output, check=True, env=env_vars)


def restore_database(args, target: dict, dump_path: Path) -> None:
    quoted_db = target["database"].replace("`", "``")
    run_mysql_statement(
        args,
        target,
        (
            f"DROP DATABASE IF EXISTS `{quoted_db}`; "
            f"CREATE DATABASE `{quoted_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        ),
    )
    command, env_vars = mysql_client_command(args, target, "mysql")
    command += mysql_args(target["host"], target["port"], target["user"], target["database"])
    with dump_path.open("rb") as input_file:
        subprocess.run(command, stdin=input_file, check=True, env=env_vars)


def prompt_password(label: str, current: str) -> str:
    if current:
        return current
    return getpass.getpass(f"{label} password: ")


def build_local_endpoint(args) -> dict:
    return {
        "label": "mac-docker",
        "host": env("LOCAL_DB_HOST", "127.0.0.1"),
        "port": int(env("LOCAL_DB_PORT", "3306")),
        "user": env("LOCAL_DB_ROOT_USER", "root"),
        "password": env("LOCAL_DB_ROOT_PASSWORD", env("MYSQL_ROOT_PASSWORD", "fitroot")),
        "database": args.database,
        "client": "docker",
    }


def build_remote_endpoint(args) -> dict:
    return {
        "label": "windows",
        "host": args.remote_host,
        "port": int(args.remote_port),
        "user": args.remote_user,
        "password": prompt_password("Remote MySQL", args.remote_password),
        "database": args.database,
        "client": args.remote_client,
    }


def require_confirm(args, target: dict) -> None:
    if args.confirm:
        return
    raise SystemExit(
        "Refusing to restore without --confirm. "
        f"Target would be overwritten: {target['label']} {target['host']}:{target['port']}/{target['database']}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Manual MySQL full-database sync for akshareProkect/FIT.")
    parser.add_argument("direction", choices=["windows-to-mac", "mac-to-windows", "backup-only"])
    parser.add_argument("--fit-root", type=Path, default=Path(env("FIT_ROOT", str(DEFAULT_FIT_ROOT))))
    parser.add_argument("--compose-file", type=Path)
    parser.add_argument("--mysql-service", default=env("LOCAL_MYSQL_SERVICE", "mysql"))
    parser.add_argument("--database", default=env("DB_NAME", env("MYSQL_DATABASE", "stock_info")))
    parser.add_argument("--remote-host", default=env("REMOTE_DB_HOST", "192.168.1.16"))
    parser.add_argument("--remote-port", type=int, default=int(env("REMOTE_DB_PORT", "3306")))
    parser.add_argument("--remote-user", default=env("REMOTE_DB_USER", "root"))
    parser.add_argument("--remote-password", default=env("REMOTE_DB_PASSWORD", ""))
    parser.add_argument("--remote-client", choices=["host", "docker"], default=env("REMOTE_DB_CLIENT", "host"))
    parser.add_argument("--host-mysql-bin-dir", type=Path, default=Path(env("HOST_MYSQL_BIN_DIR")) if env("HOST_MYSQL_BIN_DIR") else None)
    parser.add_argument("--host-mysql", type=Path, default=Path(env("HOST_MYSQL")) if env("HOST_MYSQL") else None)
    parser.add_argument("--host-mysqldump", type=Path, default=Path(env("HOST_MYSQLDUMP")) if env("HOST_MYSQLDUMP") else None)
    parser.add_argument("--artifacts-dir", type=Path, default=REPO_ROOT / "runtime" / "artifacts" / "db_sync")
    parser.add_argument("--confirm", action="store_true", help="Required for restore operations.")
    parser.add_argument("--skip-target-backup", action="store_true")
    args = parser.parse_args()

    args.compose_file = args.compose_file or args.fit_root / "docker-compose.yml"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    args.artifacts_dir.mkdir(parents=True, exist_ok=True)

    local = build_local_endpoint(args)
    if args.direction == "windows-to-mac":
        remote = build_remote_endpoint(args)
        source, target = remote, local
    elif args.direction == "mac-to-windows":
        remote = build_remote_endpoint(args)
        source, target = local, remote
    else:
        source, target = local, None

    source_dump = args.artifacts_dir / f"{timestamp}_{source['label']}_{source['database']}.sql"
    print(f"Dumping {source['label']} -> {source_dump}")
    dump_database(args, source, source_dump)

    if target is None:
        print(f"Backup finished: {source_dump}")
        return 0

    require_confirm(args, target)
    if not args.skip_target_backup:
        backup_path = args.artifacts_dir / f"{timestamp}_before_restore_{target['label']}_{target['database']}.sql"
        print(f"Backing up target {target['label']} -> {backup_path}")
        dump_database(args, target, backup_path)

    print(f"Restoring {source_dump} -> {target['label']} {target['host']}:{target['port']}/{target['database']}")
    restore_database(args, target, source_dump)
    print("Sync finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
