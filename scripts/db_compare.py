import argparse
import getpass
import os


KEY_DATE_COLUMNS = {
    "stock_daily_data": "trade_date",
    "index_daily_data": "trade_date",
    "index_us_daily_data": "trade_date",
    "index_hk_daily_data": "trade_date",
    "quant_index_dashboard_daily": "trade_date",
    "futures_daily_data": "trade_date",
    "etf_daily_data_sina": "trade_date",
    "forex_daily_data": "trade_date",
}


def env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return str(value).strip() if value is not None and str(value).strip() else default


def password(label: str, value: str) -> str:
    return value if value else getpass.getpass(f"{label} password: ")


def connect(endpoint: dict):
    import pymysql
    from pymysql.cursors import DictCursor

    return pymysql.connect(
        host=endpoint["host"],
        port=int(endpoint["port"]),
        user=endpoint["user"],
        password=endpoint["password"],
        database=endpoint["database"],
        charset="utf8mb4",
        cursorclass=DictCursor,
    )


def table_names(conn) -> list[str]:
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        rows = cursor.fetchall()
    return sorted(next(iter(row.values())) for row in rows)


def row_count(conn, table_name: str) -> int:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) AS total FROM `{table_name}`")
        return int((cursor.fetchone() or {}).get("total") or 0)


def max_date(conn, table_name: str, column_name: str):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT MAX(`{column_name}`) AS max_value FROM `{table_name}`")
        return (cursor.fetchone() or {}).get("max_value")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare local Docker MySQL and remote Windows MySQL.")
    parser.add_argument("--database", default=env("DB_NAME", env("MYSQL_DATABASE", "stock_info")))
    parser.add_argument("--local-host", default=env("LOCAL_DB_HOST", "127.0.0.1"))
    parser.add_argument("--local-port", type=int, default=int(env("LOCAL_DB_PORT", "3306")))
    parser.add_argument("--local-user", default=env("LOCAL_DB_USER", "fit"))
    parser.add_argument("--local-password", default=env("LOCAL_DB_PASSWORD", env("MYSQL_PASSWORD", "fitpass")))
    parser.add_argument("--remote-host", default=env("REMOTE_DB_HOST", "192.168.1.16"))
    parser.add_argument("--remote-port", type=int, default=int(env("REMOTE_DB_PORT", "3306")))
    parser.add_argument("--remote-user", default=env("REMOTE_DB_USER", "root"))
    parser.add_argument("--remote-password", default=env("REMOTE_DB_PASSWORD", ""))
    args = parser.parse_args()

    local = {
        "host": args.local_host,
        "port": args.local_port,
        "user": args.local_user,
        "password": args.local_password,
        "database": args.database,
    }
    remote = {
        "host": args.remote_host,
        "port": args.remote_port,
        "user": args.remote_user,
        "password": password("Remote MySQL", args.remote_password),
        "database": args.database,
    }

    with connect(local) as local_conn, connect(remote) as remote_conn:
        local_tables = set(table_names(local_conn))
        remote_tables = set(table_names(remote_conn))
        missing_local = sorted(remote_tables - local_tables)
        missing_remote = sorted(local_tables - remote_tables)
        print(f"local_tables={len(local_tables)} remote_tables={len(remote_tables)}")
        if missing_local:
            print("missing_on_local=" + ",".join(missing_local))
        if missing_remote:
            print("missing_on_remote=" + ",".join(missing_remote))

        for table_name in sorted(local_tables & remote_tables):
            local_count = row_count(local_conn, table_name)
            remote_count = row_count(remote_conn, table_name)
            status = "OK" if local_count == remote_count else "DIFF"
            print(f"{status} rows {table_name}: local={local_count} remote={remote_count}")

        for table_name, column_name in KEY_DATE_COLUMNS.items():
            if table_name not in local_tables or table_name not in remote_tables:
                continue
            local_value = max_date(local_conn, table_name, column_name)
            remote_value = max_date(remote_conn, table_name, column_name)
            status = "OK" if str(local_value) == str(remote_value) else "DIFF"
            print(f"{status} max_date {table_name}.{column_name}: local={local_value} remote={remote_value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
