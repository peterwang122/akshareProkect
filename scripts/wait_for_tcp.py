import argparse
import os
import socket
import sys
import time


def _parse_endpoint(value: str) -> tuple[str, int]:
    host, separator, port_text = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError(f"invalid endpoint: {value}")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid endpoint: {value}") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(f"invalid endpoint: {value}")
    return host, port


def main() -> int:
    if "--" not in sys.argv:
        raise SystemExit("usage: wait_for_tcp.py [options] host:port [...] -- command [args ...]")
    separator_index = sys.argv.index("--")
    wait_args = sys.argv[1:separator_index]
    command = sys.argv[separator_index + 1 :]
    if not command:
        raise SystemExit("command is required after --")

    parser = argparse.ArgumentParser(description="Wait for TCP dependencies before starting a service.")
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("endpoints", nargs="+", type=_parse_endpoint)
    args = parser.parse_args(wait_args)

    started_at = time.monotonic()
    last_report_at = 0.0
    while True:
        pending: list[str] = []
        for host, port in args.endpoints:
            try:
                with socket.create_connection((host, port), timeout=1.0):
                    pass
            except OSError:
                pending.append(f"{host}:{port}")
        if not pending:
            os.execv(command[0], command)

        elapsed = time.monotonic() - started_at
        if elapsed >= args.timeout:
            print(f"dependency wait timed out after {elapsed:.1f}s: {', '.join(pending)}", file=sys.stderr)
            return 75
        if elapsed - last_report_at >= 15.0 or last_report_at == 0.0:
            print(f"waiting for dependencies: {', '.join(pending)}", file=sys.stderr, flush=True)
            last_report_at = elapsed
        time.sleep(max(0.2, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
