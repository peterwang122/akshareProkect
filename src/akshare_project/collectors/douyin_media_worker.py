"""Download child: signed media URLs arrive only via private stdin."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from akshare_project.collectors.douyin_local_asr import download_official_video


def main():
    try:
        payload = json.loads(sys.stdin.read())
        result = download_official_video(payload["media_url"], payload["destination"], payload["source_url"])
        print(json.dumps({"status": "SUCCESS", "metadata": result}))
        return 0
    except Exception as exc:
        # No traceback, signed address, or request headers leave this worker.
        message = str(exc) if isinstance(exc, (ValueError, ConnectionError, TimeoutError)) else type(exc).__name__
        print(json.dumps({"status": "FAILED", "error": message}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
