import asyncio
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from akshare_project.collectors import (  # noqa: E402
    cffex,
    douyin_emotion,
    etf,
    excel_emotion,
    failed_tasks,
    forex,
    futures,
    index,
    option,
    runner,
    stock,
)
from akshare_project.core.paths import ensure_runtime_layout  # noqa: E402


def set_argv(label, args):
    sys.argv = [label, *args]


async def dispatch():
    ensure_runtime_layout()

    if len(sys.argv) < 3:
        raise ValueError(
            "usage: python run.py <domain> <command> [args]\n"
            "domains: stock, index, cffex, douyin, forex, futures, etf, option, runner, emotion-excel"
        )

    domain = sys.argv[1].strip().lower()
    command = sys.argv[2].strip().lower()
    args = sys.argv[3:]

    if domain == "stock":
        set_argv("stock", [command, *args])
        await stock.main()
        return
    if domain == "index":
        set_argv("index", [command, *args])
        await index.main()
        return
    if domain == "cffex":
        set_argv("cffex", [command, *args])
        await cffex.main()
        return
    if domain == "douyin":
        set_argv("douyin", [command, *args])
        await douyin_emotion.main()
        return
    if domain == "forex":
        set_argv("forex", [command, *args])
        await forex.main()
        return
    if domain == "futures":
        set_argv("futures", [command, *args])
        await futures.main()
        return
    if domain == "etf":
        set_argv("etf", [command, *args])
        await etf.main()
        return
    if domain == "option":
        set_argv("option", [command, *args])
        await option.main()
        return
    if domain == "runner":
        if command == "daily":
            set_argv("runner", [])
            await runner.main()
            return
        if command == "retry-failures":
            set_argv("failed_tasks", args)
            await failed_tasks.main()
            return
        raise ValueError("runner supports: daily | retry-failures [task_name] [limit]")
    if domain == "emotion-excel":
        if command != "import":
            raise ValueError("emotion-excel supports: import [xlsx_path]")
        set_argv("emotion_excel", args)
        await excel_emotion.run()
        return

    raise ValueError(f"unsupported domain: {domain}")


if __name__ == "__main__":
    asyncio.run(dispatch())
