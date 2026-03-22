import builtins
import logging
from typing import Any

from .paths import get_log_path


def get_logger(module_name: str) -> logging.Logger:
    logger_name = f"akshare_project.{module_name}"
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    file_handler = logging.FileHandler(get_log_path(module_name), encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)
    return logger


def infer_log_level(message: str) -> int:
    text = str(message or "").lower()
    if any(token in text for token in ("failed", "error", "exception", "traceback")):
        return logging.ERROR
    if any(token in text for token in ("warning", "retry", "fallback", "skip", "skipped")):
        return logging.WARNING
    return logging.INFO


def echo_and_log(logger: logging.Logger, *args: Any, level: int | None = None, **kwargs: Any) -> None:
    builtins.print(*args, **kwargs)
    sep = kwargs.get("sep", " ")
    message = sep.join(str(arg) for arg in args)
    logger.log(level or infer_log_level(message), message)
