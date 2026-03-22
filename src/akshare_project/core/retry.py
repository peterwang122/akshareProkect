import random
import time
from typing import Callable

from .logging_utils import echo_and_log


def default_error_classifier(exc) -> str:
    return "unexpected"


def fetch_with_retry(
    func,
    *args,
    retries=5,
    sleep_seconds=3,
    logger=None,
    classify_error: Callable | None = None,
    sleep_cap_seconds: float | None = None,
    jitter_max_seconds: float = 0.0,
    backoff: str = "fixed",
    **kwargs,
):
    last_error = None
    classifier = classify_error or default_error_classifier

    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break

            wait_seconds = float(sleep_seconds)
            if backoff == "exponential":
                wait_seconds *= 2 ** (attempt - 1)
            if sleep_cap_seconds is not None:
                wait_seconds = min(wait_seconds, float(sleep_cap_seconds))
            if jitter_max_seconds:
                wait_seconds += random.uniform(0, float(jitter_max_seconds))

            category = classifier(exc)
            message = (
                f"{getattr(func, '__name__', 'callable')} attempt {attempt}/{retries} failed "
                f"[{category}]: {exc}; retrying in {wait_seconds:.1f}s"
            )
            if logger is not None:
                echo_and_log(logger, message)
            else:
                print(message)
            time.sleep(wait_seconds)

    raise last_error
