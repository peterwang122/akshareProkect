from .config import load_scheduler_config


def run_healthcheck(*args, **kwargs):
    from .service import run_healthcheck as _run_healthcheck

    return _run_healthcheck(*args, **kwargs)


def run_scheduler_service(*args, **kwargs):
    from .service import run_scheduler_service as _run_scheduler_service

    return _run_scheduler_service(*args, **kwargs)

__all__ = ["load_scheduler_config", "run_healthcheck", "run_scheduler_service"]
