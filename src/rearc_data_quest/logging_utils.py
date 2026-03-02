from __future__ import annotations

import logging
import os
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler
from pathlib import Path

_RUN_ID: ContextVar[str] = ContextVar("run_id", default="-")


def set_run_id(run_id: str) -> None:
    _RUN_ID.set(run_id)


def get_run_id() -> str:
    return _RUN_ID.get()


class _RunIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            record.run_id = get_run_id()
        return True


def _str_to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_file_logging_enabled() -> bool:
    configured = os.getenv("LOG_FILE_ENABLED")
    if configured is not None:
        return _str_to_bool(configured)
    return os.getenv("AWS_LAMBDA_FUNCTION_NAME") is None


def _build_handlers(level: int, formatter: logging.Formatter) -> list[logging.Handler]:
    run_id_filter = _RunIdFilter()
    handlers: list[logging.Handler] = []

    stream = logging.StreamHandler()
    stream.setLevel(level)
    stream.setFormatter(formatter)
    stream.addFilter(run_id_filter)
    handlers.append(stream)

    if _resolve_file_logging_enabled():
        file_path = Path(os.getenv("LOG_FILE_PATH", "logs/pipeline.log"))
        file_path.parent.mkdir(parents=True, exist_ok=True)
        max_bytes = int(os.getenv("LOG_FILE_MAX_BYTES", "5242880"))
        backup_count = int(os.getenv("LOG_FILE_BACKUP_COUNT", "5"))
        rotating = RotatingFileHandler(
            file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        rotating.setLevel(level)
        rotating.setFormatter(formatter)
        rotating.addFilter(run_id_filter)
        handlers.append(rotating)

    return handlers


def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    if getattr(configure_logging, "_configured", False):
        return

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s run_id=%(run_id)s - %(message)s"
    )

    root.handlers.clear()
    for handler in _build_handlers(level, formatter):
        root.addHandler(handler)

    configure_logging._configured = True
