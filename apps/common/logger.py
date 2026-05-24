"""Shared logging helper for ingestion, batch, and speed modules."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path


def _resolve_log_dir() -> Path:
    log_dir = Path(os.getenv("CRAWLER_LOG_DIR", "runtime/logs"))

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir
    except PermissionError:
        fallback_dir = Path("/tmp/topcv-crawler-logs")
        fallback_dir.mkdir(parents=True, exist_ok=True)
        return fallback_dir


def get_logger(name: str) -> logging.Logger:
    """Return a logger that writes to stdout and a local log file.

    The file log directory defaults to runtime/logs. In Kubernetes, the runtime
    directory is mounted as a PVC. If the process cannot write there, logs fall
    back to /tmp/topcv-crawler-logs.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_dir = _resolve_log_dir()
    file_handler = logging.FileHandler(
        log_dir / f"{name}.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger