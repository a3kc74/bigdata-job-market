import logging
import os
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Logger dùng chung cho toàn hệ thống.

    - Ghi ra stdout để cron/K8s đọc được.
    - Ghi vào runtime/logs/<name>.log để debug local/server.
    - Tránh add handler lặp nhiều lần khi module bị import lại.
    """
    os.makedirs("runtime/logs", exist_ok=True)

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(
        f"runtime/logs/{name}.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger