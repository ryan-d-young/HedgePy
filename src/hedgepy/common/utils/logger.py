import logging
import datetime
from pathlib import Path

from hedgepy.common.utils import config


__all__ = ["get"]


file_formatter = logging.Formatter(
    fmt="%(asctime)s | %(name)s.%(funcName)s:%(lineno)d | %(levelname)s: %(message)s",
    datefmt="%H:%M:%S")
print_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s: %(message)s",
    datefmt="%H:%M:%S")

logfile = Path(config.PROJECT_ROOT) / "logs" / f"{datetime.datetime.now().strftime('%Y-%m-%d')}.log"

file_handler = logging.FileHandler(logfile)
file_handler.setFormatter(file_formatter)
file_handler.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(print_formatter)
stream_handler.setLevel(logging.DEBUG)


def get(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.DEBUG)
    return logger
    