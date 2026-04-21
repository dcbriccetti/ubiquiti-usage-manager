"""Shared logging configuration helpers for app and scripts."""

import logging
import os

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def configure_logging(default_level: str = "INFO") -> None:
    """Configure root logging once; honor LOG_LEVEL env var when present."""
    level_name = os.getenv("LOG_LEVEL", default_level).upper()
    level = getattr(logging, level_name, logging.INFO)

    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.setLevel(level)
        return

    logging.basicConfig(level=level, format=DEFAULT_LOG_FORMAT)
