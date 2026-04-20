from __future__ import annotations

import logging
import os


DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_CONFIGURED = False


def setup_logging(level: str | int | None = None) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    resolved_level = level or os.environ.get("MARKET_INTEL_LOG_LEVEL", "INFO")
    if isinstance(resolved_level, str):
        resolved_level = resolved_level.upper()

    logging.basicConfig(level=resolved_level, format=DEFAULT_FORMAT)
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
