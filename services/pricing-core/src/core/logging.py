import json
import logging
import os
from datetime import UTC, datetime

_LOGGING_CONFIGURED = False


def configure_logging(level: str | None = None):
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    resolved_level = getattr(logging, str(level or os.getenv("LOG_LEVEL", "INFO")).upper(), logging.INFO)
    logging.basicConfig(level=resolved_level, format="%(message)s")
    _LOGGING_CONFIGURED = True


def get_logger(name: str):
    configure_logging()
    return logging.getLogger(name)


def log_event(logger, level: int, event: str, **fields):
    payload = {
        "timestamp": datetime.now(UTC).replace(tzinfo=None).isoformat(),
        "level": logging.getLevelName(level),
        "logger": logger.name,
        "event": event,
        **fields,
    }
    logger.log(level, json.dumps(payload, ensure_ascii=False, default=str))
