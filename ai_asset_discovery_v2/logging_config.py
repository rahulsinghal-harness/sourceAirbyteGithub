import json
import logging
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, default=str)


def setup_logging(verbose: bool = False, airbyte_mode: bool = False) -> None:
    """Configure logging for CLI or Airbyte mode.

    Args:
        verbose: Enable DEBUG level logging.
        airbyte_mode: If True, use JSON formatter and log only to stderr.
    """
    level = logging.DEBUG if verbose else logging.INFO

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    if airbyte_mode:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JSONFormatter())
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ))

    root_logger.addHandler(handler)
