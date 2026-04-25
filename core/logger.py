"""
Streamd BI - Unified Logging Module
Provides consistent logging across all modules with file rotation.
"""
import os
import logging
from logging.handlers import RotatingFileHandler

_LOG_FORMAT = "[%(asctime)s] [%(name)s] %(levelname)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_MAX_BYTES = 5 * 1024 * 1024  # 5 MB per file
_BACKUP_COUNT = 3

_initialized = False


def setup_logging(base_dir: str, level: int = logging.INFO) -> None:
    """Initialize the global logging configuration. Call once at startup."""
    global _initialized
    if _initialized:
        return

    log_path = os.path.join(base_dir, "streamd_bi.log")

    root = logging.getLogger()
    root.setLevel(level)

    # File handler with rotation
    fh = RotatingFileHandler(
        log_path, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT, encoding="utf-8"
    )
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(ch)

    _initialized = True


def get_logger(name: str) -> logging.Logger:
    """Get a named logger. Usage: logger = get_logger(__name__)"""
    return logging.getLogger(name)
