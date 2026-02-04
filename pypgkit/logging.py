"""Logging configuration for pypgkit."""

from __future__ import annotations

import logging
import sys
from enum import Enum
from typing import TextIO


class LogLevel(Enum):
    """Log levels for pypgkit."""

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


# Default format for pypgkit logs
DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
SIMPLE_FORMAT = "[%(levelname)s] %(message)s"
DETAILED_FORMAT = (
    "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
)


def configure_logging(
    level: LogLevel | str = LogLevel.INFO,
    format: str = DEFAULT_FORMAT,
    stream: TextIO | None = None,
    filename: str | None = None,
    include_psycopg: bool = False,
) -> logging.Logger:
    """Configure logging for pypgkit.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format: Log format string
        stream: Stream to write logs to (default: sys.stderr)
        filename: Optional file to write logs to
        include_psycopg: If True, also configure psycopg/psycopg_pool loggers

    Returns:
        The configured pypgkit logger

    Example:
        from pypgkit.logging import configure_logging, LogLevel

        # Simple setup - INFO level to stderr
        configure_logging()

        # Debug level with detailed format
        configure_logging(level=LogLevel.DEBUG, format=DETAILED_FORMAT)

        # Log to file
        configure_logging(level=LogLevel.INFO, filename="pypgkit.log")
    """
    # Convert string level to LogLevel
    if isinstance(level, str):
        level = LogLevel[level.upper()]

    # Get the pypgkit logger
    logger = logging.getLogger("pypgkit")
    logger.setLevel(level.value)

    # Remove existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(format)

    # Add stream handler
    if stream is None:
        stream = sys.stderr
    stream_handler = logging.StreamHandler(stream)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # Add file handler if specified
    if filename:
        file_handler = logging.FileHandler(filename)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Optionally configure psycopg loggers
    if include_psycopg:
        for name in ("psycopg", "psycopg.pool"):
            psycopg_logger = logging.getLogger(name)
            psycopg_logger.setLevel(level.value)
            psycopg_logger.handlers.clear()
            psycopg_logger.addHandler(stream_handler)
            if filename:
                psycopg_logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "pypgkit") -> logging.Logger:
    """Get a pypgkit logger.

    Args:
        name: Logger name (will be prefixed with 'pypgkit.' if not already)

    Returns:
        Logger instance
    """
    if not name.startswith("pypgkit"):
        name = f"pypgkit.{name}"
    return logging.getLogger(name)


def set_level(level: LogLevel | str) -> None:
    """Set the log level for all pypgkit loggers.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    if isinstance(level, str):
        level = LogLevel[level.upper()]

    logger = logging.getLogger("pypgkit")
    logger.setLevel(level.value)


def enable_debug() -> None:
    """Enable debug logging for pypgkit."""
    set_level(LogLevel.DEBUG)


def disable_logging() -> None:
    """Disable all pypgkit logging."""
    logger = logging.getLogger("pypgkit")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
