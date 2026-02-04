"""Tests for pypgkit.logging module."""

import logging
from io import StringIO

from pypgkit.logging import (
    DEFAULT_FORMAT,
    DETAILED_FORMAT,
    SIMPLE_FORMAT,
    LogLevel,
    configure_logging,
    disable_logging,
    enable_debug,
    get_logger,
    set_level,
)


class TestLogLevel:
    """Tests for LogLevel enum."""

    def test_log_levels_exist(self):
        """Test all log levels exist."""
        assert LogLevel.DEBUG.value == logging.DEBUG
        assert LogLevel.INFO.value == logging.INFO
        assert LogLevel.WARNING.value == logging.WARNING
        assert LogLevel.ERROR.value == logging.ERROR
        assert LogLevel.CRITICAL.value == logging.CRITICAL


class TestConfigureLogging:
    """Tests for configure_logging function."""

    def setup_method(self):
        """Reset logging before each test."""
        logger = logging.getLogger("pypgkit")
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)

    def test_configure_logging_default(self):
        """Test default logging configuration."""
        logger = configure_logging()
        assert logger.name == "pypgkit"
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1

    def test_configure_logging_debug_level(self):
        """Test debug level configuration."""
        logger = configure_logging(level=LogLevel.DEBUG)
        assert logger.level == logging.DEBUG

    def test_configure_logging_string_level(self):
        """Test string level configuration."""
        logger = configure_logging(level="WARNING")
        assert logger.level == logging.WARNING

    def test_configure_logging_custom_format(self):
        """Test custom format configuration."""
        logger = configure_logging(format=SIMPLE_FORMAT)
        assert len(logger.handlers) == 1

    def test_configure_logging_to_stream(self):
        """Test logging to custom stream."""
        stream = StringIO()
        logger = configure_logging(level=LogLevel.INFO, stream=stream)
        logger.info("test message")
        output = stream.getvalue()
        assert "test message" in output

    def test_configure_logging_replaces_handlers(self):
        """Test that configure_logging replaces existing handlers."""
        configure_logging()
        configure_logging()
        logger = logging.getLogger("pypgkit")
        # Should only have one handler, not two
        assert len(logger.handlers) == 1


class TestGetLogger:
    """Tests for get_logger function."""

    def test_get_logger_default(self):
        """Test getting default logger."""
        logger = get_logger()
        assert logger.name == "pypgkit"

    def test_get_logger_with_name(self):
        """Test getting named logger."""
        logger = get_logger("mymodule")
        assert logger.name == "pypgkit.mymodule"

    def test_get_logger_with_pgkit_prefix(self):
        """Test logger name already has pypgkit prefix."""
        logger = get_logger("pypgkit.test")
        assert logger.name == "pypgkit.test"


class TestSetLevel:
    """Tests for set_level function."""

    def setup_method(self):
        """Setup logging before each test."""
        configure_logging(level=LogLevel.INFO)

    def test_set_level_enum(self):
        """Test setting level with enum."""
        set_level(LogLevel.DEBUG)
        logger = logging.getLogger("pypgkit")
        assert logger.level == logging.DEBUG

    def test_set_level_string(self):
        """Test setting level with string."""
        set_level("ERROR")
        logger = logging.getLogger("pypgkit")
        assert logger.level == logging.ERROR


class TestEnableDebug:
    """Tests for enable_debug function."""

    def setup_method(self):
        """Setup logging before each test."""
        configure_logging(level=LogLevel.INFO)

    def test_enable_debug(self):
        """Test enable_debug sets DEBUG level."""
        enable_debug()
        logger = logging.getLogger("pypgkit")
        assert logger.level == logging.DEBUG


class TestDisableLogging:
    """Tests for disable_logging function."""

    def setup_method(self):
        """Setup logging before each test."""
        configure_logging(level=LogLevel.INFO)

    def test_disable_logging(self):
        """Test disable_logging removes handlers."""
        disable_logging()
        logger = logging.getLogger("pypgkit")
        # Should have NullHandler
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.NullHandler)


class TestFormatConstants:
    """Tests for format constants."""

    def test_default_format_has_timestamp(self):
        """Test DEFAULT_FORMAT includes timestamp."""
        assert "asctime" in DEFAULT_FORMAT

    def test_simple_format_no_timestamp(self):
        """Test SIMPLE_FORMAT has no timestamp."""
        assert "asctime" not in SIMPLE_FORMAT

    def test_detailed_format_has_lineno(self):
        """Test DETAILED_FORMAT includes line number."""
        assert "lineno" in DETAILED_FORMAT
