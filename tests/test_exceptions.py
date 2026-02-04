"""Tests for pypgkit.exceptions module."""

import pytest

from pypgkit.exceptions import (
    ConfigurationError,
    DatabaseConnectionError,
    PyPgKitError,
    RepositoryError,
    SchemaError,
    UserManagementError,
)


class TestExceptions:
    """Tests for custom exceptions."""

    def test_pgkit_error_is_base(self):
        """Test PyPgKitError is the base exception."""
        assert issubclass(DatabaseConnectionError, PyPgKitError)
        assert issubclass(SchemaError, PyPgKitError)
        assert issubclass(UserManagementError, PyPgKitError)
        assert issubclass(RepositoryError, PyPgKitError)
        assert issubclass(ConfigurationError, PyPgKitError)

    def test_pgkit_error_can_be_raised(self):
        """Test PyPgKitError can be raised and caught."""
        with pytest.raises(PyPgKitError):
            raise PyPgKitError("test error")

    def test_database_connection_error(self):
        """Test DatabaseConnectionError."""
        with pytest.raises(DatabaseConnectionError) as exc_info:
            raise DatabaseConnectionError("connection failed")
        assert "connection failed" in str(exc_info.value)

    def test_schema_error(self):
        """Test SchemaError."""
        with pytest.raises(SchemaError) as exc_info:
            raise SchemaError("schema error")
        assert "schema error" in str(exc_info.value)

    def test_repository_error(self):
        """Test RepositoryError."""
        with pytest.raises(RepositoryError) as exc_info:
            raise RepositoryError("repository error")
        assert "repository error" in str(exc_info.value)

    def test_configuration_error(self):
        """Test ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            raise ConfigurationError("config error")
        assert "config error" in str(exc_info.value)

    def test_user_management_error(self):
        """Test UserManagementError."""
        with pytest.raises(UserManagementError) as exc_info:
            raise UserManagementError("user error")
        assert "user error" in str(exc_info.value)

    def test_catch_all_with_pgkit_error(self):
        """Test that all exceptions can be caught with PyPgKitError."""
        exceptions = [
            DatabaseConnectionError("test"),
            SchemaError("test"),
            RepositoryError("test"),
            ConfigurationError("test"),
            UserManagementError("test"),
        ]
        for exc in exceptions:
            with pytest.raises(PyPgKitError):
                raise exc
