"""Custom exceptions for the pypgkit database framework."""


class PyPgKitError(Exception):
    """Base exception for all pypgkit errors."""

    pass


class DatabaseConnectionError(PyPgKitError):
    """Raised when a database connection cannot be established or is lost."""

    pass


class SchemaError(PyPgKitError):
    """Raised when schema operations fail."""

    pass


class UserManagementError(PyPgKitError):
    """Raised when user management operations fail."""

    pass


class RepositoryError(PyPgKitError):
    """Raised when repository operations fail."""

    pass


class ConfigurationError(PyPgKitError):
    """Raised when configuration is invalid or missing."""

    pass
