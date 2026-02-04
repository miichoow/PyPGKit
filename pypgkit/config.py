"""Configuration management for pypgkit database framework."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

from .exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    """Database configuration with validation.

    Supports both connection string and individual parameters.
    Credentials are handled securely and not logged.
    """

    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    connection_string: str | None = None

    # Connection pool settings
    min_connections: int = 1
    max_connections: int = 10
    connection_timeout: float = 30.0

    # SSL settings
    sslmode: str = "prefer"

    # Pool health check settings
    check_connection: bool = True
    max_idle_time: float = 600.0  # 10 minutes

    # Additional connection options
    options: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        self._validate()

    def _validate(self) -> None:
        """Validate configuration values."""
        if self.port < 1 or self.port > 65535:
            raise ConfigurationError(f"Invalid port number: {self.port}")

        if self.min_connections < 1:
            raise ConfigurationError("min_connections must be at least 1")

        if self.max_connections < self.min_connections:
            raise ConfigurationError("max_connections must be >= min_connections")

        if self.connection_timeout <= 0:
            raise ConfigurationError("connection_timeout must be positive")

        valid_sslmodes = (
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        )
        if self.sslmode not in valid_sslmodes:
            raise ConfigurationError(
                f"Invalid sslmode: {self.sslmode}. Must be one of {valid_sslmodes}"
            )

    @classmethod
    def from_env(cls, prefix: str = "PYPGKIT_") -> DatabaseConfig:
        """Load configuration from environment variables.

        Args:
            prefix: Environment variable prefix (default: PYPGKIT_)

        Returns:
            DatabaseConfig instance populated from environment
        """
        load_dotenv()

        def get_env(key: str, default: str = "") -> str:
            return os.getenv(f"{prefix}{key}", default)

        def get_env_int(key: str, default: int) -> int:
            value = get_env(key)
            return int(value) if value else default

        def get_env_float(key: str, default: float) -> float:
            value = get_env(key)
            return float(value) if value else default

        def get_env_bool(key: str, default: bool) -> bool:
            value = get_env(key).lower()
            if value in ("true", "1", "yes"):
                return True
            if value in ("false", "0", "no"):
                return False
            return default

        # Check for connection string first
        connection_string = get_env("CONNECTION_STRING") or None

        return cls(
            host=get_env("HOST", "localhost"),
            port=get_env_int("PORT", 5432),
            database=get_env("DATABASE", "postgres"),
            user=get_env("USER", "postgres"),
            password=get_env("PASSWORD", ""),
            connection_string=connection_string,
            min_connections=get_env_int("MIN_CONNECTIONS", 1),
            max_connections=get_env_int("MAX_CONNECTIONS", 10),
            connection_timeout=get_env_float("CONNECTION_TIMEOUT", 30.0),
            sslmode=get_env("SSLMODE", "prefer"),
            check_connection=get_env_bool("CHECK_CONNECTION", True),
            max_idle_time=get_env_float("MAX_IDLE_TIME", 600.0),
        )

    def get_connection_kwargs(self) -> dict:
        """Get connection parameters as a dictionary.

        Returns:
            Dictionary of connection parameters for psycopg
        """
        if self.connection_string:
            return {"conninfo": self.connection_string}

        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
            **self.options,
        }

    def __repr__(self) -> str:
        """Safe representation without password."""
        if self.connection_string:
            # Mask password in connection string
            return "DatabaseConfig(connection_string='***masked***')"
        return (
            f"DatabaseConfig(host='{self.host}', port={self.port}, "
            f"database='{self.database}', user='{self.user}', password='***')"
        )
