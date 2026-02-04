"""Tests for pypgkit.config module."""

import os
from unittest import mock

import pytest

from pypgkit.config import DatabaseConfig
from pypgkit.exceptions import ConfigurationError


class TestDatabaseConfig:
    """Tests for DatabaseConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = DatabaseConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "postgres"
        assert config.user == "postgres"
        assert config.password == ""
        assert config.min_connections == 1
        assert config.max_connections == 10
        assert config.sslmode == "prefer"

    def test_custom_values(self):
        """Test custom configuration values."""
        config = DatabaseConfig(
            host="dbhost",
            port=5433,
            database="mydb",
            user="myuser",
            password="secret",
            min_connections=5,
            max_connections=20,
        )
        assert config.host == "dbhost"
        assert config.port == 5433
        assert config.database == "mydb"
        assert config.user == "myuser"
        assert config.password == "secret"
        assert config.min_connections == 5
        assert config.max_connections == 20

    def test_invalid_port_zero(self):
        """Test that port 0 raises error."""
        with pytest.raises(ConfigurationError, match="Invalid port"):
            DatabaseConfig(port=0)

    def test_invalid_port_negative(self):
        """Test that negative port raises error."""
        with pytest.raises(ConfigurationError, match="Invalid port"):
            DatabaseConfig(port=-1)

    def test_invalid_port_too_high(self):
        """Test that port > 65535 raises error."""
        with pytest.raises(ConfigurationError, match="Invalid port"):
            DatabaseConfig(port=70000)

    def test_invalid_min_connections(self):
        """Test that min_connections < 1 raises error."""
        with pytest.raises(ConfigurationError, match="min_connections"):
            DatabaseConfig(min_connections=0)

    def test_invalid_max_connections(self):
        """Test that max_connections < min_connections raises error."""
        with pytest.raises(ConfigurationError, match="max_connections"):
            DatabaseConfig(min_connections=10, max_connections=5)

    def test_invalid_connection_timeout(self):
        """Test that connection_timeout <= 0 raises error."""
        with pytest.raises(ConfigurationError, match="connection_timeout"):
            DatabaseConfig(connection_timeout=0)

    def test_invalid_sslmode(self):
        """Test that invalid sslmode raises error."""
        with pytest.raises(ConfigurationError, match="Invalid sslmode"):
            DatabaseConfig(sslmode="invalid")

    def test_valid_sslmodes(self):
        """Test all valid sslmode values."""
        valid_modes = [
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ]
        for mode in valid_modes:
            config = DatabaseConfig(sslmode=mode)
            assert config.sslmode == mode

    def test_get_connection_kwargs(self):
        """Test get_connection_kwargs returns correct dict."""
        config = DatabaseConfig(
            host="myhost",
            port=5433,
            database="mydb",
            user="myuser",
            password="secret",
        )
        kwargs = config.get_connection_kwargs()
        assert kwargs["host"] == "myhost"
        assert kwargs["port"] == 5433
        assert kwargs["dbname"] == "mydb"
        assert kwargs["user"] == "myuser"
        assert kwargs["password"] == "secret"
        assert kwargs["sslmode"] == "prefer"

    def test_get_connection_kwargs_with_connection_string(self):
        """Test that connection string takes precedence."""
        config = DatabaseConfig(
            connection_string="postgresql://user:pass@host:5432/db",
            host="otherhost",
        )
        kwargs = config.get_connection_kwargs()
        assert "conninfo" in kwargs
        assert kwargs["conninfo"] == "postgresql://user:pass@host:5432/db"

    def test_repr_hides_password(self):
        """Test that repr doesn't expose password."""
        config = DatabaseConfig(password="supersecret")
        repr_str = repr(config)
        assert "supersecret" not in repr_str
        assert "***" in repr_str

    def test_repr_hides_connection_string(self):
        """Test that repr masks connection string."""
        config = DatabaseConfig(connection_string="postgresql://user:pass@host/db")
        repr_str = repr(config)
        assert "pass" not in repr_str
        assert "masked" in repr_str.lower()

    def test_from_env_defaults(self):
        """Test from_env with no environment variables."""
        with mock.patch.dict(os.environ, {}, clear=True):
            config = DatabaseConfig.from_env()
            assert config.host == "localhost"
            assert config.port == 5432

    def test_from_env_with_variables(self):
        """Test from_env reads environment variables."""
        env = {
            "PYPGKIT_HOST": "envhost",
            "PYPGKIT_PORT": "5433",
            "PYPGKIT_DATABASE": "envdb",
            "PYPGKIT_USER": "envuser",
            "PYPGKIT_PASSWORD": "envpass",
            "PYPGKIT_MIN_CONNECTIONS": "2",
            "PYPGKIT_MAX_CONNECTIONS": "15",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = DatabaseConfig.from_env()
            assert config.host == "envhost"
            assert config.port == 5433
            assert config.database == "envdb"
            assert config.user == "envuser"
            assert config.password == "envpass"
            assert config.min_connections == 2
            assert config.max_connections == 15

    def test_from_env_custom_prefix(self):
        """Test from_env with custom prefix."""
        env = {
            "MYAPP_HOST": "customhost",
            "MYAPP_DATABASE": "customdb",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = DatabaseConfig.from_env(prefix="MYAPP_")
            assert config.host == "customhost"
            assert config.database == "customdb"

    def test_from_env_connection_string(self):
        """Test from_env with connection string."""
        env = {
            "PYPGKIT_CONNECTION_STRING": "postgresql://user:pass@host/db",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            config = DatabaseConfig.from_env()
            assert config.connection_string == "postgresql://user:pass@host/db"

    def test_from_env_boolean_values(self):
        """Test from_env parses boolean values correctly."""
        for true_val in ["true", "1", "yes", "TRUE", "True"]:
            with mock.patch.dict(
                os.environ, {"PYPGKIT_CHECK_CONNECTION": true_val}, clear=True
            ):
                config = DatabaseConfig.from_env()
                assert config.check_connection is True

        for false_val in ["false", "0", "no", "FALSE", "False"]:
            with mock.patch.dict(
                os.environ, {"PYPGKIT_CHECK_CONNECTION": false_val}, clear=True
            ):
                config = DatabaseConfig.from_env()
                assert config.check_connection is False
