"""Tests for pypgkit.connection module."""

from unittest import mock

import pytest

from pypgkit.config import DatabaseConfig
from pypgkit.connection import ConnectionPoolSingleton, get_pool
from pypgkit.exceptions import DatabaseConnectionError


class TestConnectionPoolSingleton:
    """Tests for ConnectionPoolSingleton class."""

    def setup_method(self):
        """Reset singleton before each test."""
        ConnectionPoolSingleton.reset()

    def teardown_method(self):
        """Clean up after each test."""
        ConnectionPoolSingleton.reset()

    def test_singleton_requires_config(self):
        """Test singleton raises without config on first call."""
        with pytest.raises(DatabaseConnectionError, match="Configuration required"):
            ConnectionPoolSingleton()

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_singleton_creates_pool(self, mock_pool_class):
        """Test singleton creates pool on first call."""
        mock_pool = mock.MagicMock()
        mock_pool_class.return_value = mock_pool

        config = DatabaseConfig()
        pool = ConnectionPoolSingleton(config)

        mock_pool_class.assert_called_once()
        mock_pool.wait.assert_called_once()
        assert pool._pool is mock_pool

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_singleton_returns_same_instance(self, mock_pool_class):
        """Test singleton returns same instance."""
        mock_pool_class.return_value = mock.MagicMock()

        config = DatabaseConfig()
        pool1 = ConnectionPoolSingleton(config)
        pool2 = ConnectionPoolSingleton(config)

        assert pool1 is pool2

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_singleton_ignores_config_after_init(self, mock_pool_class):
        """Test singleton ignores config after initialization."""
        mock_pool_class.return_value = mock.MagicMock()

        config1 = DatabaseConfig(database="db1")
        config2 = DatabaseConfig(database="db2")

        pool1 = ConnectionPoolSingleton(config1)
        pool2 = ConnectionPoolSingleton(config2)

        assert pool1 is pool2
        assert pool1._config.database == "db1"

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_reset_clears_singleton(self, mock_pool_class):
        """Test reset clears singleton."""
        mock_pool = mock.MagicMock()
        mock_pool_class.return_value = mock_pool

        config = DatabaseConfig()
        pool1 = ConnectionPoolSingleton(config)

        ConnectionPoolSingleton.reset()

        pool2 = ConnectionPoolSingleton(config)
        assert pool1 is not pool2

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_close(self, mock_pool_class):
        """Test close method."""
        mock_pool = mock.MagicMock()
        mock_pool_class.return_value = mock_pool

        config = DatabaseConfig()
        pool = ConnectionPoolSingleton(config)
        pool.close()

        mock_pool.close.assert_called_once()

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_close_idempotent(self, mock_pool_class):
        """Test close is idempotent."""
        mock_pool = mock.MagicMock()
        mock_pool_class.return_value = mock_pool

        config = DatabaseConfig()
        pool = ConnectionPoolSingleton(config)
        pool.close()
        pool.close()

        # Should only close once
        assert mock_pool.close.call_count == 1

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_pool_property(self, mock_pool_class):
        """Test pool property returns underlying pool."""
        mock_pool = mock.MagicMock()
        mock_pool_class.return_value = mock_pool

        config = DatabaseConfig()
        singleton = ConnectionPoolSingleton(config)

        assert singleton.pool is mock_pool

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_pool_property_raises_when_closed(self, mock_pool_class):
        """Test pool property raises when pool is closed."""
        mock_pool = mock.MagicMock()
        mock_pool_class.return_value = mock_pool

        config = DatabaseConfig()
        singleton = ConnectionPoolSingleton(config)
        singleton.close()

        with pytest.raises(DatabaseConnectionError, match="not initialized"):
            _ = singleton.pool

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_get_stats(self, mock_pool_class):
        """Test get_stats method."""
        mock_pool = mock.MagicMock()
        mock_pool.get_stats.return_value = {
            "pool_min": 1,
            "pool_max": 10,
            "pool_size": 5,
            "pool_available": 3,
            "requests_waiting": 0,
        }
        mock_pool_class.return_value = mock_pool

        config = DatabaseConfig()
        singleton = ConnectionPoolSingleton(config)
        stats = singleton.get_stats()

        assert stats["pool_min"] == 1
        assert stats["pool_max"] == 10

    def test_get_stats_not_initialized(self):
        """Test get_stats when not initialized."""
        # Create a real instance with mocked pool creation, then set _pool to None
        with mock.patch("pypgkit.connection.ConnectionPool") as mock_pool_class:
            mock_pool_class.return_value = mock.MagicMock()
            config = DatabaseConfig()
            singleton = ConnectionPoolSingleton(config)
            # Simulate uninitialized pool
            singleton._pool = None

            stats = singleton.get_stats()
            assert stats == {"status": "not_initialized"}

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_pool_creation_failure(self, mock_pool_class):
        """Test handling of pool creation failure."""
        mock_pool_class.side_effect = Exception("Connection failed")

        config = DatabaseConfig()
        with pytest.raises(DatabaseConnectionError, match="Failed to create"):
            ConnectionPoolSingleton(config)


class TestGetPool:
    """Tests for get_pool function."""

    def setup_method(self):
        """Reset singleton before each test."""
        ConnectionPoolSingleton.reset()

    def teardown_method(self):
        """Clean up after each test."""
        ConnectionPoolSingleton.reset()

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_get_pool_returns_singleton(self, mock_pool_class):
        """Test get_pool returns singleton instance."""
        mock_pool_class.return_value = mock.MagicMock()

        config = DatabaseConfig()
        pool = get_pool(config)

        assert isinstance(pool, ConnectionPoolSingleton)

    @mock.patch("pypgkit.connection.ConnectionPool")
    def test_get_pool_same_instance(self, mock_pool_class):
        """Test get_pool returns same instance on subsequent calls."""
        mock_pool_class.return_value = mock.MagicMock()

        config = DatabaseConfig()
        pool1 = get_pool(config)
        pool2 = get_pool()

        assert pool1 is pool2
