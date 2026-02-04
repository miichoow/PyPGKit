"""Tests for pypgkit.database module."""

from unittest import mock

import pytest

from pypgkit.config import DatabaseConfig
from pypgkit.database import Database
from pypgkit.exceptions import DatabaseConnectionError


class TestDatabase:
    """Tests for Database class."""

    def setup_method(self):
        """Reset singleton before each test."""
        Database.reset_instance()

    def teardown_method(self):
        """Clean up after each test."""
        Database.reset_instance()

    def test_init_with_config(self):
        """Test Database initialization with config."""
        config = DatabaseConfig(
            host="localhost",
            database="testdb",
            user="testuser",
            password="testpass",
        )
        db = Database(config)
        assert db._config == config
        assert db._pool is None

    def test_init_without_config_uses_env(self):
        """Test Database initialization loads from env."""
        with mock.patch.dict(
            "os.environ",
            {"PYPGKIT_DATABASE": "envdb", "PYPGKIT_USER": "envuser"},
            clear=True,
        ):
            db = Database()
            assert db._config.database == "envdb"
            assert db._config.user == "envuser"

    def test_get_instance_before_init_raises(self):
        """Test get_instance raises if not initialized."""
        with pytest.raises(DatabaseConnectionError, match="not initialized"):
            Database.get_instance()

    def test_is_initialized_false_by_default(self):
        """Test is_initialized returns False by default."""
        assert Database.is_initialized() is False

    @mock.patch("pypgkit.database.get_pool")
    @mock.patch("pypgkit.setup.ensure_database")
    def test_init_creates_singleton(self, _mock_ensure, _mock_pool):
        """Test init() creates and stores singleton."""
        config = DatabaseConfig()
        db = Database.init(config=config, auto_setup=True)

        assert Database.is_initialized() is True
        assert Database.get_instance() is db

    @mock.patch("pypgkit.database.get_pool")
    @mock.patch("pypgkit.setup.ensure_database")
    def test_init_returns_existing_instance(self, mock_ensure, _mock_pool):
        """Test init() returns existing instance if already initialized."""
        config = DatabaseConfig()

        db1 = Database.init(config=config)
        db2 = Database.init(config=config)

        assert db1 is db2
        # ensure_database should only be called once
        assert mock_ensure.call_count == 1

    @mock.patch("pypgkit.database.get_pool")
    def test_init_without_auto_setup(self, _mock_pool):
        """Test init() with auto_setup=False."""
        config = DatabaseConfig()
        with mock.patch("pypgkit.setup.check_connection", return_value=True):
            db = Database.init(config=config, auto_setup=False)
            assert db is not None

    def test_reset_instance(self):
        """Test reset_instance clears singleton."""
        with (
            mock.patch("pypgkit.database.get_pool"),
            mock.patch("pypgkit.setup.ensure_database"),
        ):
            Database.init(config=DatabaseConfig())
            assert Database.is_initialized() is True

            Database.reset_instance()
            assert Database.is_initialized() is False

    @mock.patch("pypgkit.database.get_pool")
    def test_connect(self, mock_pool):
        """Test connect creates pool."""
        config = DatabaseConfig()
        db = Database(config)
        assert db._pool is None

        db.connect()
        mock_pool.assert_called_once_with(config)
        assert db._pool is not None

    @mock.patch("pypgkit.database.get_pool")
    def test_connect_idempotent(self, mock_pool):
        """Test connect is idempotent."""
        config = DatabaseConfig()
        db = Database(config)

        db.connect()
        db.connect()

        # Should only create pool once
        assert mock_pool.call_count == 1


class TestDatabaseQueries:
    """Tests for Database query methods using mocks."""

    def setup_method(self):
        """Setup mock database."""
        Database.reset_instance()
        self.mock_pool = mock.MagicMock()
        self.mock_conn = mock.MagicMock()
        self.mock_cursor = mock.MagicMock()

        # Setup context managers
        self.mock_pool.connection.return_value.__enter__ = mock.MagicMock(
            return_value=self.mock_conn
        )
        self.mock_pool.connection.return_value.__exit__ = mock.MagicMock(
            return_value=False
        )
        self.mock_conn.cursor.return_value.__enter__ = mock.MagicMock(
            return_value=self.mock_cursor
        )
        self.mock_conn.cursor.return_value.__exit__ = mock.MagicMock(return_value=False)

    def teardown_method(self):
        """Clean up."""
        Database.reset_instance()

    def _get_db(self):
        """Get a Database with mocked pool."""
        config = DatabaseConfig()
        db = Database(config)
        db._pool = self.mock_pool
        return db

    def test_execute(self):
        """Test execute method."""
        db = self._get_db()
        self.mock_cursor.rowcount = 5

        result = db.execute("UPDATE users SET active = %s", (True,))

        self.mock_cursor.execute.assert_called_once()
        assert result == 5

    def test_fetch_one(self):
        """Test fetch_one method."""
        db = self._get_db()
        self.mock_cursor.fetchone.return_value = (1, "test@example.com")

        result = db.fetch_one("SELECT * FROM users WHERE id = %s", (1,))

        assert result == (1, "test@example.com")

    def test_fetch_one_no_result(self):
        """Test fetch_one returns None when no result."""
        db = self._get_db()
        self.mock_cursor.fetchone.return_value = None

        result = db.fetch_one("SELECT * FROM users WHERE id = %s", (999,))

        assert result is None

    def test_fetch_all(self):
        """Test fetch_all method."""
        db = self._get_db()
        self.mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]

        result = db.fetch_all("SELECT * FROM users")

        assert result == [(1, "a"), (2, "b")]

    def test_fetch_value(self):
        """Test fetch_value method."""
        db = self._get_db()
        self.mock_cursor.fetchone.return_value = (42,)

        result = db.fetch_value("SELECT COUNT(*) FROM users")

        assert result == 42

    def test_fetch_value_no_result(self):
        """Test fetch_value returns None when no result."""
        db = self._get_db()
        self.mock_cursor.fetchone.return_value = None

        result = db.fetch_value("SELECT id FROM users WHERE id = %s", (999,))

        assert result is None

    def test_table_exists_true(self):
        """Test table_exists returns True when table exists."""
        db = self._get_db()
        self.mock_cursor.fetchone.return_value = (True,)

        result = db.table_exists("users")

        assert result is True

    def test_table_exists_false(self):
        """Test table_exists returns False when table doesn't exist."""
        db = self._get_db()
        self.mock_cursor.fetchone.return_value = (False,)

        result = db.table_exists("nonexistent")

        assert result is False

    def test_health_check_success(self):
        """Test health_check returns True on success."""
        db = self._get_db()
        self.mock_cursor.fetchone.return_value = (1,)

        result = db.health_check()

        assert result is True

    def test_health_check_failure(self):
        """Test health_check returns False on failure."""
        db = self._get_db()
        self.mock_cursor.fetchone.side_effect = Exception("connection lost")

        result = db.health_check()

        assert result is False
