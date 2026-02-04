"""Tests for pypgkit.setup module."""

import tempfile
from pathlib import Path
from unittest import mock

import pytest

from pypgkit.config import DatabaseConfig
from pypgkit.exceptions import DatabaseConnectionError, SchemaError
from pypgkit.setup import check_connection, ensure_database, init_schema, setup_database


class TestCheckConnection:
    """Tests for check_connection function."""

    def test_check_connection_success(self):
        """Test check_connection returns True on success."""
        config = DatabaseConfig()

        with mock.patch("pypgkit.setup.psycopg.connect") as mock_connect:
            mock_conn = mock.MagicMock()
            mock_cursor = mock.MagicMock()
            mock_connect.return_value.__enter__ = mock.MagicMock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = mock.MagicMock(return_value=False)
            mock_conn.cursor.return_value.__enter__ = mock.MagicMock(
                return_value=mock_cursor
            )
            mock_conn.cursor.return_value.__exit__ = mock.MagicMock(return_value=False)

            result = check_connection(config)

            assert result is True

    def test_check_connection_failure(self):
        """Test check_connection returns False on failure."""
        config = DatabaseConfig()

        with mock.patch("pypgkit.setup.psycopg.connect") as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")

            result = check_connection(config)

            assert result is False


class TestSetupDatabase:
    """Tests for setup_database function."""

    def test_setup_database_non_interactive_requires_credentials(self):
        """Test non-interactive mode requires credentials."""
        config = DatabaseConfig()

        with pytest.raises(DatabaseConnectionError, match="Admin credentials"):
            setup_database(config, interactive=False)

    def test_setup_database_with_credentials(self):
        """Test setup_database with provided credentials."""
        config = DatabaseConfig(database="testdb", user="testuser", password="testpass")

        with mock.patch("pypgkit.setup.psycopg.connect") as mock_connect:
            mock_conn = mock.MagicMock()
            mock_cursor = mock.MagicMock()
            mock_connect.return_value.__enter__ = mock.MagicMock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = mock.MagicMock(return_value=False)
            mock_conn.cursor.return_value.__enter__ = mock.MagicMock(
                return_value=mock_cursor
            )
            mock_conn.cursor.return_value.__exit__ = mock.MagicMock(return_value=False)

            # Database doesn't exist
            mock_cursor.fetchone.return_value = None

            result = setup_database(
                config,
                admin_user="admin",
                admin_password="adminpass",
                interactive=False,
            )

            assert result is True

    def test_setup_database_connection_failure(self):
        """Test setup_database handles connection failure."""
        config = DatabaseConfig()

        with mock.patch("pypgkit.setup.psycopg.connect") as mock_connect:
            import psycopg

            mock_connect.side_effect = psycopg.OperationalError("Connection failed")

            result = setup_database(
                config,
                admin_user="admin",
                admin_password="adminpass",
                interactive=False,
            )

            assert result is False


class TestInitSchema:
    """Tests for init_schema function."""

    def test_init_schema_requires_path_or_sql(self):
        """Test init_schema raises without path or SQL."""
        config = DatabaseConfig()

        with pytest.raises(SchemaError, match="Either schema_path or schema_sql"):
            init_schema(config)

    def test_init_schema_file_not_found(self):
        """Test init_schema raises when file not found."""
        config = DatabaseConfig()

        with pytest.raises(SchemaError, match="not found"):
            init_schema(config, schema_path="/nonexistent/path.sql")

    def test_init_schema_with_sql(self):
        """Test init_schema with SQL string."""
        config = DatabaseConfig()
        sql = "CREATE TABLE test (id INT);"

        with mock.patch("pypgkit.setup.psycopg.connect") as mock_connect:
            mock_conn = mock.MagicMock()
            mock_connect.return_value.__enter__ = mock.MagicMock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = mock.MagicMock(return_value=False)

            init_schema(config, schema_sql=sql)

            mock_conn.execute.assert_called_once_with(sql)
            mock_conn.commit.assert_called_once()

    def test_init_schema_with_file(self):
        """Test init_schema with file path."""
        config = DatabaseConfig()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("CREATE TABLE test (id INT);")
            f.flush()
            schema_path = f.name

        try:
            with mock.patch("pypgkit.setup.psycopg.connect") as mock_connect:
                mock_conn = mock.MagicMock()
                mock_connect.return_value.__enter__ = mock.MagicMock(
                    return_value=mock_conn
                )
                mock_connect.return_value.__exit__ = mock.MagicMock(return_value=False)

                init_schema(config, schema_path=schema_path)

                mock_conn.execute.assert_called_once()
                mock_conn.commit.assert_called_once()
        finally:
            Path(schema_path).unlink()

    def test_init_schema_execution_failure(self):
        """Test init_schema handles execution failure."""
        config = DatabaseConfig()
        sql = "INVALID SQL"

        with mock.patch("pypgkit.setup.psycopg.connect") as mock_connect:
            mock_conn = mock.MagicMock()
            mock_connect.return_value.__enter__ = mock.MagicMock(return_value=mock_conn)
            mock_connect.return_value.__exit__ = mock.MagicMock(return_value=False)
            mock_conn.execute.side_effect = Exception("SQL error")

            with pytest.raises(SchemaError, match="Failed to initialize"):
                init_schema(config, schema_sql=sql)


class TestEnsureDatabase:
    """Tests for ensure_database function."""

    def test_ensure_database_already_connected(self):
        """Test ensure_database when already connected."""
        config = DatabaseConfig()

        with mock.patch("pypgkit.setup.check_connection", return_value=True):
            result = ensure_database(config)

            assert result is True

    def test_ensure_database_setup_and_connect(self):
        """Test ensure_database runs setup when not connected."""
        config = DatabaseConfig()

        with (
            mock.patch("pypgkit.setup.check_connection") as mock_check,
            mock.patch("pypgkit.setup.setup_database", return_value=True),
        ):
            # First call fails, second succeeds (after setup)
            mock_check.side_effect = [False, True]

            ensure_database(config, interactive=False)

            # setup_database would be called but we're mocking it
            assert mock_check.call_count == 2

    def test_ensure_database_setup_fails(self):
        """Test ensure_database raises when setup fails."""
        config = DatabaseConfig()

        with (
            mock.patch("pypgkit.setup.check_connection", return_value=False),
            mock.patch("pypgkit.setup.setup_database", return_value=False),
            pytest.raises(DatabaseConnectionError, match="setup failed"),
        ):
            ensure_database(config, interactive=False)

    def test_ensure_database_with_schema(self):
        """Test ensure_database initializes schema."""
        config = DatabaseConfig()
        sql = "CREATE TABLE test (id INT);"

        with (
            mock.patch("pypgkit.setup.check_connection", return_value=True),
            mock.patch("pypgkit.setup.init_schema") as mock_init,
        ):
            ensure_database(config, schema_sql=sql)

            mock_init.assert_called_once_with(config, schema_path=None, schema_sql=sql)
