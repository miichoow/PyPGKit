"""Main database facade for pypgkit."""

from __future__ import annotations

import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

from psycopg import sql
from psycopg.rows import dict_row

from .config import DatabaseConfig
from .connection import ConnectionPoolSingleton, get_pool
from .exceptions import DatabaseConnectionError

if TYPE_CHECKING:
    from psycopg import Connection, Cursor

logger = logging.getLogger("pypgkit.database")


class Database:
    """High-level database facade.

    Provides a clean API for database operations with automatic
    connection management, transactions, and reconnection handling.

    Example usage:
        # Initialize once (typically at app startup)
        Database.init(schema_path="schemas/init.sql")

        # Then use get_instance() anywhere in your code
        db = Database.get_instance()
        users = db.fetch_all("SELECT * FROM users")

        # Or in one line for repositories
        repo = UserRepository(Database.get_instance())
    """

    # Singleton instance
    _instance: Database | None = None

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        """Initialize the database facade.

        Args:
            config: Database configuration. If None, loads from environment.
        """
        if config is None:
            config = DatabaseConfig.from_env()

        self._config = config
        self._pool: ConnectionPoolSingleton | None = None

    @classmethod
    def get_instance(cls) -> Database:
        """Get the singleton Database instance.

        Returns the instance created by init(). Call init() first
        to configure and initialize the database.

        Returns:
            The singleton Database instance

        Raises:
            DatabaseConnectionError: If init() was not called first

        Example:
            # At app startup
            Database.init(schema_path="schemas/init.sql")

            # Anywhere else in your code
            db = Database.get_instance()
            users = db.fetch_all("SELECT * FROM users")
        """
        if cls._instance is None:
            raise DatabaseConnectionError(
                "Database not initialized. Call Database.init() first."
            )
        return cls._instance

    @classmethod
    def is_initialized(cls) -> bool:
        """Check if the database singleton has been initialized.

        Returns:
            True if init() has been called
        """
        return cls._instance is not None

    @classmethod
    def init(
        cls,
        config: DatabaseConfig | None = None,
        schema_path: str | Path | None = None,
        schema_sql: str | None = None,
        auto_setup: bool = True,
        interactive: bool = True,
    ) -> Database:
        """Initialize the Database singleton and return it.

        This is the recommended way to initialize the database. It will:
        1. Check if the database/user exist
        2. Create them if needed (with admin credentials prompt)
        3. Initialize the schema if provided
        4. Store the instance as a singleton for get_instance()

        Call this once at application startup, then use get_instance()
        anywhere else in your code.

        Args:
            config: Database configuration. If None, loads from environment.
            schema_path: Path to SQL file with schema definition
            schema_sql: SQL string with schema (alternative to schema_path)
            auto_setup: If True, automatically create database/user if needed
            interactive: If True, prompt for admin credentials when needed

        Returns:
            Connected Database instance (also stored as singleton)

        Example:
            # At startup
            Database.init(schema_path="schemas/init.sql")

            # Anywhere else
            db = Database.get_instance()
            users = db.fetch_all("SELECT * FROM users")
        """
        # Return existing instance if already initialized
        if cls._instance is not None:
            logger.debug("Database already initialized, returning existing instance")
            return cls._instance

        from .setup import ensure_database

        if config is None:
            logger.debug("Loading configuration from environment")
            config = DatabaseConfig.from_env()

        logger.info(
            f"Initializing database: {config.database}@{config.host}:{config.port}"
        )

        if auto_setup:
            ensure_database(
                config,
                schema_path=schema_path,
                schema_sql=schema_sql,
                interactive=interactive,
            )
        elif schema_path is not None or schema_sql is not None:
            from .setup import init_schema

            init_schema(config, schema_path=schema_path, schema_sql=schema_sql)

        db = cls(config)
        db.connect()

        # Store as singleton
        cls._instance = db

        logger.info("Database initialization complete")
        return db

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance.

        Useful for testing or reinitializing with different config.
        """
        if cls._instance is not None:
            with suppress(Exception):
                cls._instance.disconnect()
            cls._instance = None
            logger.debug("Database singleton reset")

    def connect(self) -> None:
        """Establish connection to the database.

        Creates the connection pool if not already created.
        """
        if self._pool is None:
            self._pool = get_pool(self._config)
            logger.info("Database connected")

    def disconnect(self) -> None:
        """Disconnect from the database.

        Closes the connection pool.
        """
        if self._pool is not None:
            self._pool.close()
            ConnectionPoolSingleton.reset()
            self._pool = None
            logger.info("Database disconnected")

    @property
    def pool(self) -> ConnectionPoolSingleton:
        """Get the connection pool, connecting if necessary."""
        if self._pool is None:
            self.connect()
        return self._pool

    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        """Get a connection from the pool.

        Usage:
            with db.connection() as conn:
                # use connection
        """
        with self.pool.connection() as conn:
            yield conn

    @contextmanager
    def transaction(self) -> Generator[Connection, None, None]:
        """Execute operations within a transaction.

        Automatically commits on success, rolls back on exception.

        Usage:
            with db.transaction() as conn:
                conn.execute("INSERT INTO ...")
                conn.execute("UPDATE ...")
        """
        with self.connection() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @contextmanager
    def cursor(
        self,
        *,
        row_factory: Any = None,
    ) -> Generator[Cursor, None, None]:
        """Get a cursor for executing queries.

        Args:
            row_factory: Optional row factory (e.g., dict_row)

        Usage:
            with db.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM users")
                rows = cur.fetchall()
        """
        with self.connection() as conn, conn.cursor(row_factory=row_factory) as cur:
            yield cur
            conn.commit()

    def execute(
        self,
        query: str | sql.SQL | sql.Composed,
        params: Sequence[Any] | dict[str, Any] | None = None,
    ) -> int:
        """Execute a query and return the number of affected rows.

        Args:
            query: SQL query (parameterized)
            params: Query parameters

        Returns:
            Number of affected rows
        """
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
            return cur.rowcount

    def execute_many(
        self,
        query: str | sql.SQL | sql.Composed,
        params_seq: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> int:
        """Execute a query with multiple parameter sets.

        Args:
            query: SQL query (parameterized)
            params_seq: Sequence of parameter sets

        Returns:
            Total number of affected rows
        """
        with self.connection() as conn, conn.cursor() as cur:
            cur.executemany(query, params_seq)
            conn.commit()
            return cur.rowcount

    def fetch_one(
        self,
        query: str | sql.SQL | sql.Composed,
        params: Sequence[Any] | dict[str, Any] | None = None,
        *,
        as_dict: bool = False,
    ) -> tuple | dict | None:
        """Fetch a single row.

        Args:
            query: SQL query (parameterized)
            params: Query parameters
            as_dict: If True, return row as dictionary

        Returns:
            Single row or None if no results
        """
        row_factory = dict_row if as_dict else None
        with self.connection() as conn, conn.cursor(row_factory=row_factory) as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def fetch_all(
        self,
        query: str | sql.SQL | sql.Composed,
        params: Sequence[Any] | dict[str, Any] | None = None,
        *,
        as_dict: bool = False,
    ) -> list[tuple] | list[dict]:
        """Fetch all rows.

        Args:
            query: SQL query (parameterized)
            params: Query parameters
            as_dict: If True, return rows as dictionaries

        Returns:
            List of rows
        """
        row_factory = dict_row if as_dict else None
        with self.connection() as conn, conn.cursor(row_factory=row_factory) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def fetch_value(
        self,
        query: str | sql.SQL | sql.Composed,
        params: Sequence[Any] | dict[str, Any] | None = None,
    ) -> Any:
        """Fetch a single value from the first column of the first row.

        Args:
            query: SQL query (parameterized)
            params: Query parameters

        Returns:
            Single value or None
        """
        row = self.fetch_one(query, params)
        return row[0] if row else None

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table
            schema: Schema name (default: public)

        Returns:
            True if table exists
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """
        result = self.fetch_value(query, (schema, table_name))
        return bool(result)

    def get_stats(self) -> dict:
        """Get connection pool statistics.

        Returns:
            Dictionary with pool statistics
        """
        return self.pool.get_stats()

    def health_check(self) -> bool:
        """Perform a health check on the database connection.

        Returns:
            True if database is reachable
        """
        try:
            result = self.fetch_value("SELECT 1")
            return result == 1
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    def __enter__(self) -> Database:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()
