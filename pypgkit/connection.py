"""Thread-safe connection pool singleton for pypgkit."""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from typing import TYPE_CHECKING

from psycopg_pool import ConnectionPool

from .config import DatabaseConfig
from .exceptions import DatabaseConnectionError

if TYPE_CHECKING:
    from psycopg import Connection

logger = logging.getLogger("pypgkit.connection")

# Track the PID where the pool was created (for fork safety)
_pool_pid: int | None = None


class ConnectionPoolSingleton:
    """Thread-safe singleton connection pool.

    Uses double-checked locking pattern for thread safety.
    The underlying psycopg_pool.ConnectionPool is inherently thread-safe.
    """

    _instance: ConnectionPoolSingleton | None = None
    _lock: threading.RLock = threading.RLock()
    _initialized: bool = False

    def __new__(cls, _config: DatabaseConfig | None = None) -> ConnectionPoolSingleton:
        """Create or return the singleton instance.

        Fork-safe: automatically resets if called from a different process
        than where the pool was created (e.g., after gunicorn fork).

        Args:
            config: Database configuration (required for first initialization)

        Returns:
            The singleton ConnectionPoolSingleton instance
        """
        global _pool_pid

        current_pid = os.getpid()

        # Check if we're in a forked process
        if _pool_pid is not None and _pool_pid != current_pid:
            # We're in a forked child - reset without closing (parent owns connections)
            with cls._lock:
                cls._instance = None
                cls._initialized = False
                _pool_pid = None
                logger.info(
                    f"Pool reset after fork (was pid {_pool_pid}, now {current_pid})"
                )

        if cls._instance is None:
            with cls._lock:
                # Double-checked locking
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    _pool_pid = current_pid
        return cls._instance

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        """Initialize the connection pool.

        Args:
            config: Database configuration (required for first initialization)
        """
        # Prevent re-initialization
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            if config is None:
                raise DatabaseConnectionError(
                    "Configuration required for initial pool creation"
                )

            self._config = config
            self._pool: ConnectionPool | None = None
            self._closed = False
            self._create_pool()
            self._initialized = True
            # Note: No atexit handler - causes issues with gunicorn/multiprocessing
            # The pool uses daemon threads that will be cleaned up on process exit

    def _create_pool(self) -> None:
        """Create the connection pool."""
        pool = None
        try:
            conn_kwargs = self._config.get_connection_kwargs()

            pool = ConnectionPool(
                min_size=self._config.min_connections,
                max_size=self._config.max_connections,
                timeout=self._config.connection_timeout,
                max_idle=self._config.max_idle_time,
                check=ConnectionPool.check_connection
                if self._config.check_connection
                else None,
                kwargs=conn_kwargs,
            )

            # Wait for pool to be ready
            pool.wait()
            self._pool = pool
            logger.info("Connection pool created successfully")

        except Exception as e:
            # Clean up partially created pool
            if pool is not None:
                with contextlib.suppress(Exception):
                    pool.close()
            self._pool = None
            logger.error(f"Failed to create connection pool: {e}")
            raise DatabaseConnectionError(
                f"Failed to create connection pool: {e}"
            ) from e

    @property
    def pool(self) -> ConnectionPool:
        """Get the underlying connection pool.

        Returns:
            The psycopg ConnectionPool instance

        Raises:
            DatabaseConnectionError: If pool is not initialized
        """
        if self._pool is None:
            raise DatabaseConnectionError("Connection pool not initialized")
        return self._pool

    def get_connection(self) -> Connection:
        """Get a connection from the pool.

        This should be used with a context manager to ensure proper release.

        Returns:
            A database connection

        Raises:
            DatabaseConnectionError: If unable to get a connection
        """
        try:
            return self.pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise DatabaseConnectionError(f"Failed to get connection: {e}") from e

    def return_connection(self, conn: Connection) -> None:
        """Return a connection to the pool.

        Args:
            conn: The connection to return
        """
        try:
            self.pool.putconn(conn)
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {e}")

    def connection(self):
        """Context manager for getting a connection.

        Usage:
            with pool.connection() as conn:
                # use connection
        """
        return self.pool.connection()

    def close(self) -> None:
        """Close the connection pool and release all connections."""
        with self._lock:
            if self._pool is not None and not self._closed:
                self._closed = True  # Mark as closed first
                pool = self._pool
                self._pool = None
                try:
                    # Close with 0 timeout - don't wait for connections
                    pool.close(timeout=0)
                    logger.info("Connection pool closed")
                except Exception as e:
                    logger.warning(f"Error closing connection pool: {e}")

    def get_stats(self) -> dict:
        """Get pool statistics.

        Returns:
            Dictionary with pool statistics
        """
        if self._pool is None:
            return {"status": "not_initialized"}

        stats = self._pool.get_stats()
        return {
            "pool_min": stats.get("pool_min", 0),
            "pool_max": stats.get("pool_max", 0),
            "pool_size": stats.get("pool_size", 0),
            "pool_available": stats.get("pool_available", 0),
            "requests_waiting": stats.get("requests_waiting", 0),
        }

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (useful for testing).

        Warning: This will close all connections.
        """
        global _pool_pid
        with cls._lock:
            if cls._instance is not None:
                with contextlib.suppress(Exception):
                    cls._instance.close()
            # Always reset state, even if close failed
            cls._instance = None
            cls._initialized = False
            _pool_pid = None


def get_pool(config: DatabaseConfig | None = None) -> ConnectionPoolSingleton:
    """Get the connection pool singleton.

    Args:
        config: Database configuration (required for first call)

    Returns:
        The ConnectionPoolSingleton instance
    """
    return ConnectionPoolSingleton(config)
