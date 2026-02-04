"""pypgkit - A production-grade PostgreSQL database framework.

This package provides a clean, thread-safe interface to PostgreSQL with:
- Automatic database/user creation with admin prompts
- Connection pooling via psycopg3
- Repository pattern for data access
- Schema initialization
- Thread safety and gunicorn compatibility

Quick Start:
    from pypgkit import Database, DatabaseConfig, BaseRepository
    from dataclasses import dataclass

    # 1. Define your entity
    @dataclass
    class User:
        id: int = None
        email: str = ""
        name: str = ""

    # 2. Define your repository
    class UserRepository(BaseRepository[User]):
        table_name = "users"
        primary_key = "id"

        def _row_to_entity(self, row: dict) -> User:
            return User(**row)

        def _entity_to_row(self, entity: User) -> dict:
            return {"email": entity.email, "name": entity.name}

    # 3. Initialize database (handles everything automatically)
    db = Database.init(schema_path="schemas/init.sql")

    # 4. Use your repository
    repo = UserRepository(db)
    user = repo.create(User(email="john@example.com", name="John"))
    users = repo.find_all()

Environment Variables:
    PYPGKIT_HOST, PYPGKIT_PORT, PYPGKIT_DATABASE, PYPGKIT_USER, PYPGKIT_PASSWORD
    Or use PYPGKIT_CONNECTION_STRING for a full connection string.
"""

from .config import DatabaseConfig
from .connection import ConnectionPoolSingleton, get_pool
from .database import Database
from .exceptions import (
    ConfigurationError,
    DatabaseConnectionError,
    PyPgKitError,
    RepositoryError,
    SchemaError,
    UserManagementError,
)
from .logging import (
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
from .repositories.base import BaseRepository
from .schema import MigrationManager, SchemaManager
from .setup import check_connection, ensure_database, init_schema, setup_database
from .user_manager import AdminUserManager, UserManager

__version__ = "1.0.0"

__all__ = [
    # Main classes
    "Database",
    "DatabaseConfig",
    # Setup utilities
    "ensure_database",
    "setup_database",
    "check_connection",
    "init_schema",
    # Logging
    "configure_logging",
    "get_logger",
    "set_level",
    "enable_debug",
    "disable_logging",
    "LogLevel",
    "DEFAULT_FORMAT",
    "SIMPLE_FORMAT",
    "DETAILED_FORMAT",
    # Connection pool
    "ConnectionPoolSingleton",
    "get_pool",
    # Schema management
    "SchemaManager",
    "MigrationManager",
    # User management
    "UserManager",
    "AdminUserManager",
    # Repository
    "BaseRepository",
    # Exceptions
    "PyPgKitError",
    "DatabaseConnectionError",
    "ConfigurationError",
    "SchemaError",
    "UserManagementError",
    "RepositoryError",
]
