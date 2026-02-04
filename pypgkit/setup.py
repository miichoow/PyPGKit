"""Database setup and initialization utilities for pypgkit."""

from __future__ import annotations

import getpass
import logging
from pathlib import Path

import psycopg
from psycopg import sql

from .config import DatabaseConfig
from .exceptions import DatabaseConnectionError, SchemaError

logger = logging.getLogger("pypgkit.setup")


def check_connection(config: DatabaseConfig) -> bool:
    """Check if we can connect to the database with given configuration.

    Uses a direct connection (no pool) for a quick test.

    Args:
        config: Database configuration to test

    Returns:
        True if connection succeeds, False otherwise
    """
    logger.debug(
        f"Checking connection to {config.database}@{config.host}:{config.port}"
    )
    try:
        kwargs = config.get_connection_kwargs()
        # Remove pool-specific settings
        kwargs.pop("conninfo", None)
        kwargs["connect_timeout"] = 5

        with psycopg.connect(**kwargs) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            logger.info(f"Connection to {config.database} successful")
            return True
    except Exception as e:
        logger.debug(f"Connection check failed: {e}")
        return False


def setup_database(
    config: DatabaseConfig,
    admin_user: str | None = None,
    admin_password: str | None = None,
    interactive: bool = True,
) -> bool:
    """Set up the database and application user if they don't exist.

    Args:
        config: Application database configuration (target database/user to create)
        admin_user: Admin username (if None and interactive, will prompt)
        admin_password: Admin password (if None and interactive, will prompt)
        interactive: If True, prompt for admin credentials when needed

    Returns:
        True if setup was successful

    Raises:
        DatabaseConnectionError: If setup fails and not interactive
    """
    database_name = config.database
    app_user = config.user
    app_password = config.password
    host = config.host
    port = config.port

    # Get admin credentials
    if admin_user is None or admin_password is None:
        if not interactive:
            raise DatabaseConnectionError(
                "Admin credentials required for database setup"
            )

        print("\n" + "=" * 50)
        print("DATABASE SETUP")
        print("=" * 50)
        print("Admin credentials required to create database/user.")
        print("(Usually 'postgres' with your PostgreSQL admin password)")
        print()

        if admin_user is None:
            admin_user = input("Admin username [postgres]: ").strip() or "postgres"
        if admin_password is None:
            admin_password = getpass.getpass("Admin password: ")

    try:
        logger.info(f"Connecting to PostgreSQL as '{admin_user}'...")
        if interactive:
            print(f"\nConnecting to PostgreSQL as '{admin_user}'...")

        with (
            psycopg.connect(
                host=host,
                port=port,
                dbname="postgres",
                user=admin_user,
                password=admin_password,
                autocommit=True,
            ) as conn,
            conn.cursor() as cur,
        ):
            # Check/create database
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (database_name,),
            )
            if not cur.fetchone():
                if interactive:
                    print(f"Creating database '{database_name}'...")
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
                )
                logger.info(f"Database '{database_name}' created")
            else:
                logger.info(f"Database '{database_name}' already exists")

            # Check/create user
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s",
                (app_user,),
            )
            if not cur.fetchone():
                if interactive:
                    print(f"Creating user '{app_user}'...")
                cur.execute(
                    sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD {}").format(
                        sql.Identifier(app_user),
                        sql.Literal(app_password),
                    )
                )
                logger.info(f"User '{app_user}' created")
            else:
                # Update password to ensure it matches
                cur.execute(
                    sql.SQL("ALTER ROLE {} WITH PASSWORD {}").format(
                        sql.Identifier(app_user),
                        sql.Literal(app_password),
                    )
                )
                logger.info(f"User '{app_user}' password updated")

            # Grant database privileges
            cur.execute(
                sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {}").format(
                    sql.Identifier(database_name),
                    sql.Identifier(app_user),
                )
            )

        # Grant schema privileges (requires connecting to the target database)
        with (
            psycopg.connect(
                host=host,
                port=port,
                dbname=database_name,
                user=admin_user,
                password=admin_password,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                sql.SQL("GRANT ALL ON SCHEMA public TO {}").format(
                    sql.Identifier(app_user),
                )
            )
            cur.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                    "GRANT ALL ON TABLES TO {}"
                ).format(sql.Identifier(app_user))
            )
            cur.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                    "GRANT ALL ON SEQUENCES TO {}"
                ).format(sql.Identifier(app_user))
            )
            conn.commit()

        if interactive:
            print("\n" + "=" * 50)
            print("DATABASE SETUP COMPLETE!")
            print("=" * 50 + "\n")

        logger.info("Database setup complete")
        return True

    except psycopg.OperationalError as e:
        logger.error(f"Failed to connect with admin credentials: {e}")
        if interactive:
            print(f"\nERROR: Failed to connect with admin credentials: {e}")
        return False
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        if interactive:
            print(f"\nERROR: Setup failed: {e}")
        return False


def init_schema(
    config: DatabaseConfig,
    schema_path: str | Path | None = None,
    schema_sql: str | None = None,
) -> None:
    """Initialize database schema.

    Args:
        config: Database configuration
        schema_path: Path to SQL file with schema
        schema_sql: SQL string with schema (alternative to schema_path)

    Raises:
        SchemaError: If schema initialization fails
    """
    if schema_path is None and schema_sql is None:
        raise SchemaError("Either schema_path or schema_sql must be provided")

    if schema_path is not None:
        path = Path(schema_path)
        if not path.exists():
            raise SchemaError(f"Schema file not found: {path}")
        schema_sql = path.read_text(encoding="utf-8")

    try:
        kwargs = config.get_connection_kwargs()
        kwargs.pop("conninfo", None)

        with psycopg.connect(**kwargs) as conn:
            conn.execute(schema_sql)
            conn.commit()

        logger.info("Schema initialized successfully")

    except Exception as e:
        raise SchemaError(f"Failed to initialize schema: {e}") from e


def ensure_database(
    config: DatabaseConfig,
    schema_path: str | Path | None = None,
    schema_sql: str | None = None,
    interactive: bool = True,
) -> bool:
    """Ensure database exists and is properly configured.

    This is the main entry point for database initialization. It will:
    1. Check if connection works
    2. If not, run setup (create database/user)
    3. Initialize schema if provided

    Args:
        config: Database configuration
        schema_path: Optional path to schema SQL file
        schema_sql: Optional schema SQL string
        interactive: If True, prompt for admin credentials when needed

    Returns:
        True if database is ready

    Raises:
        DatabaseConnectionError: If database cannot be set up
        SchemaError: If schema initialization fails
    """
    # Check if we can connect
    if not check_connection(config):
        logger.info("Connection failed, attempting setup...")

        if not setup_database(config, interactive=interactive):
            raise DatabaseConnectionError("Database setup failed")

        # Verify connection after setup
        if not check_connection(config):
            raise DatabaseConnectionError("Connection still fails after setup")

    # Initialize schema if provided
    if schema_path is not None or schema_sql is not None:
        init_schema(config, schema_path=schema_path, schema_sql=schema_sql)

    return True
