"""Schema and table management for pypgkit."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from .exceptions import SchemaError

if TYPE_CHECKING:
    from .database import Database

logger = logging.getLogger("pypgkit.schema")


class SchemaManager:
    """Manages database schema operations.

    Provides methods for checking table existence, executing SQL scripts,
    and managing schema versions.
    """

    def __init__(self, database: Database) -> None:
        """Initialize the schema manager.

        Args:
            database: Database instance to use
        """
        self._db = database

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table
            schema: Schema name (default: public)

        Returns:
            True if the table exists
        """
        return self._db.table_exists(table_name, schema)

    def schema_exists(self, schema_name: str) -> bool:
        """Check if a schema exists.

        Args:
            schema_name: Name of the schema

        Returns:
            True if the schema exists
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.schemata
                WHERE schema_name = %s
            )
        """
        result = self._db.fetch_value(query, (schema_name,))
        return bool(result)

    def create_schema(self, schema_name: str, if_not_exists: bool = True) -> None:
        """Create a database schema.

        Args:
            schema_name: Name of the schema to create
            if_not_exists: If True, don't error if schema exists
        """
        try:
            if if_not_exists:
                query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
            else:
                query = f"CREATE SCHEMA {schema_name}"
            self._db.execute(query)
            logger.info(f"Schema '{schema_name}' created")
        except Exception as e:
            raise SchemaError(f"Failed to create schema '{schema_name}': {e}") from e

    def execute_sql_file(self, file_path: str | Path) -> None:
        """Execute an SQL file.

        Args:
            file_path: Path to the SQL file

        Raises:
            SchemaError: If file doesn't exist or execution fails
        """
        path = Path(file_path)
        if not path.exists():
            raise SchemaError(f"SQL file not found: {path}")

        try:
            sql_content = path.read_text(encoding="utf-8")
            self.execute_sql(sql_content)
            logger.info(f"Executed SQL file: {path}")
        except SchemaError:
            raise
        except Exception as e:
            raise SchemaError(f"Failed to execute SQL file '{path}': {e}") from e

    def execute_sql(self, sql: str) -> None:
        """Execute raw SQL.

        Args:
            sql: SQL string to execute

        Raises:
            SchemaError: If execution fails
        """
        try:
            with self._db.transaction() as conn:
                conn.execute(sql)
            logger.debug("SQL executed successfully")
        except Exception as e:
            raise SchemaError(f"Failed to execute SQL: {e}") from e

    def get_tables(self, schema: str = "public") -> list[str]:
        """Get list of tables in a schema.

        Args:
            schema: Schema name (default: public)

        Returns:
            List of table names
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        rows = self._db.fetch_all(query, (schema,))
        return [row[0] for row in rows]

    def get_columns(
        self,
        table_name: str,
        schema: str = "public",
    ) -> list[dict]:
        """Get column information for a table.

        Args:
            table_name: Name of the table
            schema: Schema name (default: public)

        Returns:
            List of column info dictionaries
        """
        query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        rows = self._db.fetch_all(query, (schema, table_name), as_dict=True)
        return list(rows)

    def drop_table(
        self,
        table_name: str,
        schema: str = "public",
        cascade: bool = False,
        if_exists: bool = True,
    ) -> None:
        """Drop a table.

        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            cascade: If True, drop dependent objects
            if_exists: If True, don't error if table doesn't exist
        """
        try:
            parts = ["DROP TABLE"]
            if if_exists:
                parts.append("IF EXISTS")
            parts.append(f"{schema}.{table_name}")
            if cascade:
                parts.append("CASCADE")

            self._db.execute(" ".join(parts))
            logger.info(f"Table '{schema}.{table_name}' dropped")
        except Exception as e:
            raise SchemaError(
                f"Failed to drop table '{schema}.{table_name}': {e}"
            ) from e

    def init_schema(
        self,
        sql_file: str | Path | None = None,
        sql_content: str | None = None,
    ) -> None:
        """Initialize the database schema.

        Args:
            sql_file: Path to initialization SQL file
            sql_content: SQL content to execute

        Raises:
            SchemaError: If neither sql_file nor sql_content provided
        """
        if sql_file:
            self.execute_sql_file(sql_file)
        elif sql_content:
            self.execute_sql(sql_content)
        else:
            raise SchemaError("Either sql_file or sql_content must be provided")


class MigrationManager:
    """Simple migration tracking (optional feature).

    Tracks applied migrations in a migrations table.
    """

    MIGRATIONS_TABLE = "_pypgkit_migrations"

    def __init__(self, schema_manager: SchemaManager, database: Database) -> None:
        """Initialize the migration manager.

        Args:
            schema_manager: Schema manager instance
            database: Database instance
        """
        self._schema = schema_manager
        self._db = database

    def init_migrations_table(self) -> None:
        """Create the migrations tracking table if it doesn't exist."""
        sql = f"""
            CREATE TABLE IF NOT EXISTS {self.MIGRATIONS_TABLE} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """
        self._schema.execute_sql(sql)
        logger.info("Migrations table initialized")

    def is_applied(self, migration_name: str) -> bool:
        """Check if a migration has been applied.

        Args:
            migration_name: Name of the migration

        Returns:
            True if migration has been applied
        """
        query = f"SELECT EXISTS (SELECT 1 FROM {self.MIGRATIONS_TABLE} WHERE name = %s)"
        result = self._db.fetch_value(query, (migration_name,))
        return bool(result)

    def mark_applied(self, migration_name: str) -> None:
        """Mark a migration as applied.

        Args:
            migration_name: Name of the migration
        """
        query = f"INSERT INTO {self.MIGRATIONS_TABLE} (name) VALUES (%s)"
        self._db.execute(query, (migration_name,))
        logger.info(f"Migration '{migration_name}' marked as applied")

    def get_applied_migrations(self) -> list[str]:
        """Get list of applied migrations.

        Returns:
            List of migration names in order applied
        """
        query = f"SELECT name FROM {self.MIGRATIONS_TABLE} ORDER BY applied_at"
        rows = self._db.fetch_all(query)
        return [row[0] for row in rows]

    def run_migration(
        self,
        migration_name: str,
        sql_content: str,
        skip_if_applied: bool = True,
    ) -> bool:
        """Run a migration.

        Args:
            migration_name: Unique name for the migration
            sql_content: SQL to execute
            skip_if_applied: If True, skip already applied migrations

        Returns:
            True if migration was applied, False if skipped
        """
        if skip_if_applied and self.is_applied(migration_name):
            logger.info(f"Migration '{migration_name}' already applied, skipping")
            return False

        with self._db.transaction() as conn:
            conn.execute(sql_content)
            conn.execute(
                f"INSERT INTO {self.MIGRATIONS_TABLE} (name) VALUES (%s)",
                (migration_name,),
            )

        logger.info(f"Migration '{migration_name}' applied successfully")
        return True
