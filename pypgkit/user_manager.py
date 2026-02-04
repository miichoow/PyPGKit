"""Database user management for pypgkit."""

from __future__ import annotations

import getpass
import logging
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql

from .config import DatabaseConfig
from .exceptions import UserManagementError

if TYPE_CHECKING:
    from .database import Database

logger = logging.getLogger("pypgkit.user_manager")


class UserManager:
    """Manages PostgreSQL database users and roles.

    Provides methods for creating users, managing privileges,
    and checking user existence.
    """

    def __init__(self, database: Database) -> None:
        """Initialize the user manager.

        Args:
            database: Database instance to use
        """
        self._db = database

    def user_exists(self, username: str) -> bool:
        """Check if a database user exists.

        Args:
            username: Username to check

        Returns:
            True if user exists
        """
        query = "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)"
        result = self._db.fetch_value(query, (username,))
        return bool(result)

    def create_user(
        self,
        username: str,
        password: str,
        *,
        login: bool = True,
        superuser: bool = False,
        createdb: bool = False,
        createrole: bool = False,
        inherit: bool = True,
        connection_limit: int = -1,
    ) -> None:
        """Create a new database user.

        Args:
            username: Username for the new user
            password: Password for the new user
            login: Allow login (default: True)
            superuser: Grant superuser privileges (default: False)
            createdb: Allow creating databases (default: False)
            createrole: Allow creating roles (default: False)
            inherit: Inherit privileges from roles (default: True)
            connection_limit: Max connections (-1 for unlimited)

        Raises:
            UserManagementError: If user creation fails
        """
        if self.user_exists(username):
            raise UserManagementError(f"User '{username}' already exists")

        try:
            options = []
            options.append("LOGIN" if login else "NOLOGIN")
            options.append("SUPERUSER" if superuser else "NOSUPERUSER")
            options.append("CREATEDB" if createdb else "NOCREATEDB")
            options.append("CREATEROLE" if createrole else "NOCREATEROLE")
            options.append("INHERIT" if inherit else "NOINHERIT")

            if connection_limit >= 0:
                options.append(f"CONNECTION LIMIT {connection_limit}")

            # Use proper SQL escaping for identifiers and literals
            query = sql.SQL("CREATE ROLE {} WITH {} PASSWORD {}").format(
                sql.Identifier(username),
                sql.SQL(" ".join(options)),
                sql.Literal(password),
            )

            self._db.execute(query)
            logger.info(f"User '{username}' created successfully")

        except Exception as e:
            raise UserManagementError(f"Failed to create user '{username}': {e}") from e

    def drop_user(self, username: str, if_exists: bool = True) -> None:
        """Drop a database user.

        Args:
            username: Username to drop
            if_exists: If True, don't error if user doesn't exist

        Raises:
            UserManagementError: If user deletion fails
        """
        try:
            if if_exists and not self.user_exists(username):
                logger.info(f"User '{username}' does not exist, nothing to drop")
                return

            query = sql.SQL("DROP ROLE {}").format(sql.Identifier(username))
            self._db.execute(query)
            logger.info(f"User '{username}' dropped")

        except Exception as e:
            raise UserManagementError(f"Failed to drop user '{username}': {e}") from e

    def grant_privileges(
        self,
        username: str,
        _database: str,
        privileges: list[str],
        schema: str = "public",
    ) -> None:
        """Grant privileges to a user.

        Args:
            username: Username to grant privileges to
            database: Database name
            privileges: List of privileges (SELECT, INSERT, UPDATE, DELETE, etc.)
            schema: Schema name (default: public)

        Raises:
            UserManagementError: If granting privileges fails
        """
        try:
            # Grant schema usage
            schema_query = sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema),
                sql.Identifier(username),
            )
            self._db.execute(schema_query)

            # Grant table privileges
            privs = ", ".join(privileges)
            table_query = sql.SQL("GRANT {} ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.SQL(privs),
                sql.Identifier(schema),
                sql.Identifier(username),
            )
            self._db.execute(table_query)

            logger.info(f"Granted {privs} on {schema}.* to '{username}'")

        except Exception as e:
            raise UserManagementError(
                f"Failed to grant privileges to '{username}': {e}"
            ) from e

    def revoke_privileges(
        self,
        username: str,
        _database: str,
        privileges: list[str],
        schema: str = "public",
    ) -> None:
        """Revoke privileges from a user.

        Args:
            username: Username to revoke privileges from
            database: Database name
            privileges: List of privileges to revoke
            schema: Schema name (default: public)

        Raises:
            UserManagementError: If revoking privileges fails
        """
        try:
            privs = ", ".join(privileges)
            query = sql.SQL("REVOKE {} ON ALL TABLES IN SCHEMA {} FROM {}").format(
                sql.SQL(privs),
                sql.Identifier(schema),
                sql.Identifier(username),
            )
            self._db.execute(query)
            logger.info(f"Revoked {privs} on {schema}.* from '{username}'")

        except Exception as e:
            raise UserManagementError(
                f"Failed to revoke privileges from '{username}': {e}"
            ) from e

    def change_password(self, username: str, new_password: str) -> None:
        """Change a user's password.

        Args:
            username: Username
            new_password: New password

        Raises:
            UserManagementError: If password change fails
        """
        try:
            query = sql.SQL("ALTER ROLE {} WITH PASSWORD {}").format(
                sql.Identifier(username),
                sql.Literal(new_password),
            )
            self._db.execute(query)
            logger.info(f"Password changed for user '{username}'")

        except Exception as e:
            raise UserManagementError(
                f"Failed to change password for '{username}': {e}"
            ) from e

    def get_user_privileges(self, username: str) -> list[dict]:
        """Get privileges for a user.

        Args:
            username: Username to check

        Returns:
            List of privilege dictionaries
        """
        query = """
            SELECT
                table_schema,
                table_name,
                privilege_type
            FROM information_schema.role_table_grants
            WHERE grantee = %s
            ORDER BY table_schema, table_name, privilege_type
        """
        return list(self._db.fetch_all(query, (username,), as_dict=True))


class AdminUserManager:
    """Manages administrative operations requiring elevated privileges.

    Uses interactive prompts for secure credential input.
    """

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        """Initialize the admin user manager.

        Args:
            config: Base configuration (admin credentials will be prompted)
        """
        self._base_config = config or DatabaseConfig.from_env()

    def _get_admin_credentials(self) -> tuple[str, str]:
        """Prompt for admin credentials securely.

        Returns:
            Tuple of (username, password)
        """
        print("Admin credentials required for this operation.")
        username = input("Admin username: ")
        password = getpass.getpass("Admin password: ")
        return username, password

    def _get_admin_connection(self) -> psycopg.Connection:
        """Create a connection with admin credentials.

        Returns:
            Database connection with admin privileges
        """
        username, password = self._get_admin_credentials()

        conn_kwargs = self._base_config.get_connection_kwargs()
        conn_kwargs["user"] = username
        conn_kwargs["password"] = password

        # Remove conninfo if present since we're using individual params
        conn_kwargs.pop("conninfo", None)

        try:
            return psycopg.connect(**conn_kwargs)
        except Exception as e:
            raise UserManagementError(f"Failed to connect as admin: {e}") from e

    def create_application_user(
        self,
        username: str,
        password: str,
        database: str,
        privileges: list[str] | None = None,
    ) -> None:
        """Create an application user with standard privileges.

        Prompts for admin credentials to perform the operation.

        Args:
            username: Username for the application user
            password: Password for the application user
            database: Database to grant access to
            privileges: List of privileges (default: SELECT, INSERT, UPDATE, DELETE)
        """
        if privileges is None:
            privileges = ["SELECT", "INSERT", "UPDATE", "DELETE"]

        with self._get_admin_connection() as conn, conn.cursor() as cur:
            # Check if user exists
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
                (username,),
            )
            if cur.fetchone()[0]:
                raise UserManagementError(f"User '{username}' already exists")

            # Create user
            cur.execute(
                sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD {}").format(
                    sql.Identifier(username),
                    sql.Literal(password),
                )
            )

            # Grant connect
            cur.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database),
                    sql.Identifier(username),
                )
            )

            # Grant schema usage
            cur.execute(
                sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                    sql.Identifier(username),
                )
            )

            # Grant table privileges
            privs = ", ".join(privileges)
            cur.execute(
                sql.SQL("GRANT {} ON ALL TABLES IN SCHEMA public TO {}").format(
                    sql.SQL(privs),
                    sql.Identifier(username),
                )
            )

            # Grant default privileges for future tables
            cur.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT {} ON TABLES TO {}"
                ).format(
                    sql.SQL(privs),
                    sql.Identifier(username),
                )
            )

            conn.commit()
            logger.info(f"Application user '{username}' created with {privileges}")

    def setup_database(self, database_name: str) -> None:
        """Create a new database.

        Prompts for admin credentials to perform the operation.

        Args:
            database_name: Name of the database to create
        """
        username, password = self._get_admin_credentials()

        # Connect to postgres database for database creation
        conn_kwargs = self._base_config.get_connection_kwargs()
        conn_kwargs["user"] = username
        conn_kwargs["password"] = password
        conn_kwargs["dbname"] = "postgres"
        conn_kwargs.pop("conninfo", None)

        try:
            # Need autocommit for CREATE DATABASE
            with (
                psycopg.connect(**conn_kwargs, autocommit=True) as conn,
                conn.cursor() as cur,
            ):
                # Check if database exists
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                    (database_name,),
                )
                if cur.fetchone()[0]:
                    logger.info(f"Database '{database_name}' already exists")
                    return

                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
                )
                logger.info(f"Database '{database_name}' created")

        except Exception as e:
            raise UserManagementError(f"Failed to create database: {e}") from e
