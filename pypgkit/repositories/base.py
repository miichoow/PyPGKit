"""Base repository class with CRUD operations."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from psycopg import sql

from ..exceptions import RepositoryError

if TYPE_CHECKING:
    from ..database import Database

logger = logging.getLogger("pypgkit.repositories")

T = TypeVar("T")


class BaseRepository(ABC, Generic[T]):
    """Abstract base repository with common CRUD operations.

    Provides a consistent interface for data access with automatic
    SQL generation and parameter handling.

    Subclasses must define:
        - table_name: Name of the database table
        - primary_key: Name of the primary key column

    Example:
        class UserRepository(BaseRepository[dict]):
            table_name = "users"
            primary_key = "id"

            def _row_to_entity(self, row: dict) -> dict:
                return row

            def _entity_to_row(self, entity: dict) -> dict:
                return entity
    """

    table_name: str
    primary_key: str = "id"
    schema: str = "public"

    def __init__(self, database: Database) -> None:
        """Initialize the repository.

        Args:
            database: Database instance to use
        """
        self._db = database
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        """Validate repository configuration."""
        if not hasattr(self, "table_name") or not self.table_name:
            raise RepositoryError(f"{self.__class__.__name__} must define 'table_name'")

    @property
    def _qualified_table(self) -> sql.Composed:
        """Get fully qualified table name."""
        return sql.SQL("{}.{}").format(
            sql.Identifier(self.schema),
            sql.Identifier(self.table_name),
        )

    @abstractmethod
    def _row_to_entity(self, row: dict) -> T:
        """Convert a database row to an entity.

        Args:
            row: Dictionary from database

        Returns:
            Entity of type T
        """
        pass

    @abstractmethod
    def _entity_to_row(self, entity: T) -> dict:
        """Convert an entity to a database row.

        Args:
            entity: Entity of type T

        Returns:
            Dictionary for database insertion
        """
        pass

    def find_by_id(self, id_value: Any) -> T | None:
        """Find an entity by its primary key.

        Args:
            id_value: Primary key value

        Returns:
            Entity if found, None otherwise
        """
        try:
            query = sql.SQL("SELECT * FROM {} WHERE {} = %s").format(
                self._qualified_table,
                sql.Identifier(self.primary_key),
            )
            row = self._db.fetch_one(query, (id_value,), as_dict=True)
            return self._row_to_entity(row) if row else None
        except Exception as e:
            raise RepositoryError(f"Failed to find by id {id_value}: {e}") from e

    def find_all(
        self,
        *,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
    ) -> list[T]:
        """Find all entities.

        Args:
            limit: Maximum number of results
            offset: Number of results to skip
            order_by: Column to order by
            order_desc: If True, order descending

        Returns:
            List of entities
        """
        try:
            parts = [sql.SQL("SELECT * FROM {}").format(self._qualified_table)]

            if order_by:
                direction = sql.SQL("DESC") if order_desc else sql.SQL("ASC")
                parts.append(
                    sql.SQL("ORDER BY {} {}").format(
                        sql.Identifier(order_by),
                        direction,
                    )
                )

            if limit is not None:
                parts.append(sql.SQL("LIMIT {}").format(sql.Literal(limit)))

            if offset is not None:
                parts.append(sql.SQL("OFFSET {}").format(sql.Literal(offset)))

            query = sql.SQL(" ").join(parts)
            rows = self._db.fetch_all(query, as_dict=True)
            return [self._row_to_entity(row) for row in rows]

        except Exception as e:
            raise RepositoryError(f"Failed to find all: {e}") from e

    def find_by(
        self,
        conditions: dict[str, Any],
        *,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
        order_desc: bool = False,
    ) -> list[T]:
        """Find entities by conditions.

        Args:
            conditions: Dictionary of column -> value conditions (AND)
            limit: Maximum number of results
            offset: Number of results to skip
            order_by: Column to order by
            order_desc: If True, order descending

        Returns:
            List of matching entities
        """
        if not conditions:
            return self.find_all(
                limit=limit, offset=offset, order_by=order_by, order_desc=order_desc
            )

        try:
            where_parts = []
            values = []

            for column, value in conditions.items():
                if value is None:
                    where_parts.append(
                        sql.SQL("{} IS NULL").format(sql.Identifier(column))
                    )
                else:
                    where_parts.append(
                        sql.SQL("{} = %s").format(sql.Identifier(column))
                    )
                    values.append(value)

            parts = [
                sql.SQL("SELECT * FROM {} WHERE {}").format(
                    self._qualified_table,
                    sql.SQL(" AND ").join(where_parts),
                )
            ]

            if order_by:
                direction = sql.SQL("DESC") if order_desc else sql.SQL("ASC")
                parts.append(
                    sql.SQL("ORDER BY {} {}").format(
                        sql.Identifier(order_by),
                        direction,
                    )
                )

            if limit is not None:
                parts.append(sql.SQL("LIMIT {}").format(sql.Literal(limit)))

            if offset is not None:
                parts.append(sql.SQL("OFFSET {}").format(sql.Literal(offset)))

            query = sql.SQL(" ").join(parts)
            rows = self._db.fetch_all(query, tuple(values), as_dict=True)
            return [self._row_to_entity(row) for row in rows]

        except Exception as e:
            raise RepositoryError(f"Failed to find by conditions: {e}") from e

    def find_one_by(self, conditions: dict[str, Any]) -> T | None:
        """Find a single entity by conditions.

        Args:
            conditions: Dictionary of column -> value conditions

        Returns:
            Entity if found, None otherwise
        """
        results = self.find_by(conditions, limit=1)
        return results[0] if results else None

    def create(self, entity: T) -> T:
        """Create a new entity.

        Args:
            entity: Entity to create

        Returns:
            Created entity with generated fields (e.g., id)
        """
        try:
            row = self._entity_to_row(entity)
            columns = list(row.keys())
            values = list(row.values())

            query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING *").format(
                self._qualified_table,
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            )

            result = self._db.fetch_one(query, tuple(values), as_dict=True)
            if result is None:
                raise RepositoryError("Insert returned no result")

            logger.debug(f"Created entity in {self.table_name}")
            return self._row_to_entity(result)

        except RepositoryError:
            raise
        except Exception as e:
            raise RepositoryError(f"Failed to create entity: {e}") from e

    def create_many(self, entities: list[T]) -> list[T]:
        """Create multiple entities.

        Args:
            entities: List of entities to create

        Returns:
            List of created entities
        """
        if not entities:
            return []

        try:
            rows = [self._entity_to_row(e) for e in entities]
            columns = list(rows[0].keys())

            query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING *").format(
                self._qualified_table,
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            )

            results = []
            with (
                self._db.transaction() as conn,
                conn.cursor(row_factory=dict_row_factory) as cur,
            ):
                for row in rows:
                    cur.execute(query, tuple(row.values()))
                    result = cur.fetchone()
                    if result:
                        results.append(self._row_to_entity(result))

            logger.debug(f"Created {len(results)} entities in {self.table_name}")
            return results

        except Exception as e:
            raise RepositoryError(f"Failed to create entities: {e}") from e

    def update(self, entity: T) -> T:
        """Update an existing entity.

        Args:
            entity: Entity to update (must have primary key)

        Returns:
            Updated entity
        """
        try:
            row = self._entity_to_row(entity)

            if self.primary_key not in row:
                raise RepositoryError(
                    f"Entity must have '{self.primary_key}' for update"
                )

            pk_value = row.pop(self.primary_key)
            columns = list(row.keys())
            values = list(row.values())

            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(c)) for c in columns
            )

            query = sql.SQL("UPDATE {} SET {} WHERE {} = %s RETURNING *").format(
                self._qualified_table,
                set_clause,
                sql.Identifier(self.primary_key),
            )

            result = self._db.fetch_one(query, (*values, pk_value), as_dict=True)
            if result is None:
                raise RepositoryError(
                    f"Entity with {self.primary_key}={pk_value} not found"
                )

            logger.debug(f"Updated entity in {self.table_name}")
            return self._row_to_entity(result)

        except RepositoryError:
            raise
        except Exception as e:
            raise RepositoryError(f"Failed to update entity: {e}") from e

    def delete(self, id_value: Any) -> bool:
        """Delete an entity by its primary key.

        Args:
            id_value: Primary key value

        Returns:
            True if entity was deleted
        """
        try:
            query = sql.SQL("DELETE FROM {} WHERE {} = %s").format(
                self._qualified_table,
                sql.Identifier(self.primary_key),
            )
            rows_affected = self._db.execute(query, (id_value,))
            deleted = rows_affected > 0

            if deleted:
                logger.debug(f"Deleted entity from {self.table_name}")

            return deleted

        except Exception as e:
            raise RepositoryError(f"Failed to delete entity: {e}") from e

    def delete_by(self, conditions: dict[str, Any]) -> int:
        """Delete entities matching conditions.

        Args:
            conditions: Dictionary of column -> value conditions

        Returns:
            Number of deleted entities
        """
        if not conditions:
            raise RepositoryError("Conditions required for delete_by")

        try:
            where_parts = []
            values = []

            for column, value in conditions.items():
                if value is None:
                    where_parts.append(
                        sql.SQL("{} IS NULL").format(sql.Identifier(column))
                    )
                else:
                    where_parts.append(
                        sql.SQL("{} = %s").format(sql.Identifier(column))
                    )
                    values.append(value)

            query = sql.SQL("DELETE FROM {} WHERE {}").format(
                self._qualified_table,
                sql.SQL(" AND ").join(where_parts),
            )

            rows_affected = self._db.execute(query, tuple(values))
            logger.debug(f"Deleted {rows_affected} entities from {self.table_name}")
            return rows_affected

        except Exception as e:
            raise RepositoryError(f"Failed to delete by conditions: {e}") from e

    def count(self, conditions: dict[str, Any] | None = None) -> int:
        """Count entities.

        Args:
            conditions: Optional conditions to filter by

        Returns:
            Number of matching entities
        """
        try:
            if conditions:
                where_parts = []
                values = []

                for column, value in conditions.items():
                    if value is None:
                        where_parts.append(
                            sql.SQL("{} IS NULL").format(sql.Identifier(column))
                        )
                    else:
                        where_parts.append(
                            sql.SQL("{} = %s").format(sql.Identifier(column))
                        )
                        values.append(value)

                query = sql.SQL("SELECT COUNT(*) FROM {} WHERE {}").format(
                    self._qualified_table,
                    sql.SQL(" AND ").join(where_parts),
                )
                result = self._db.fetch_value(query, tuple(values))
            else:
                query = sql.SQL("SELECT COUNT(*) FROM {}").format(self._qualified_table)
                result = self._db.fetch_value(query)

            return int(result) if result else 0

        except Exception as e:
            raise RepositoryError(f"Failed to count: {e}") from e

    def exists(self, id_value: Any) -> bool:
        """Check if an entity exists.

        Args:
            id_value: Primary key value

        Returns:
            True if entity exists
        """
        try:
            query = sql.SQL("SELECT EXISTS(SELECT 1 FROM {} WHERE {} = %s)").format(
                self._qualified_table,
                sql.Identifier(self.primary_key),
            )
            result = self._db.fetch_value(query, (id_value,))
            return bool(result)

        except Exception as e:
            raise RepositoryError(f"Failed to check existence: {e}") from e


def dict_row_factory(cursor):
    """Row factory that returns dictionaries."""
    from psycopg.rows import dict_row

    return dict_row(cursor)
