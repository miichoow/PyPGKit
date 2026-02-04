"""Tests for pypgkit.repositories.base module."""

from dataclasses import dataclass
from typing import Optional
from unittest import mock

import pytest

from pypgkit.database import Database
from pypgkit.exceptions import RepositoryError
from pypgkit.repositories.base import BaseRepository


@dataclass
class SampleEntity:
    """Sample entity for repository tests."""

    id: Optional[int] = None
    name: str = ""
    active: bool = True


class SampleRepository(BaseRepository[SampleEntity]):
    """Sample repository implementation."""

    table_name = "test_entities"
    primary_key = "id"

    def _row_to_entity(self, row: dict) -> SampleEntity:
        return SampleEntity(
            id=row.get("id"),
            name=row.get("name", ""),
            active=row.get("active", True),
        )

    def _entity_to_row(self, entity: SampleEntity) -> dict:
        row = {"name": entity.name, "active": entity.active}
        if entity.id is not None:
            row["id"] = entity.id
        return row


class InvalidRepository(BaseRepository[SampleEntity]):
    """Repository without table_name for testing validation."""

    def _row_to_entity(self, _row: dict) -> SampleEntity:
        return SampleEntity()

    def _entity_to_row(self, _entity: SampleEntity) -> dict:
        return {}


class TestBaseRepository:
    """Tests for BaseRepository class."""

    def setup_method(self):
        """Setup mock database."""
        Database.reset_instance()
        self.mock_db = mock.MagicMock(spec=Database)

    def teardown_method(self):
        """Clean up."""
        Database.reset_instance()

    def test_repository_requires_table_name(self):
        """Test repository raises error without table_name."""
        with pytest.raises(RepositoryError, match="table_name"):
            InvalidRepository(self.mock_db)

    def test_repository_init(self):
        """Test repository initialization."""
        repo = SampleRepository(self.mock_db)
        assert repo._db == self.mock_db
        assert repo.table_name == "test_entities"
        assert repo.primary_key == "id"

    def test_find_by_id(self):
        """Test find_by_id method."""
        self.mock_db.fetch_one.return_value = {"id": 1, "name": "test", "active": True}
        repo = SampleRepository(self.mock_db)

        result = repo.find_by_id(1)

        assert result is not None
        assert result.id == 1
        assert result.name == "test"

    def test_find_by_id_not_found(self):
        """Test find_by_id returns None when not found."""
        self.mock_db.fetch_one.return_value = None
        repo = SampleRepository(self.mock_db)

        result = repo.find_by_id(999)

        assert result is None

    def test_find_all(self):
        """Test find_all method."""
        self.mock_db.fetch_all.return_value = [
            {"id": 1, "name": "a", "active": True},
            {"id": 2, "name": "b", "active": False},
        ]
        repo = SampleRepository(self.mock_db)

        result = repo.find_all()

        assert len(result) == 2
        assert result[0].name == "a"
        assert result[1].name == "b"

    def test_find_all_with_limit(self):
        """Test find_all with limit."""
        self.mock_db.fetch_all.return_value = [{"id": 1, "name": "a", "active": True}]
        repo = SampleRepository(self.mock_db)

        repo.find_all(limit=10)

        # Verify LIMIT was included in query
        call_args = self.mock_db.fetch_all.call_args
        query = str(call_args[0][0])
        assert "LIMIT" in query.upper()

    def test_find_all_with_order(self):
        """Test find_all with ordering."""
        self.mock_db.fetch_all.return_value = []
        repo = SampleRepository(self.mock_db)

        repo.find_all(order_by="name", order_desc=True)

        call_args = self.mock_db.fetch_all.call_args
        query = str(call_args[0][0])
        assert "ORDER BY" in query.upper()

    def test_find_by(self):
        """Test find_by method with conditions."""
        self.mock_db.fetch_all.return_value = [
            {"id": 1, "name": "test", "active": True}
        ]
        repo = SampleRepository(self.mock_db)

        result = repo.find_by({"active": True})

        assert len(result) == 1

    def test_find_by_empty_conditions(self):
        """Test find_by with empty conditions calls find_all."""
        self.mock_db.fetch_all.return_value = []
        repo = SampleRepository(self.mock_db)

        repo.find_by({})

        # Should still work (returns all)
        self.mock_db.fetch_all.assert_called()

    def test_find_one_by(self):
        """Test find_one_by method."""
        self.mock_db.fetch_all.return_value = [
            {"id": 1, "name": "test", "active": True}
        ]
        repo = SampleRepository(self.mock_db)

        result = repo.find_one_by({"name": "test"})

        assert result is not None
        assert result.name == "test"

    def test_find_one_by_not_found(self):
        """Test find_one_by returns None when not found."""
        self.mock_db.fetch_all.return_value = []
        repo = SampleRepository(self.mock_db)

        result = repo.find_one_by({"name": "nonexistent"})

        assert result is None

    def test_create(self):
        """Test create method."""
        self.mock_db.fetch_one.return_value = {"id": 1, "name": "new", "active": True}
        repo = SampleRepository(self.mock_db)

        entity = SampleEntity(name="new")
        result = repo.create(entity)

        assert result.id == 1
        assert result.name == "new"

    def test_create_returns_none_raises(self):
        """Test create raises when insert returns None."""
        self.mock_db.fetch_one.return_value = None
        repo = SampleRepository(self.mock_db)

        with pytest.raises(RepositoryError, match="no result"):
            repo.create(SampleEntity(name="test"))

    def test_update(self):
        """Test update method."""
        self.mock_db.fetch_one.return_value = {
            "id": 1,
            "name": "updated",
            "active": True,
        }
        repo = SampleRepository(self.mock_db)

        entity = SampleEntity(id=1, name="updated")
        result = repo.update(entity)

        assert result.name == "updated"

    def test_update_without_id_raises(self):
        """Test update raises without primary key."""
        repo = SampleRepository(self.mock_db)

        with pytest.raises(RepositoryError, match="id"):
            repo.update(SampleEntity(name="test"))

    def test_update_not_found_raises(self):
        """Test update raises when entity not found."""
        self.mock_db.fetch_one.return_value = None
        repo = SampleRepository(self.mock_db)

        with pytest.raises(RepositoryError, match="not found"):
            repo.update(SampleEntity(id=999, name="test"))

    def test_delete(self):
        """Test delete method."""
        self.mock_db.execute.return_value = 1
        repo = SampleRepository(self.mock_db)

        result = repo.delete(1)

        assert result is True

    def test_delete_not_found(self):
        """Test delete returns False when not found."""
        self.mock_db.execute.return_value = 0
        repo = SampleRepository(self.mock_db)

        result = repo.delete(999)

        assert result is False

    def test_delete_by(self):
        """Test delete_by method."""
        self.mock_db.execute.return_value = 3
        repo = SampleRepository(self.mock_db)

        result = repo.delete_by({"active": False})

        assert result == 3

    def test_delete_by_empty_conditions_raises(self):
        """Test delete_by raises with empty conditions."""
        repo = SampleRepository(self.mock_db)

        with pytest.raises(RepositoryError, match="Conditions required"):
            repo.delete_by({})

    def test_count(self):
        """Test count method."""
        self.mock_db.fetch_value.return_value = 42
        repo = SampleRepository(self.mock_db)

        result = repo.count()

        assert result == 42

    def test_count_with_conditions(self):
        """Test count with conditions."""
        self.mock_db.fetch_value.return_value = 10
        repo = SampleRepository(self.mock_db)

        result = repo.count({"active": True})

        assert result == 10

    def test_exists(self):
        """Test exists method."""
        self.mock_db.fetch_value.return_value = True
        repo = SampleRepository(self.mock_db)

        result = repo.exists(1)

        assert result is True

    def test_exists_not_found(self):
        """Test exists returns False when not found."""
        self.mock_db.fetch_value.return_value = False
        repo = SampleRepository(self.mock_db)

        result = repo.exists(999)

        assert result is False
