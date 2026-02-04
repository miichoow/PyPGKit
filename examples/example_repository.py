"""Example repository implementation using pypgkit.

This example shows how simple it is to use pypgkit:
1. Configure logging (optional)
2. Define your entity (dataclass)
3. Define your repository (extend BaseRepository)
4. Initialize the database once with Database.init()
5. Use Database.get_instance() anywhere

That's it! pypgkit handles:
- Singleton database instance
- Connection pooling
- Database/user creation (with admin prompts if needed)
- Schema initialization
- Thread safety
- Configurable logging
"""

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pypgkit import (
    BaseRepository,
    Database,
    DatabaseConfig,
    configure_logging,
    LogLevel,
)


# =============================================================================
# 1. DEFINE YOUR ENTITY
# =============================================================================

@dataclass
class User:
    """User entity."""

    id: Optional[int] = None
    email: str = ""
    username: str = ""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    is_active: bool = True
    is_verified: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# =============================================================================
# 2. DEFINE YOUR REPOSITORY
# =============================================================================

class UserRepository(BaseRepository[User]):
    """Repository for User entities."""

    table_name = "users"
    primary_key = "id"

    def _row_to_entity(self, row: dict) -> User:
        return User(
            id=row.get("id"),
            email=row.get("email", ""),
            username=row.get("username", ""),
            first_name=row.get("first_name"),
            last_name=row.get("last_name"),
            is_active=row.get("is_active", True),
            is_verified=row.get("is_verified", False),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    def _entity_to_row(self, entity: User) -> dict:
        row = {
            "email": entity.email,
            "username": entity.username,
            "first_name": entity.first_name,
            "last_name": entity.last_name,
            "is_active": entity.is_active,
            "is_verified": entity.is_verified,
        }
        if entity.id is not None:
            row["id"] = entity.id
        return row

    def find_by_email(self, email: str) -> Optional[User]:
        return self.find_one_by({"email": email})

    def find_active_users(self) -> list[User]:
        return self.find_by({"is_active": True}, order_by="created_at", order_desc=True)


# =============================================================================
# 3. SERVICE FUNCTIONS (use Database.get_instance())
# =============================================================================

def get_user_repo() -> UserRepository:
    """Get UserRepository using the database singleton."""
    return UserRepository(Database.get_instance())


def create_user(email: str, username: str, first_name: str, last_name: str) -> User:
    """Create a new user."""
    return get_user_repo().create(User(
        email=email,
        username=username,
        first_name=first_name,
        last_name=last_name,
    ))


def get_user_by_email(email: str) -> Optional[User]:
    """Find user by email."""
    return get_user_repo().find_by_email(email)


def delete_user(user_id: int) -> bool:
    """Delete a user."""
    return get_user_repo().delete(user_id)


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Configure logging
    configure_logging(level=LogLevel.INFO)

    # Get paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    schema_path = os.path.join(project_dir, "schemas", "init.sql")

    # Configuration
    config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="example_db",
        user="example_user",
        password="example_password",
    )

    # ==========================================================================
    # INITIALIZE ONCE - at application startup
    # ==========================================================================
    Database.init(config=config, schema_path=schema_path)

    # ==========================================================================
    # USE ANYWHERE - with Database.get_instance()
    # ==========================================================================

    print("\n" + "=" * 50)
    print("PYPGKIT SINGLETON EXAMPLE")
    print("=" * 50)

    # Verify it's a singleton
    print("\n--- Singleton verification ---")
    db1 = Database.get_instance()
    db2 = Database.get_instance()
    print(f"Same instance: {db1 is db2}")

    # Clean up existing test user
    repo = get_user_repo()
    existing = repo.find_by_email("john@example.com")
    if existing:
        repo.delete(existing.id)

    # Use service functions (they use get_instance() internally)
    print("\n--- Create ---")
    user = create_user("john@example.com", "johndoe", "John", "Doe")
    print(f"Created: {user.first_name} (id={user.id})")

    print("\n--- Read ---")
    found = get_user_by_email("john@example.com")
    print(f"Found: {found.email}")

    print("\n--- Delete ---")
    deleted = delete_user(user.id)
    print(f"Deleted: {deleted}")

    print("\n" + "=" * 50)
    print("SUCCESS!")
    print("=" * 50)


if __name__ == "__main__":
    main()
