# PyPGKit

[![PyPI version](https://img.shields.io/pypi/v/PyPGKit.svg)](https://pypi.org/project/PyPGKit/)
[![Python versions](https://img.shields.io/pypi/pyversions/PyPGKit.svg)](https://pypi.org/project/PyPGKit/)
[![License](https://img.shields.io/pypi/l/PyPGKit.svg)](https://github.com/miichoow/PyPGKit/blob/main/LICENSE)
[![Tests](https://github.com/miichoow/PyPGKit/actions/workflows/tests.yml/badge.svg)](https://github.com/miichoow/PyPGKit/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/miichoow/PyPGKit/branch/main/graph/badge.svg)](https://codecov.io/gh/miichoow/PyPGKit)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A PostgreSQL database framework for Python using psycopg3 with connection pooling, thread-safety, and repository pattern.

## Features

- **Connection Pooling**: Thread-safe connection pool using `psycopg_pool`
- **Singleton Pattern**: Global database instance with `Database.init()` and `Database.get_instance()`
- **Repository Pattern**: Base repository class with CRUD operations
- **Automatic Setup**: Create database, user, and schema automatically
- **Fork Safety**: Compatible with gunicorn and other forking servers
- **Configurable Logging**: Built-in logging with multiple formats
- **Type Hints**: Full type annotations for better IDE support

## Installation

```bash
pip install PyPGKit
```

## Quick Start

### Basic Usage

```python
from pypgkit import Database, DatabaseConfig

# Initialize once at application startup
config = DatabaseConfig(
    host="localhost",
    port=5432,
    database="myapp",
    user="myuser",
    password="mypassword",
)

# This creates the database/user if needed and initializes the connection pool
Database.init(config=config, schema_path="schemas/init.sql")

# Use anywhere in your application
db = Database.get_instance()

# Execute queries
users = db.fetch_all("SELECT * FROM users WHERE active = %s", (True,))
```

### Environment Variables

pypgkit supports configuration via environment variables:

```bash
export PYPGKIT_HOST=localhost
export PYPGKIT_PORT=5432
export PYPGKIT_DATABASE=myapp
export PYPGKIT_USER=myuser
export PYPGKIT_PASSWORD=mypassword
```

```python
from pypgkit import Database, DatabaseConfig

# Load from environment
config = DatabaseConfig.from_env()
Database.init(config=config)
```

### Repository Pattern

Create type-safe repositories for your entities:

```python
from dataclasses import dataclass
from typing import Optional
from pypgkit import Database
from pypgkit.repositories import BaseRepository

@dataclass
class User:
    id: Optional[int] = None
    email: str = ""
    name: str = ""
    active: bool = True

class UserRepository(BaseRepository[User]):
    table_name = "users"
    primary_key = "id"

    def _row_to_entity(self, row: dict) -> User:
        return User(
            id=row.get("id"),
            email=row.get("email", ""),
            name=row.get("name", ""),
            active=row.get("active", True),
        )

    def _entity_to_row(self, entity: User) -> dict:
        row = {"email": entity.email, "name": entity.name, "active": entity.active}
        if entity.id is not None:
            row["id"] = entity.id
        return row

# Usage
db = Database.get_instance()
repo = UserRepository(db)

# Create
user = repo.create(User(email="user@example.com", name="John"))

# Read
user = repo.find_by_id(1)
users = repo.find_all(limit=10)
active_users = repo.find_by({"active": True})

# Update
user.name = "Jane"
repo.update(user)

# Delete
repo.delete(1)
```

### Logging Configuration

```python
from pypgkit.logging import configure_logging, LogLevel, DETAILED_FORMAT

# Basic configuration
configure_logging(level=LogLevel.INFO)

# Debug mode with detailed format
configure_logging(level=LogLevel.DEBUG, format=DETAILED_FORMAT)

# Log to file
configure_logging(level=LogLevel.INFO, filename="pypgkit.log")

# Include psycopg logs
configure_logging(level=LogLevel.DEBUG, include_psycopg=True)
```

### Automatic Database Setup

pypgkit can automatically create the database and user if they don't exist:

```python
from pypgkit import Database, DatabaseConfig

config = DatabaseConfig(
    database="myapp",
    user="myuser",
    password="mypassword",
)

# Interactive mode (prompts for admin credentials)
Database.init(config=config, auto_setup=True, interactive=True)

# Non-interactive mode (for CI/CD)
from pypgkit.setup import setup_database
setup_database(
    config,
    admin_user="postgres",
    admin_password="adminpass",
    interactive=False,
)
```

### Using with Gunicorn

pypgkit automatically handles fork detection for compatibility with gunicorn:

```python
# wsgi.py
from pypgkit import Database, DatabaseConfig

config = DatabaseConfig.from_env()
Database.init(config=config)

# The connection pool automatically resets after fork
```

```bash
gunicorn -w 4 myapp:app
```

## API Reference

### DatabaseConfig

Configuration dataclass for database connections.

| Parameter          | Type  | Default     | Description                        |
|--------------------|-------|-------------|------------------------------------|
| host               | str   | "localhost" | Database host                      |
| port               | int   | 5432        | Database port                      |
| database           | str   | "postgres"  | Database name                      |
| user               | str   | "postgres"  | Database user                      |
| password           | str   | ""          | Database password                  |
| connection_string  | str   | None        | Full connection string (optional)  |
| min_connections    | int   | 1           | Minimum pool connections           |
| max_connections    | int   | 10          | Maximum pool connections           |
| connection_timeout | float | 30.0        | Connection timeout in seconds      |
| sslmode            | str   | "prefer"    | SSL mode                           |
| check_connection   | bool  | True        | Enable pool health checks          |
| max_idle_time      | float | 600.0       | Max idle time before connection recycled |

### Database

Main database facade class.

| Method                                                                    | Description                          |
|---------------------------------------------------------------------------|--------------------------------------|
| `Database.init(config, schema_path, schema_sql, auto_setup, interactive)` | Initialize singleton                 |
| `Database.get_instance()`                                                 | Get initialized instance             |
| `Database.is_initialized()`                                               | Check if initialized                 |
| `Database.reset_instance()`                                               | Reset singleton (for testing)        |
| `db.connect()`                                                            | Establish connection                 |
| `db.disconnect()`                                                         | Close connection pool                |
| `db.execute(query, params)`                                               | Execute query, return row count      |
| `db.execute_many(query, params_seq)`                                      | Execute with multiple parameter sets |
| `db.fetch_one(query, params, as_dict)`                                    | Fetch single row                     |
| `db.fetch_all(query, params, as_dict)`                                    | Fetch all rows                       |
| `db.fetch_value(query, params)`                                           | Fetch single value                   |
| `db.table_exists(table_name, schema)`                                     | Check if table exists                |
| `db.transaction()`                                                        | Context manager for transactions     |
| `db.get_stats()`                                                          | Get connection pool statistics       |
| `db.health_check()`                                                       | Check connection health              |

### BaseRepository

Base class for repositories with CRUD operations.

| Method                                                        | Description                |
|---------------------------------------------------------------|----------------------------|
| `find_by_id(id)`                                              | Find entity by primary key |
| `find_all(limit, offset, order_by, order_desc)`               | Find all entities          |
| `find_by(conditions, limit, offset, order_by, order_desc)`    | Find by conditions         |
| `find_one_by(conditions)`                                     | Find one by conditions     |
| `create(entity)`                                              | Create new entity          |
| `create_many(entities)`                                       | Create multiple entities   |
| `update(entity)`                                              | Update existing entity     |
| `delete(id)`                                                  | Delete by primary key      |
| `delete_by(conditions)`                                       | Delete by conditions       |
| `count(conditions)`                                           | Count entities             |
| `exists(id)`                                                  | Check if entity exists     |

## Development

### Setup

```bash
git clone https://github.com/miichoow/PyPGKit.git
cd PyPGKit
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest tests/ -v --cov=pypgkit --cov-report=term-missing
```

### Linting

```bash
ruff check pypgkit/
ruff format pypgkit/
```

## License

MIT License - see LICENSE file for details.
