# pytest-neon

[![Tests](https://github.com/ZainRizvi/pytest-neon/actions/workflows/tests.yml/badge.svg)](https://github.com/ZainRizvi/pytest-neon/actions/workflows/tests.yml)

A pytest plugin that provides Neon database branches for integration testing.

## Features

- **Automatic branch management**: Creates a test branch at session start, deletes at end
- **Branch expiry**: Auto-cleanup via 10-minute expiry (crash-safe)
- **Migration support**: Run migrations once, all tests share the migrated schema
- **pytest-xdist support**: All workers share a single branch
- **Minimal API calls**: Single branch creation reduces rate limiting issues

## Installation

```bash
pip install pytest-neon

# With optional database drivers
pip install pytest-neon[psycopg]     # psycopg v3 support
pip install pytest-neon[psycopg2]    # psycopg2 support
pip install pytest-neon[sqlalchemy]  # SQLAlchemy engine support
```

## Quick Start

1. Set environment variables:
```bash
export NEON_API_KEY="your-api-key"
export NEON_PROJECT_ID="your-project-id"
```

2. Use the `neon_branch` fixture in your tests:
```python
def test_query_users(neon_branch):
    import psycopg
    with psycopg.connect(neon_branch.connection_string) as conn:
        result = conn.execute("SELECT * FROM users").fetchall()
        assert len(result) >= 0
```

The `DATABASE_URL` environment variable is automatically set when the fixture is active.

## Fixtures

### `neon_branch` (session-scoped)

The main fixture providing a shared Neon branch for all tests.

```python
def test_example(neon_branch):
    # neon_branch.branch_id - Neon branch ID
    # neon_branch.project_id - Neon project ID
    # neon_branch.connection_string - PostgreSQL connection string
    # neon_branch.host - Database host
    pass
```

**Important**: All tests share the same branch. Data written by one test is visible to subsequent tests. See [Test Isolation](#test-isolation) for patterns to handle this.

### `neon_apply_migrations` (session-scoped)

Override this fixture to run migrations before tests:

```python
# conftest.py
@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_test_branch):
    """Run database migrations."""
    import subprocess
    subprocess.run(["alembic", "upgrade", "head"], check=True)
```

Or with Django:
```python
@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_test_branch):
    from django.core.management import call_command
    call_command("migrate", "--noinput")
```

Or with raw SQL:
```python
@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_test_branch):
    import psycopg
    branch, is_creator = _neon_test_branch
    with psycopg.connect(branch.connection_string) as conn:
        with open("schema.sql") as f:
            conn.execute(f.read())
        conn.commit()
```

### Connection Fixtures (Optional)

These require extra dependencies:

**`neon_connection`** - psycopg2 connection (requires `pytest-neon[psycopg2]`)
```python
def test_insert(neon_connection):
    cur = neon_connection.cursor()
    cur.execute("INSERT INTO users (name) VALUES (%s)", ("test",))
    neon_connection.commit()
```

**`neon_connection_psycopg`** - psycopg v3 connection (requires `pytest-neon[psycopg]`)
```python
def test_insert(neon_connection_psycopg):
    with neon_connection_psycopg.cursor() as cur:
        cur.execute("INSERT INTO users (name) VALUES ('test')")
    neon_connection_psycopg.commit()
```

**`neon_engine`** - SQLAlchemy engine (requires `pytest-neon[sqlalchemy]`)
```python
def test_query(neon_engine):
    from sqlalchemy import text
    with neon_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
```

## Test Isolation

Since all tests share a single branch, you may need to handle test isolation yourself. Here are recommended patterns:

### Transaction Rollback (Recommended)

```python
@pytest.fixture
def db_transaction(neon_branch):
    """Provide a database transaction that rolls back after each test."""
    import psycopg
    conn = psycopg.connect(neon_branch.connection_string)
    conn.execute("BEGIN")
    yield conn
    conn.execute("ROLLBACK")
    conn.close()

def test_insert(db_transaction):
    db_transaction.execute("INSERT INTO users (name) VALUES ('test')")
    # Automatically rolled back - next test won't see this
```

### Table Truncation

```python
@pytest.fixture(autouse=True)
def clean_tables(neon_branch):
    """Clean up test data after each test."""
    yield
    import psycopg
    with psycopg.connect(neon_branch.connection_string) as conn:
        conn.execute("TRUNCATE users, orders CASCADE")
        conn.commit()
```

### Unique Identifiers

```python
import uuid

def test_create_user(neon_branch):
    unique_id = uuid.uuid4().hex[:8]
    email = f"test_{unique_id}@example.com"
    # Create user with unique email - no conflicts with other tests
```

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `NEON_API_KEY` | Neon API key (required) |
| `NEON_PROJECT_ID` | Neon project ID (required) |
| `NEON_PARENT_BRANCH_ID` | Parent branch to create test branches from |
| `NEON_DATABASE` | Database name (default: `neondb`) |
| `NEON_ROLE` | Database role (default: `neondb_owner`) |

### Command Line Options

```bash
pytest --neon-api-key=KEY --neon-project-id=ID
pytest --neon-parent-branch=BRANCH_ID
pytest --neon-database=mydb --neon-role=myrole
pytest --neon-keep-branches  # Don't delete branches (for debugging)
pytest --neon-branch-expiry=600  # Branch expiry in seconds (default: 600)
pytest --neon-env-var=CUSTOM_URL  # Use custom env var instead of DATABASE_URL
```

### pytest.ini / pyproject.toml

```ini
[pytest]
neon_api_key = your-api-key
neon_project_id = your-project-id
neon_parent_branch = br-parent-123
neon_database = mydb
neon_role = myrole
neon_keep_branches = false
neon_branch_expiry = 600
neon_env_var = DATABASE_URL
```

## Architecture

```
Parent Branch (configured or project default)
    └── Test Branch (session-scoped, 10-min expiry)
            ↑ migrations run here ONCE, all tests share this
```

The plugin creates exactly **one branch per test session**:
1. First test triggers branch creation with auto-expiry
2. Migrations run once (if `neon_apply_migrations` is overridden)
3. All tests share the same branch
4. Branch deleted at session end (plus auto-expiry as safety net)

### pytest-xdist Support

When running with pytest-xdist, all workers share the same branch:
- First worker creates the branch and runs migrations
- Other workers wait for migrations to complete
- All workers see the same database state

```bash
pytest -n 4  # 4 workers, all sharing one branch
```

## Branch Naming

Branches are automatically named to help identify their source:

```
pytest-[git-branch]-[random]-test
```

**Examples:**
- `pytest-main-a1b2-test` - Test branch from `main`
- `pytest-feature-auth-c3d4-test` - Test branch from `feature/auth`
- `pytest-a1b2-test` - When not in a git repo

The git branch name is sanitized (only `a-z`, `0-9`, `-`, `_` allowed) and truncated to 15 characters.

## Upgrading from v2.x

Version 3.0 simplifies the plugin significantly. If you're upgrading from v2.x:

### Removed Fixtures

These fixtures have been removed:
- `neon_branch_readonly` → use `neon_branch`
- `neon_branch_readwrite` → use `neon_branch`
- `neon_branch_isolated` → use `neon_branch` + transaction rollback
- `neon_branch_dirty` → use `neon_branch`
- `neon_branch_shared` → use `neon_branch`

### Migration Hook Change

The migration hook now uses `_neon_test_branch` instead of `_neon_migration_branch`:

```python
# Before (v2.x)
@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_migration_branch):
    ...

# After (v3.x)
@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_test_branch):
    ...
```

### No Per-Test Reset

The v2.x `neon_branch_isolated` fixture reset the branch after each test. In v3.x, there's no automatic reset. Use transaction rollback or cleanup fixtures for test isolation.

## Troubleshooting

### Rate Limiting

The plugin includes automatic retry with exponential backoff for Neon API rate limits. If you're hitting rate limits:
- The plugin creates only 1-2 API calls per session (create + delete)
- Consider increasing `--neon-branch-expiry` to reduce cleanup calls

### Stale Connections (SQLAlchemy)

If using SQLAlchemy with connection pooling, use `pool_pre_ping=True`:
```python
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
```

This is a best practice for any cloud database where connections can be terminated externally.

### Branch Not Deleted

If a test run crashes, the branch auto-expires after 10 minutes (configurable). You can also use `--neon-keep-branches` to prevent deletion for debugging.

## License

MIT
