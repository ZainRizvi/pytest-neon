# pytest-neon

[![Tests](https://github.com/ZainRizvi/pytest-neon/actions/workflows/tests.yml/badge.svg)](https://github.com/ZainRizvi/pytest-neon/actions/workflows/tests.yml)

Pytest plugin for [Neon](https://neon.tech) database branch isolation in tests.

Each test gets its own isolated database state via Neon's instant branching and reset features. Branches are automatically cleaned up after tests complete.

## Features

- **Isolated test environments**: Each test runs against a clean database state
- **Fast resets**: ~0.5s per test to reset the branch (not create a new one)
- **Automatic cleanup**: Branches are deleted after tests, with auto-expiry fallback
- **Zero infrastructure**: No Docker, no local Postgres, no manual setup
- **Real database testing**: Test against actual Postgres with your production schema
- **Automatic `DATABASE_URL`**: Connection string is set in environment automatically
- **Driver agnostic**: Bring your own driver, or use the optional convenience fixtures

## Installation

Core package (bring your own database driver):

```bash
pip install pytest-neon
```

With optional convenience fixtures:

```bash
# For psycopg v3 (recommended)
pip install pytest-neon[psycopg]

# For psycopg2 (legacy)
pip install pytest-neon[psycopg2]

# For SQLAlchemy
pip install pytest-neon[sqlalchemy]

# Multiple extras
pip install pytest-neon[psycopg,sqlalchemy]
```

## Quick Start

1. Set environment variables:

```bash
export NEON_API_KEY="your-api-key"
export NEON_PROJECT_ID="your-project-id"
```

2. Write tests:

```python
def test_user_creation(neon_branch_isolated):
    # DATABASE_URL is automatically set to the test branch
    import psycopg  # Your own install

    with psycopg.connect() as conn:  # Uses DATABASE_URL by default
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (email) VALUES ('test@example.com')")
        conn.commit()
    # Branch automatically resets after test - next test sees clean state
```

3. Run tests:

```bash
pytest
```

## Fixtures

**Which fixture should I use?**

| Fixture | Scope | Reset | Best For | Overhead |
|---------|-------|-------|----------|----------|
| `neon_branch_readonly` | session | None | Read-only tests (SELECT) - enforced | ~0s/test |
| `neon_branch_dirty` | session | None | Fast writes, shared state OK | ~0s/test |
| `neon_branch_isolated` | function | After each test | Write tests needing isolation | ~0.5s/test |
| `neon_branch_shared` | module | None | Module-level shared state | ~0s/test |

**Quick guide:**
- **Use `neon_branch_readonly`** if your test only reads data (SELECT queries). This uses a true read-only endpoint that enforces read-only access at the database level. Any write attempt will fail with a database error.
- **Use `neon_branch_dirty`** if your tests write data but can tolerate shared state across the session. Fast because no reset or branch creation per test.
- **Use `neon_branch_isolated`** if your test modifies data and needs isolation from other tests. Resets the branch after each test.

### Architecture

The plugin creates a branch hierarchy to efficiently support all fixture types:

```
Parent Branch (configured or project default)
    └── Migration Branch (session-scoped, read_write endpoint)
            │   ↑ migrations run here ONCE
            │
            ├── Read-only Endpoint (read_only endpoint ON migration branch)
            │       ↑ neon_branch_readonly uses this (enforced read-only)
            │
            ├── Dirty Branch (session-scoped child, shared across ALL workers)
            │       ↑ neon_branch_dirty uses this
            │
            └── Isolated Branch (one per xdist worker, lazily created)
                    ↑ neon_branch_isolated uses this, reset after each test
```

### `neon_branch_readonly` (session-scoped, enforced read-only)

**Use this fixture** for tests that only read data. It uses a true `read_only` endpoint on the migration branch, which enforces read-only access at the database level. Any attempt to INSERT, UPDATE, or DELETE will result in a database error.

```python
def test_query_users(neon_branch_readonly):
    # DATABASE_URL is set automatically
    import psycopg
    with psycopg.connect(neon_branch_readonly.connection_string) as conn:
        result = conn.execute("SELECT * FROM users").fetchall()
        assert len(result) >= 0

        # This would fail with a database error:
        # conn.execute("INSERT INTO users (name) VALUES ('test')")
```

**Use this when**:
- Tests only perform SELECT queries
- Tests don't modify database state
- You want maximum performance with true enforcement

**Session-scoped**: The endpoint is created once and shared across all tests and workers.

**Performance**: ~1.5s initial setup per session, **no per-test overhead**. For 10 read-only tests, expect only ~1.5s total overhead.

### `neon_branch_dirty` (session-scoped, shared state)

Use this fixture when your tests write data but can tolerate shared state across the session. All tests share the same branch and writes persist (no cleanup between tests). This is faster than `neon_branch_isolated` because there's no reset overhead.

```python
def test_insert_user(neon_branch_dirty):
    # DATABASE_URL is set automatically
    import psycopg
    with psycopg.connect(neon_branch_dirty.connection_string) as conn:
        conn.execute("INSERT INTO users (name) VALUES ('test')")
        conn.commit()
    # Data persists - subsequent tests will see this user

def test_count_users(neon_branch_dirty):
    # This test sees data from previous tests
    import psycopg
    with psycopg.connect(neon_branch_dirty.connection_string) as conn:
        result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        # Count includes users from previous tests
```

**Use this when**:
- Most tests can share database state without interference
- You want maximum performance with minimal API calls
- You manually manage test data cleanup if needed
- You're combining it with `neon_branch_isolated` for specific tests that need clean state

**Warning**: Data written by one test WILL be visible to subsequent tests AND to other xdist workers. This is truly shared - use `neon_branch_isolated` for tests that require guaranteed clean state.

**pytest-xdist note**: ALL workers share the same dirty branch. Concurrent writes from different workers may conflict. This is "dirty" by design - for isolation, use `neon_branch_isolated`.

**Performance**: ~1.5s initial setup per session, **no per-test overhead**. For 10 write tests, expect only ~1.5s total overhead.

### `neon_branch_isolated` (function-scoped, full isolation)

Use this fixture when your test modifies data and needs isolation from other tests. Each xdist worker has its own branch, and the branch is reset to the migration state after each test.

```python
def test_insert_user(neon_branch_isolated):
    # DATABASE_URL is set automatically
    import psycopg
    with psycopg.connect(neon_branch_isolated.connection_string) as conn:
        # Guaranteed clean state - no data from other tests
        # (only migration/seed data if you defined neon_apply_migrations)
        result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        initial_count = result[0]

        conn.execute("INSERT INTO users (name) VALUES ('test')")
        conn.commit()

        # Verify our insert worked
        result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        assert result[0] == initial_count + 1
    # Branch resets after this test - next test sees clean state
```

**Use this when**:
- A test modifies database state (INSERT, UPDATE, DELETE)
- Test isolation is important
- You're combining it with `neon_branch_dirty` for most tests but need isolation for specific ones

**SQLAlchemy Users**: If you create your own engine (not using the `neon_engine` fixture), you MUST use `pool_pre_ping=True`:

```python
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
```

Branch resets terminate server-side connections. Without `pool_pre_ping`, SQLAlchemy may reuse dead pooled connections, causing SSL errors.

**pytest-xdist note**: Each worker has its own isolated branch. Resets only affect that worker's branch, so workers don't interfere with each other.

**Performance**: ~1.5s initial setup per session per worker + ~0.5s reset per test. For 10 write tests, expect ~6.5s total overhead.

### `NeonBranch` dataclass

All fixtures return a `NeonBranch` dataclass with:

- `branch_id`: The Neon branch ID
- `project_id`: The Neon project ID
- `connection_string`: Full PostgreSQL connection URI
- `host`: The database host
- `parent_id`: The parent branch ID (used for resets)
- `endpoint_id`: The endpoint ID (for cleanup)

### `neon_branch_readwrite` (deprecated)

> **Deprecated**: Use `neon_branch_isolated` instead.

This fixture is an alias for `neon_branch_isolated` and will emit a deprecation warning.

### `neon_branch` (deprecated)

> **Deprecated**: Use `neon_branch_isolated`, `neon_branch_dirty`, or `neon_branch_readonly` instead.

This fixture is an alias for `neon_branch_isolated` and will emit a deprecation warning.

### `neon_branch_shared` (module-scoped, no isolation)

Creates one branch per test module and shares it across all tests without resetting. This is the fastest option but tests can see each other's data modifications.

```python
def test_read_only_query(neon_branch_shared):
    # Fast: no reset between tests
    # Warning: data from other tests in this module may be visible
    conn = psycopg.connect(neon_branch_shared.connection_string)
```

**Use this when**:
- Tests are read-only
- Tests don't interfere with each other
- You manually clean up test data
- Maximum speed is more important than isolation

**Performance**: ~1.5s initial setup per module, no per-test overhead.

### `neon_connection_psycopg` (psycopg v3)

Convenience fixture providing a [psycopg v3](https://www.psycopg.org/psycopg3/) connection with automatic rollback and cleanup.

**Requires:** `pip install pytest-neon[psycopg]`

```python
def test_insert(neon_connection_psycopg):
    with neon_connection_psycopg.cursor() as cur:
        cur.execute("INSERT INTO users (name) VALUES (%s)", ("test",))
    neon_connection_psycopg.commit()

    with neon_connection_psycopg.cursor() as cur:
        cur.execute("SELECT name FROM users")
        assert cur.fetchone()[0] == "test"
```

### `neon_connection` (psycopg2)

Convenience fixture providing a [psycopg2](https://www.psycopg.org/docs/) connection with automatic rollback and cleanup.

**Requires:** `pip install pytest-neon[psycopg2]`

```python
def test_insert(neon_connection):
    cur = neon_connection.cursor()
    cur.execute("INSERT INTO users (name) VALUES (%s)", ("test",))
    neon_connection.commit()
```

### `neon_engine` (SQLAlchemy)

Convenience fixture providing a [SQLAlchemy](https://www.sqlalchemy.org/) engine with automatic disposal.

**Requires:** `pip install pytest-neon[sqlalchemy]`

```python
from sqlalchemy import text

def test_query(neon_engine):
    with neon_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
```

### Using Your Own SQLAlchemy Engine

If you have a module-level SQLAlchemy engine (common pattern) and use `neon_branch_isolated`, you **must** use `pool_pre_ping=True`:

```python
# database.py
from sqlalchemy import create_engine
from config import DATABASE_URL

# pool_pre_ping=True is REQUIRED when using neon_branch_isolated
# It verifies connections are alive before using them
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
```

**Why?** After each test, `neon_branch_isolated` resets the branch which terminates server-side connections. Without `pool_pre_ping`, SQLAlchemy may try to reuse a dead pooled connection, causing `SSL connection has been closed unexpectedly` errors.

**Note**: If you only use `neon_branch_readonly` or `neon_branch_dirty`, `pool_pre_ping` is not required since no resets occur.

This is also a best practice for any cloud database (Neon, RDS, etc.) where connections can be terminated externally.

## Migrations

pytest-neon supports running migrations once before tests, with all test resets preserving the migrated state.

### How It Works

The plugin always creates a migration branch from your configured parent:

```
Parent Branch (your configured parent)
    └── Migration Branch (session-scoped)
            │   ↑ migrations run here ONCE
            │
            ├── Read-only Endpoint (for neon_branch_readonly)
            ├── Dirty Branch (for neon_branch_dirty)
            └── Isolated Branches (for neon_branch_isolated)
```

This means:
- Migrations run **once per test session** (not per test or per module)
- Each test reset restores to the **post-migration state**
- Tests always see your migrated schema

### Setup

Override the `neon_apply_migrations` fixture in your `conftest.py`:

**Alembic:**
```python
# conftest.py
import subprocess
import pytest

@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_migration_branch):
    """Run Alembic migrations before tests."""
    # DATABASE_URL is already set to the migration branch
    subprocess.run(["alembic", "upgrade", "head"], check=True)
```

**Django:**
```python
# conftest.py
import pytest

@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_migration_branch):
    """Run Django migrations before tests."""
    from django.core.management import call_command
    call_command("migrate", "--noinput")
```

**Raw SQL:**
```python
# conftest.py
import pytest

@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_migration_branch):
    """Apply schema from SQL file."""
    import psycopg
    with psycopg.connect(_neon_migration_branch.connection_string) as conn:
        with open("schema.sql") as f:
            conn.execute(f.read())
        conn.commit()
```

**Custom migration tool:**
```python
# conftest.py
import pytest

@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_migration_branch):
    """Run custom migrations."""
    from myapp.migrations import run_migrations
    run_migrations(_neon_migration_branch.connection_string)
```

### Important Notes

- The `_neon_migration_branch` parameter gives you access to the `NeonBranch` object with `connection_string`, `branch_id`, etc.
- `DATABASE_URL` (or your configured env var) is automatically set when the fixture runs
- If you don't override `neon_apply_migrations`, no migrations run (the fixture is a no-op by default)
- Migrations run before any test branches are created, so all tests see the same migrated schema

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `NEON_API_KEY` | Your Neon API key | Yes |
| `NEON_PROJECT_ID` | Your Neon project ID | Yes |
| `NEON_PARENT_BRANCH_ID` | Parent branch to create test branches from | No |
| `NEON_DATABASE` | Database name (default: `neondb`) | No |
| `NEON_ROLE` | Database role (default: `neondb_owner`) | No |

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--neon-api-key` | Neon API key | `NEON_API_KEY` env |
| `--neon-project-id` | Neon project ID | `NEON_PROJECT_ID` env |
| `--neon-parent-branch` | Parent branch ID | Project default |
| `--neon-database` | Database name | `neondb` |
| `--neon-role` | Database role | `neondb_owner` |
| `--neon-keep-branches` | Don't delete branches after tests | `false` |
| `--neon-branch-expiry` | Branch auto-expiry in seconds | `600` (10 min) |
| `--neon-env-var` | Environment variable for connection string | `DATABASE_URL` |

Examples:

```bash
# Keep branches for debugging
pytest --neon-keep-branches

# Disable auto-expiry
pytest --neon-branch-expiry=0

# Use a different env var
pytest --neon-env-var=TEST_DATABASE_URL
```

### pyproject.toml / pytest.ini

You can also configure options in your `pyproject.toml`:

```toml
[tool.pytest.ini_options]
neon_database = "mydb"
neon_role = "myrole"
neon_keep_branches = true
neon_branch_expiry = "300"
```

Or in `pytest.ini`:

```ini
[pytest]
neon_database = mydb
neon_role = myrole
neon_keep_branches = true
neon_branch_expiry = 300
```

**Priority order**: CLI options > environment variables > ini settings > defaults

## CI/CD Integration

### GitHub Actions

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -e .[psycopg,dev]

      - name: Run tests
        env:
          NEON_API_KEY: ${{ secrets.NEON_API_KEY }}
          NEON_PROJECT_ID: ${{ secrets.NEON_PROJECT_ID }}
        run: pytest
```

## How It Works

1. At the start of the test session, the plugin creates a migration branch from your parent branch
2. If defined, migrations run once on the migration branch
3. Test branches (dirty, isolated) are created as children of the migration branch
4. `DATABASE_URL` is set to point to the appropriate branch for each fixture
5. For `neon_branch_isolated`, the branch is reset after each test (~0.5s)
6. After all tests complete, branches are deleted
7. As a safety net, branches auto-expire after 10 minutes even if cleanup fails

Branches use copy-on-write storage, so you only pay for data that differs from the parent branch.

### What Reset Does

The `neon_branch_isolated` fixture uses Neon's branch restore API to reset database state after each test:

- **Data changes are reverted**: All INSERT, UPDATE, DELETE operations are undone
- **Schema changes are reverted**: CREATE TABLE, ALTER TABLE, DROP TABLE, etc. are undone
- **Sequences are reset**: Auto-increment counters return to parent state
- **Complete rollback**: The branch is restored to the exact state of the parent at the time the child branch was created

This is similar to database transactions but at the branch level.

## Branch Naming

Branches are automatically named to help identify their source:

```
pytest-[git-branch]-[random]-[suffix]
```

**Examples:**
- `pytest-main-a1b2-migration` - Migration branch from `main`
- `pytest-feature-auth-c3d4-dirty` - Dirty branch from `feature/auth`
- `pytest-main-a1b2-isolated-gw0` - Isolated branch for xdist worker 0
- `pytest-a1b2-migration` - When not in a git repo

The git branch name is sanitized (only `a-z`, `0-9`, `-`, `_` allowed) and truncated to 15 characters. This makes it easy to identify orphaned branches in the Neon console.

## Parallel Test Execution (pytest-xdist)

This plugin supports parallel test execution with [pytest-xdist](https://pytest-xdist.readthedocs.io/).

```bash
# Run tests in parallel with 4 workers
pip install pytest-xdist
pytest -n 4
```

**How it works:**

| Fixture | xdist Behavior |
|---------|----------------|
| `neon_branch_readonly` | Shared across ALL workers (read-only endpoint) |
| `neon_branch_dirty` | Shared across ALL workers (concurrent writes possible) |
| `neon_branch_isolated` | One branch per worker (e.g., `-isolated-gw0`, `-isolated-gw1`) |

**Key points:**
- The migration branch is created once and shared across all workers
- `neon_branch_readonly` and `neon_branch_dirty` share resources across workers
- `neon_branch_isolated` creates one branch per worker for isolation
- Workers run tests in parallel without database state interference (when using isolated)
- All branches are cleaned up after the test session

**Cost implications:**
- Running with `-n 4` creates: 1 migration branch + 1 dirty branch + 4 isolated branches (if all workers use isolated)
- Choose your parallelism level based on your Neon plan's branch limits
- Each worker's isolated branch is reset after each test using the fast reset operation (~0.5s)

## Troubleshooting

### "psycopg not installed" or "psycopg2 not installed"

The convenience fixtures require their respective drivers. Install the appropriate extra:

```bash
# For neon_connection_psycopg fixture
pip install pytest-neon[psycopg]

# For neon_connection fixture
pip install pytest-neon[psycopg2]

# For neon_engine fixture
pip install pytest-neon[sqlalchemy]
```

Or use the core fixtures with your own driver:

```python
def test_example(neon_branch_isolated):
    import my_preferred_driver
    conn = my_preferred_driver.connect(neon_branch_isolated.connection_string)
```

### "Neon API key not configured"

Set the `NEON_API_KEY` environment variable or use the `--neon-api-key` CLI option.

### "Neon project ID not configured"

Set the `NEON_PROJECT_ID` environment variable or use the `--neon-project-id` CLI option.

### "SSL connection has been closed unexpectedly" (SQLAlchemy)

This happens when SQLAlchemy tries to reuse a pooled connection after a branch reset. The reset terminates server-side connections, but SQLAlchemy's pool doesn't know.

**Fix:** Add `pool_pre_ping=True` to your engine:

```python
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
```

This makes SQLAlchemy verify connections before using them, automatically discarding stale ones.

## License

MIT
