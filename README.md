# pytest-neon

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
def test_user_creation(neon_branch):
    # DATABASE_URL is automatically set to the test branch
    import psycopg  # Your own install

    with psycopg.connect() as conn:  # Uses DATABASE_URL by default
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (email) VALUES ('test@example.com')")
        conn.commit()
```

3. Run tests:

```bash
pytest
```

## Fixtures

### `neon_branch` (default, recommended)

The primary fixture for database testing. Creates one branch per test module, then resets it to the parent branch's state after each test. This provides test isolation with ~0.5s overhead per test.

Returns a `NeonBranch` dataclass with:

- `branch_id`: The Neon branch ID
- `project_id`: The Neon project ID
- `connection_string`: Full PostgreSQL connection URI
- `host`: The database host

```python
import os

def test_branch_info(neon_branch):
    # DATABASE_URL is set automatically
    assert os.environ["DATABASE_URL"] == neon_branch.connection_string

    # Use with any driver
    import psycopg
    conn = psycopg.connect(neon_branch.connection_string)
```

**Performance**: ~1.5s initial setup per module + ~0.5s reset per test. For a module with 10 tests, expect ~6.5s total overhead.

### `neon_branch_shared` (fastest, no isolation)

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

1. Before each test module, the plugin creates a new Neon branch from your parent branch
2. `DATABASE_URL` is set to point to the new branch
3. Tests run against this isolated branch with full access to your schema and data
4. After each test, the branch is reset to its parent state (~0.5s)
5. After all tests in the module complete, the branch is deleted
6. As a safety net, branches auto-expire after 10 minutes even if cleanup fails

Branches use copy-on-write storage, so you only pay for data that differs from the parent branch.

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

Or use the core `neon_branch` fixture with your own driver:

```python
def test_example(neon_branch):
    import my_preferred_driver
    conn = my_preferred_driver.connect(neon_branch.connection_string)
```

### "Neon API key not configured"

Set the `NEON_API_KEY` environment variable or use the `--neon-api-key` CLI option.

### "Neon project ID not configured"

Set the `NEON_PROJECT_ID` environment variable or use the `--neon-project-id` CLI option.

## License

MIT
