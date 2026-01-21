# pytest-neon

Pytest plugin for [Neon](https://neon.tech) database branch isolation in tests.

Each test module gets its own isolated Neon database branch, created instantly via Neon's branching feature. Branches are automatically cleaned up after tests complete (with a 10-minute auto-expiry safety net for interrupted runs).

## Features

- **Isolated test environments**: Each test module runs against its own database branch
- **Instant branch creation**: Branches are created in ~1 second regardless of database size
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

### `neon_branch` (core fixture)

Creates an isolated Neon branch for each test module. This is the primary fixture - it creates the branch and sets `DATABASE_URL` automatically.

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

### `neon_branch_isolated` (function-scoped)

Creates a fresh branch for each individual test function, providing complete isolation between tests.

```python
def test_isolated_operation(neon_branch_isolated):
    # Each test gets its own fresh branch
    # Changes here won't affect other tests
    conn = psycopg.connect(neon_branch_isolated.connection_string)
```

Use this when tests modify database state and you need guaranteed isolation. Note that creating a branch per test is slower than sharing a module-scoped branch.

### `neon_branch_reset` (reset after each test)

Creates one branch per module but resets it to the parent branch's state after each test. This provides test isolation while being faster than creating a new branch per test.

```python
def test_with_reset(neon_branch_reset):
    # Make changes - they'll be reset after this test
    conn = psycopg.connect(neon_branch_reset.connection_string)
```

Use this when you want isolation between tests but faster execution than `neon_branch_isolated`.

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
4. After tests complete, the branch is deleted (unless `--neon-keep-branches` is set)
5. As a safety net, branches auto-expire after 10 minutes even if cleanup fails

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
