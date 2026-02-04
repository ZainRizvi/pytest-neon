"""Integration tests that run against real Neon API.

These tests are skipped unless NEON_API_KEY is available.
Project ID can be set via NEON_PROJECT_ID env var or .neon file.
"""

import json
import os
from pathlib import Path

import pytest


def get_project_id():
    """Get project ID from env var or .neon file."""
    # First check env var
    project_id = os.environ.get("NEON_PROJECT_ID")
    if project_id:
        return project_id

    # Fall back to .neon file in project root
    neon_file = Path(__file__).parent.parent / ".neon"
    if neon_file.exists():
        try:
            data = json.loads(neon_file.read_text())
            return data.get("projectId")
        except (json.JSONDecodeError, KeyError):
            pass

    return None


def get_api_key():
    """Get API key from env var or .env file."""
    # First check env var
    api_key = os.environ.get("NEON_API_KEY")
    if api_key:
        return api_key

    # Fall back to .env file in project root
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("NEON_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")

    return None


# Load credentials
API_KEY = get_api_key()
PROJECT_ID = get_project_id()

# Set env vars if found (so the actual fixture can use them)
if API_KEY and "NEON_API_KEY" not in os.environ:
    os.environ["NEON_API_KEY"] = API_KEY
if PROJECT_ID and "NEON_PROJECT_ID" not in os.environ:
    os.environ["NEON_PROJECT_ID"] = PROJECT_ID

# Skip all tests in this module if credentials not available
pytestmark = pytest.mark.skipif(
    not API_KEY or not PROJECT_ID,
    reason="NEON_API_KEY and NEON_PROJECT_ID required for integration tests. "
    "Set NEON_API_KEY in .env file and ensure .neon file has projectId.",
)


class TestRealBranchCreation:
    """Test actual branch creation against Neon API."""

    def test_branch_is_created_and_accessible(self, neon_branch):
        """Test that a real branch is created and has valid connection info."""
        assert neon_branch.branch_id.startswith("br-")
        assert neon_branch.project_id == PROJECT_ID
        assert "neon.tech" in neon_branch.host
        assert neon_branch.connection_string.startswith("postgresql://")

    def test_database_url_is_set(self, neon_branch):
        """Test that DATABASE_URL environment variable is set."""
        assert os.environ.get("DATABASE_URL") == neon_branch.connection_string


class TestMigrations:
    """Test migration support with real Neon branches."""

    def test_migrations_run_and_persist(self, pytester, tmp_path):
        """Test that migrations run once and persist for all tests."""
        # Write a conftest that tracks migration and verifies table exists
        conftest = """
import os
import pytest

# Track if migrations ran
migrations_ran = [False]

@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_test_branch):
    \"\"\"Create a test table via migration.\"\"\"
    try:
        import psycopg
    except ImportError:
        pytest.skip("psycopg not installed")

    branch, is_creator = _neon_test_branch
    with psycopg.connect(branch.connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(\"\"\"
                CREATE TABLE IF NOT EXISTS migration_test (
                    id SERIAL PRIMARY KEY,
                    value TEXT NOT NULL
                )
            \"\"\")
        conn.commit()
    migrations_ran[0] = True

def pytest_sessionfinish(session, exitstatus):
    assert migrations_ran[0], "Migrations should have run"
"""
        pytester.makeconftest(conftest)

        # Write tests that verify the migrated table exists
        pytester.makepyfile(
            """
import psycopg

def test_first_insert(neon_branch):
    \"\"\"Insert data - table should exist from migration.\"\"\"
    with psycopg.connect(neon_branch.connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO migration_test (value) VALUES ('first')")
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM migration_test")
            count = cur.fetchone()[0]
            assert count >= 1  # At least our insert

def test_second_insert_sees_first(neon_branch):
    \"\"\"Second test sees data from first test (shared state).\"\"\"
    with psycopg.connect(neon_branch.connection_string) as conn:
        # Table should exist and have data from first test
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM migration_test")
            count = cur.fetchone()[0]
            # Data from first test should still be there (no reset)
            assert count >= 1

        # Insert new data
        with conn.cursor() as cur:
            cur.execute("INSERT INTO migration_test (value) VALUES ('second')")
        conn.commit()
"""
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)


class TestRealDatabaseConnectivity:
    """Test actual database connectivity."""

    def test_can_connect_and_query(self, neon_branch):
        """Test that we can actually connect to the created branch."""
        try:
            import psycopg
        except ImportError:
            pytest.skip("psycopg not installed - run: pip install pytest-neon[psycopg]")

        with (
            psycopg.connect(neon_branch.connection_string) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SELECT 1 AS result")
            result = cur.fetchone()
            assert result[0] == 1

    def test_can_create_and_query_table(self, neon_branch):
        """Test that we can create tables and insert data."""
        try:
            import psycopg
        except ImportError:
            pytest.skip("psycopg not installed - run: pip install pytest-neon[psycopg]")

        with (
            psycopg.connect(neon_branch.connection_string) as conn,
            conn.cursor() as cur,
        ):
            # Create a test table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pytest_neon_test (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL
                )
            """)
            # Insert data
            cur.execute(
                "INSERT INTO pytest_neon_test (name) VALUES (%s) RETURNING id",
                ("test_value",),
            )
            inserted_id = cur.fetchone()[0]
            conn.commit()

            # Query it back
            cur.execute(
                "SELECT name FROM pytest_neon_test WHERE id = %s", (inserted_id,)
            )
            result = cur.fetchone()
            assert result[0] == "test_value"


class TestSQLAlchemyConnections:
    """Test SQLAlchemy connection behavior with shared branch.

    Since the simplified plugin doesn't reset branches between tests,
    pool_pre_ping is no longer strictly required for stale connections.
    However, it's still recommended for cloud databases in general.
    """

    def test_sqlalchemy_works_with_shared_branch(self, pytester):
        """
        Verify SQLAlchemy works correctly with shared branch.
        """
        pytester.makepyfile(
            database="""
import os
from sqlalchemy import create_engine

DATABASE_URL = os.environ.get("DATABASE_URL")
# pool_pre_ping=True is recommended for any cloud DB
engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None
"""
        )

        pytester.makepyfile(
            test_sqlalchemy="""
from sqlalchemy import text

def test_first_query(neon_branch):
    '''First test - creates pooled connection.'''
    from database import engine
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
    # Connection returned to pool

def test_second_query(neon_branch):
    '''Second test - no reset between tests, connection still valid.'''
    from database import engine
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 2"))
        assert result.scalar() == 2

def test_third_query(neon_branch):
    '''Third test - still works.'''
    from database import engine
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 3"))
        assert result.scalar() == 3
"""
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=3)
