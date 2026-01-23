"""Pytest plugin providing Neon database branch fixtures.

This plugin provides fixtures for isolated database testing using Neon's
instant branching feature. Each test gets a clean database state via
branch reset after each test.

Main fixtures:
    neon_branch: Primary fixture - one branch per session, reset after each test
    neon_branch_shared: Shared branch without reset (fastest, no isolation)
    neon_connection: psycopg2 connection (requires psycopg2 extra)
    neon_connection_psycopg: psycopg v3 connection (requires psycopg extra)
    neon_engine: SQLAlchemy engine (requires sqlalchemy extra)

SQLAlchemy Users:
    If you create your own SQLAlchemy engine (not using neon_engine fixture),
    you MUST use pool_pre_ping=True:

        engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    This is required because branch resets terminate server-side connections.
    Without pool_pre_ping, SQLAlchemy may try to reuse dead pooled connections,
    causing "SSL connection has been closed unexpectedly" errors.

Configuration:
    Set NEON_API_KEY and NEON_PROJECT_ID environment variables, or use
    --neon-api-key and --neon-project-id CLI options.

For full documentation, see: https://github.com/ZainRizvi/pytest-neon
"""

from __future__ import annotations

import os
import time
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import requests
from neon_api import NeonAPI
from neon_api.schema import EndpointState

# Default branch expiry in seconds (10 minutes)
DEFAULT_BRANCH_EXPIRY_SECONDS = 600


@dataclass
class NeonBranch:
    """Information about a Neon test branch."""

    branch_id: str
    project_id: str
    connection_string: str
    host: str
    parent_id: str | None = None


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add Neon-specific command line options and ini settings."""
    group = parser.getgroup("neon", "Neon database branching")

    # CLI options
    group.addoption(
        "--neon-api-key",
        dest="neon_api_key",
        help="Neon API key (default: NEON_API_KEY env var)",
    )
    group.addoption(
        "--neon-project-id",
        dest="neon_project_id",
        help="Neon project ID (default: NEON_PROJECT_ID env var)",
    )
    group.addoption(
        "--neon-parent-branch",
        dest="neon_parent_branch",
        help="Parent branch ID to create test branches from (default: project default)",
    )
    group.addoption(
        "--neon-database",
        dest="neon_database",
        help="Database name (default: neondb)",
    )
    group.addoption(
        "--neon-role",
        dest="neon_role",
        help="Database role (default: neondb_owner)",
    )
    group.addoption(
        "--neon-keep-branches",
        action="store_true",
        dest="neon_keep_branches",
        help="Don't delete branches after tests (useful for debugging)",
    )
    group.addoption(
        "--neon-branch-expiry",
        dest="neon_branch_expiry",
        type=int,
        help=(
            f"Branch auto-expiry in seconds "
            f"(default: {DEFAULT_BRANCH_EXPIRY_SECONDS}). Set to 0 to disable."
        ),
    )
    group.addoption(
        "--neon-env-var",
        dest="neon_env_var",
        help="Environment variable to set with connection string (default: DATABASE_URL)",  # noqa: E501
    )

    # INI file settings (pytest.ini, pyproject.toml, etc.)
    parser.addini("neon_api_key", "Neon API key", default=None)
    parser.addini("neon_project_id", "Neon project ID", default=None)
    parser.addini("neon_parent_branch", "Parent branch ID", default=None)
    parser.addini("neon_database", "Database name", default="neondb")
    parser.addini("neon_role", "Database role", default="neondb_owner")
    parser.addini(
        "neon_keep_branches",
        "Don't delete branches after tests",
        type="bool",
        default=False,
    )
    parser.addini(
        "neon_branch_expiry",
        "Branch auto-expiry in seconds",
        default=str(DEFAULT_BRANCH_EXPIRY_SECONDS),
    )
    parser.addini(
        "neon_env_var",
        "Environment variable for connection string",
        default="DATABASE_URL",
    )


def _get_config_value(
    config: pytest.Config,
    option: str,
    env_var: str,
    ini_name: str | None = None,
    default: str | None = None,
) -> str | None:
    """Get config value from CLI option, env var, ini setting, or default.

    Priority order: CLI option > environment variable > ini setting > default
    """
    # 1. CLI option (highest priority)
    value = config.getoption(option, default=None)
    if value is not None:
        return value

    # 2. Environment variable
    env_value = os.environ.get(env_var)
    if env_value is not None:
        return env_value

    # 3. INI setting (pytest.ini, pyproject.toml, etc.)
    if ini_name is not None:
        ini_value = config.getini(ini_name)
        if ini_value:
            return ini_value

    # 4. Default
    return default


def _create_neon_branch(
    request: pytest.FixtureRequest,
    parent_branch_id_override: str | None = None,
    branch_expiry_override: int | None = None,
    branch_name_suffix: str = "",
) -> Generator[NeonBranch, None, None]:
    """
    Internal helper that creates and manages a Neon branch lifecycle.

    This is the core implementation used by branch fixtures.

    Args:
        request: Pytest fixture request
        parent_branch_id_override: If provided, use this as parent instead of config
        branch_expiry_override: If provided, use this expiry instead of config
        branch_name_suffix: Optional suffix for branch name (e.g., "-migrated", "-test")
    """
    config = request.config

    api_key = _get_config_value(config, "neon_api_key", "NEON_API_KEY", "neon_api_key")
    project_id = _get_config_value(
        config, "neon_project_id", "NEON_PROJECT_ID", "neon_project_id"
    )
    # Use override if provided, otherwise read from config
    parent_branch_id = parent_branch_id_override or _get_config_value(
        config, "neon_parent_branch", "NEON_PARENT_BRANCH_ID", "neon_parent_branch"
    )
    database_name = _get_config_value(
        config, "neon_database", "NEON_DATABASE", "neon_database", "neondb"
    )
    role_name = _get_config_value(
        config, "neon_role", "NEON_ROLE", "neon_role", "neondb_owner"
    )

    # For boolean/int options, check CLI first, then ini
    keep_branches = config.getoption("neon_keep_branches", default=None)
    if keep_branches is None:
        keep_branches = config.getini("neon_keep_branches")

    # Use override if provided, otherwise read from config
    if branch_expiry_override is not None:
        branch_expiry = branch_expiry_override
    else:
        branch_expiry = config.getoption("neon_branch_expiry", default=None)
        if branch_expiry is None:
            branch_expiry = int(config.getini("neon_branch_expiry"))

    env_var_name = _get_config_value(
        config, "neon_env_var", "", "neon_env_var", "DATABASE_URL"
    )

    if not api_key:
        pytest.skip(
            "Neon API key not configured (set NEON_API_KEY or use --neon-api-key)"
        )
    if not project_id:
        pytest.skip(
            "Neon project ID not configured "
            "(set NEON_PROJECT_ID or use --neon-project-id)"
        )

    neon = NeonAPI(api_key=api_key)

    # Generate unique branch name
    branch_name = f"pytest-{os.urandom(4).hex()}{branch_name_suffix}"

    # Build branch creation payload
    branch_config: dict[str, Any] = {"name": branch_name}
    if parent_branch_id:
        branch_config["parent_id"] = parent_branch_id

    # Set branch expiration (auto-delete) as a safety net for interrupted test runs
    # This uses the branch expires_at field, not endpoint suspend_timeout
    if branch_expiry and branch_expiry > 0:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=branch_expiry)
        branch_config["expires_at"] = expires_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Create branch with compute endpoint
    result = neon.branch_create(
        project_id=project_id,
        branch=branch_config,
        endpoints=[{"type": "read_write"}],
    )

    branch = result.branch

    # Get endpoint_id from operations
    # (branch_create returns operations, not endpoints directly)
    endpoint_id = None
    for op in result.operations:
        if op.endpoint_id:
            endpoint_id = op.endpoint_id
            break

    if not endpoint_id:
        raise RuntimeError(f"No endpoint created for branch {branch.id}")

    # Wait for endpoint to be ready (it starts in "init" state)
    # Endpoints typically become active in 1-2 seconds, but we allow up to 60s
    # to handle occasional Neon API slowness or high load scenarios
    max_wait_seconds = 60
    poll_interval = 0.5  # Poll every 500ms for responsive feedback
    waited = 0.0

    while True:
        endpoint_response = neon.endpoint(
            project_id=project_id, endpoint_id=endpoint_id
        )
        endpoint = endpoint_response.endpoint
        state = endpoint.current_state

        if state == EndpointState.active:
            break

        if waited >= max_wait_seconds:
            raise RuntimeError(
                f"Timeout waiting for endpoint {endpoint_id} to become active "
                f"(current state: {state})"
            )

        time.sleep(poll_interval)
        waited += poll_interval

    host = endpoint.host

    # Reset password to get the password value
    # (newly created branches don't expose password)
    password_response = neon.role_password_reset(
        project_id=project_id,
        branch_id=branch.id,
        role_name=role_name,
    )
    password = password_response.role.password

    # Build connection string
    connection_string = (
        f"postgresql://{role_name}:{password}@{host}/{database_name}?sslmode=require"
    )

    neon_branch_info = NeonBranch(
        branch_id=branch.id,
        project_id=project_id,
        connection_string=connection_string,
        host=host,
        parent_id=branch.parent_id,
    )

    # Set DATABASE_URL (or configured env var) for the duration of the fixture scope
    original_env_value = os.environ.get(env_var_name)
    os.environ[env_var_name] = connection_string

    try:
        yield neon_branch_info
    finally:
        # Restore original env var
        if original_env_value is None:
            os.environ.pop(env_var_name, None)
        else:
            os.environ[env_var_name] = original_env_value

        # Cleanup: delete branch unless --neon-keep-branches was specified
        if not keep_branches:
            try:
                neon.branch_delete(project_id=project_id, branch_id=branch.id)
            except Exception as e:
                # Log but don't fail tests due to cleanup issues
                import warnings

                warnings.warn(
                    f"Failed to delete Neon branch {branch.id}: {e}",
                    stacklevel=2,
                )


def _reset_branch_to_parent(branch: NeonBranch, api_key: str) -> None:
    """Reset a branch to its parent's state using the Neon API."""
    if not branch.parent_id:
        raise RuntimeError(f"Branch {branch.branch_id} has no parent - cannot reset")

    url = f"https://console.neon.tech/api/v2/projects/{branch.project_id}/branches/{branch.branch_id}/restore"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    response = requests.post(
        url, headers=headers, json={"source_branch_id": branch.parent_id}, timeout=30
    )
    response.raise_for_status()


@pytest.fixture(scope="session")
def _neon_migration_branch(
    request: pytest.FixtureRequest,
) -> Generator[NeonBranch, None, None]:
    """
    Session-scoped branch where migrations are applied.

    This branch is created from the configured parent and serves as
    the parent for all test branches. Migrations run once per session
    on this branch.

    Note: The migration branch cannot have an expiry because Neon doesn't
    allow creating child branches from branches with expiration dates.
    Cleanup relies on the fixture teardown at session end.
    """
    # No expiry - Neon doesn't allow children from branches with expiry
    yield from _create_neon_branch(
        request,
        branch_expiry_override=0,
        branch_name_suffix="-migrated",
    )


@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_migration_branch: NeonBranch) -> None:
    """
    Override this fixture to run migrations on the test database.

    The migration branch is already created and DATABASE_URL is set.
    Migrations run once per test session, before any tests execute.

    Example in conftest.py:

        @pytest.fixture(scope="session")
        def neon_apply_migrations(_neon_migration_branch):
            import subprocess
            subprocess.run(["alembic", "upgrade", "head"], check=True)

    Or with Django:

        @pytest.fixture(scope="session")
        def neon_apply_migrations(_neon_migration_branch):
            from django.core.management import call_command
            call_command("migrate", "--noinput")

    Or with raw SQL:

        @pytest.fixture(scope="session")
        def neon_apply_migrations(_neon_migration_branch):
            import psycopg
            with psycopg.connect(_neon_migration_branch.connection_string) as conn:
                with open("schema.sql") as f:
                    conn.execute(f.read())
                conn.commit()

    Args:
        _neon_migration_branch: The migration branch with connection details.
            Use _neon_migration_branch.connection_string to connect directly,
            or rely on DATABASE_URL which is already set.
    """
    pass  # No-op by default - users override this fixture to run migrations


@pytest.fixture(scope="session")
def _neon_branch_for_reset(
    request: pytest.FixtureRequest,
    _neon_migration_branch: NeonBranch,
    neon_apply_migrations: None,  # Ensures migrations run first
) -> Generator[NeonBranch, None, None]:
    """
    Internal fixture that creates a test branch from the migration branch.

    This is session-scoped so DATABASE_URL remains stable throughout the test
    session, avoiding issues with Python's module caching (e.g., SQLAlchemy
    engines created at import time would otherwise point to stale branches).

    The test branch is created as a child of the migration branch, so resets
    restore to post-migration state rather than the original parent state.
    """
    yield from _create_neon_branch(
        request,
        parent_branch_id_override=_neon_migration_branch.branch_id,
        branch_name_suffix="-test",
    )


@pytest.fixture(scope="function")
def neon_branch(
    request: pytest.FixtureRequest,
    _neon_branch_for_reset: NeonBranch,
) -> Generator[NeonBranch, None, None]:
    """
    Provide an isolated Neon database branch for each test.

    This is the primary fixture for database testing. It creates one branch per
    test session, then resets it to the parent branch's state after each test.
    This provides test isolation with ~0.5s overhead per test.

    The branch is automatically deleted after all tests complete, unless
    --neon-keep-branches is specified. Branches also auto-expire after
    10 minutes by default (configurable via --neon-branch-expiry) as a safety net
    for interrupted test runs.

    The connection string is automatically set in the DATABASE_URL environment
    variable (configurable via --neon-env-var).

    SQLAlchemy Users:
        If you create your own engine (not using the neon_engine fixture),
        you MUST use pool_pre_ping=True::

            engine = create_engine(DATABASE_URL, pool_pre_ping=True)

        Branch resets terminate server-side connections. Without pool_pre_ping,
        SQLAlchemy may reuse dead pooled connections, causing SSL errors.

    Requires either:
        - NEON_API_KEY and NEON_PROJECT_ID environment variables, or
        - --neon-api-key and --neon-project-id command line options

    Yields:
        NeonBranch: Object with branch_id, project_id, connection_string, and host.

    Example::

        def test_database_operation(neon_branch):
            # DATABASE_URL is automatically set
            conn_string = os.environ["DATABASE_URL"]
            # or use directly
            conn_string = neon_branch.connection_string
    """
    config = request.config
    api_key = _get_config_value(config, "neon_api_key", "NEON_API_KEY", "neon_api_key")

    # Validate that branch has a parent for reset functionality
    if not _neon_branch_for_reset.parent_id:
        pytest.fail(
            f"\n\nBranch {_neon_branch_for_reset.branch_id} has no parent. "
            f"The neon_branch fixture requires a parent branch for reset.\n\n"
            f"Use neon_branch_shared if you don't need reset, or specify "
            f"a parent branch with --neon-parent-branch or NEON_PARENT_BRANCH_ID."
        )

    yield _neon_branch_for_reset

    # Reset branch to parent state after each test
    if api_key:
        try:
            _reset_branch_to_parent(branch=_neon_branch_for_reset, api_key=api_key)
        except Exception as e:
            pytest.fail(
                f"\n\nFailed to reset branch {_neon_branch_for_reset.branch_id} "
                f"after test. Subsequent tests in this module may see dirty "
                f"database state.\n\nError: {e}\n\n"
                f"To keep the branch for debugging, use --neon-keep-branches"
            )


@pytest.fixture(scope="module")
def neon_branch_shared(
    request: pytest.FixtureRequest,
    _neon_migration_branch: NeonBranch,
    neon_apply_migrations: None,  # Ensures migrations run first
) -> Generator[NeonBranch, None, None]:
    """
    Provide a shared Neon database branch for all tests in a module.

    This fixture creates one branch per test module and shares it across all
    tests without resetting. This is the fastest option but tests can see
    each other's data modifications.

    If you override the `neon_apply_migrations` fixture, migrations will run
    once before the first test, and this branch will include the migrated schema.

    Use this when:
    - Tests are read-only or don't interfere with each other
    - You manually clean up test data within each test
    - Maximum speed is more important than isolation

    Warning: Tests in the same module will share database state. Data created
    by one test will be visible to subsequent tests. Use `neon_branch` instead
    if you need isolation between tests.

    Yields:
        NeonBranch: Object with branch_id, project_id, connection_string, and host.

    Example:
        def test_read_only_query(neon_branch_shared):
            # Fast: no reset between tests, but be careful about data leakage
            conn_string = neon_branch_shared.connection_string
    """
    yield from _create_neon_branch(
        request,
        parent_branch_id_override=_neon_migration_branch.branch_id,
        branch_name_suffix="-shared",
    )


@pytest.fixture
def neon_connection(neon_branch: NeonBranch):
    """
    Provide a psycopg2 connection to the test branch.

    Requires the psycopg2 optional dependency:
        pip install pytest-neon[psycopg2]

    The connection is rolled back and closed after each test.

    Yields:
        psycopg2 connection object

    Example:
        def test_insert(neon_connection):
            cur = neon_connection.cursor()
            cur.execute("INSERT INTO users (name) VALUES ('test')")
            neon_connection.commit()
    """
    try:
        import psycopg2
    except ImportError:
        pytest.fail(
            "\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
            "  MISSING DEPENDENCY: psycopg2\n"
            "═══════════════════════════════════════════════════════════════════\n\n"
            "  The 'neon_connection' fixture requires psycopg2.\n\n"
            "  To fix this, install the psycopg2 extra:\n\n"
            "      pip install pytest-neon[psycopg2]\n\n"
            "  Or use the 'neon_branch' fixture with your own database driver:\n\n"
            "      def test_example(neon_branch):\n"
            "          import your_driver\n"
            "          conn = your_driver.connect(neon_branch.connection_string)\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
        )

    conn = psycopg2.connect(neon_branch.connection_string)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def neon_connection_psycopg(neon_branch: NeonBranch):
    """
    Provide a psycopg (v3) connection to the test branch.

    Requires the psycopg optional dependency:
        pip install pytest-neon[psycopg]

    The connection is rolled back and closed after each test.

    Yields:
        psycopg connection object

    Example:
        def test_insert(neon_connection_psycopg):
            with neon_connection_psycopg.cursor() as cur:
                cur.execute("INSERT INTO users (name) VALUES ('test')")
            neon_connection_psycopg.commit()
    """
    try:
        import psycopg
    except ImportError:
        pytest.fail(
            "\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
            "  MISSING DEPENDENCY: psycopg (v3)\n"
            "═══════════════════════════════════════════════════════════════════\n\n"
            "  The 'neon_connection_psycopg' fixture requires psycopg v3.\n\n"
            "  To fix this, install the psycopg extra:\n\n"
            "      pip install pytest-neon[psycopg]\n\n"
            "  Or use the 'neon_branch' fixture with your own database driver:\n\n"
            "      def test_example(neon_branch):\n"
            "          import your_driver\n"
            "          conn = your_driver.connect(neon_branch.connection_string)\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
        )

    conn = psycopg.connect(neon_branch.connection_string)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def neon_engine(neon_branch: NeonBranch):
    """
    Provide a SQLAlchemy engine connected to the test branch.

    Requires the sqlalchemy optional dependency:
        pip install pytest-neon[sqlalchemy]

    The engine is disposed after each test, which handles stale connections
    after branch resets automatically.

    Note:
        If you create your own module-level engine instead of using this
        fixture, you MUST use pool_pre_ping=True::

            engine = create_engine(DATABASE_URL, pool_pre_ping=True)

        This is required because branch resets terminate server-side
        connections, and without pool_pre_ping SQLAlchemy may reuse dead
        pooled connections.

    Yields:
        SQLAlchemy Engine object

    Example::

        def test_query(neon_engine):
            with neon_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
    """
    try:
        from sqlalchemy import create_engine
    except ImportError:
        pytest.fail(
            "\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
            "  MISSING DEPENDENCY: SQLAlchemy\n"
            "═══════════════════════════════════════════════════════════════════\n\n"
            "  The 'neon_engine' fixture requires SQLAlchemy.\n\n"
            "  To fix this, install the sqlalchemy extra:\n\n"
            "      pip install pytest-neon[sqlalchemy]\n\n"
            "  Or use the 'neon_branch' fixture with your own database driver:\n\n"
            "      def test_example(neon_branch):\n"
            "          from sqlalchemy import create_engine\n"
            "          engine = create_engine(neon_branch.connection_string)\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
        )

    engine = create_engine(neon_branch.connection_string)
    yield engine
    engine.dispose()
