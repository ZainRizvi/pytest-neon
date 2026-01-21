"""Pytest plugin providing Neon database branch fixtures."""

from __future__ import annotations

import os
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest
from neon_api import NeonAPI

if TYPE_CHECKING:
    pass

# Default branch expiry in seconds (10 minutes)
DEFAULT_BRANCH_EXPIRY_SECONDS = 600


@dataclass
class NeonBranch:
    """Information about a Neon test branch."""

    branch_id: str
    project_id: str
    connection_string: str
    host: str


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add Neon-specific command line options."""
    group = parser.getgroup("neon", "Neon database branching")

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
        default="neondb",
        help="Database name (default: neondb)",
    )
    group.addoption(
        "--neon-role",
        dest="neon_role",
        default="neondb_owner",
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
        default=DEFAULT_BRANCH_EXPIRY_SECONDS,
        help=(
            f"Branch auto-expiry in seconds "
            f"(default: {DEFAULT_BRANCH_EXPIRY_SECONDS}). Set to 0 to disable."
        ),
    )
    group.addoption(
        "--neon-env-var",
        dest="neon_env_var",
        default="DATABASE_URL",
        help="Environment variable to set with connection string (default: DATABASE_URL)",  # noqa: E501
    )


def _get_config_value(
    config: pytest.Config, option: str, env_var: str, default: str | None = None
) -> str | None:
    """Get config value from CLI option, env var, or default."""
    value = config.getoption(option, default=None)
    if value is not None:
        return value
    return os.environ.get(env_var, default)


@pytest.fixture(scope="module")
def neon_branch(request: pytest.FixtureRequest) -> Generator[NeonBranch, None, None]:
    """
    Create an isolated Neon database branch for each test module.

    The branch is automatically deleted after all tests in the module complete,
    unless --neon-keep-branches is specified. Branches also auto-expire after
    10 minutes by default (configurable via --neon-branch-expiry) as a safety net
    for interrupted test runs.

    The connection string is automatically set in the DATABASE_URL environment
    variable (configurable via --neon-env-var) for the duration of the test module.

    Requires either:
    - NEON_API_KEY and NEON_PROJECT_ID environment variables, or
    - --neon-api-key and --neon-project-id command line options

    Yields:
        NeonBranch: Object with branch_id, project_id, connection_string, and host.

    Example:
        def test_database_operation(neon_branch):
            # DATABASE_URL is automatically set
            conn_string = os.environ["DATABASE_URL"]
            # or use directly
            conn_string = neon_branch.connection_string
    """
    config = request.config

    api_key = _get_config_value(config, "neon_api_key", "NEON_API_KEY")
    project_id = _get_config_value(config, "neon_project_id", "NEON_PROJECT_ID")
    parent_branch_id = _get_config_value(
        config, "neon_parent_branch", "NEON_PARENT_BRANCH_ID"
    )
    database_name = _get_config_value(
        config, "neon_database", "NEON_DATABASE", "neondb"
    )
    role_name = _get_config_value(config, "neon_role", "NEON_ROLE", "neondb_owner")
    keep_branches = config.getoption("neon_keep_branches", default=False)
    branch_expiry = config.getoption(
        "neon_branch_expiry", default=DEFAULT_BRANCH_EXPIRY_SECONDS
    )
    env_var_name = config.getoption("neon_env_var", default="DATABASE_URL")

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
    branch_name = f"pytest-{os.urandom(4).hex()}"

    # Build branch creation payload
    branch_config: dict[str, Any] = {"name": branch_name}
    if parent_branch_id:
        branch_config["parent_id"] = parent_branch_id

    # Build endpoint config with optional expiry
    endpoint_config: dict[str, Any] = {"type": "read_write"}
    if branch_expiry and branch_expiry > 0:
        endpoint_config["suspend_timeout_seconds"] = branch_expiry

    # Create branch with compute endpoint
    result = neon.branch_create(
        project_id=project_id,
        branch=branch_config,
        endpoints=[endpoint_config],
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
    # Endpoints typically become active in 1-2 seconds
    max_wait_seconds = 60
    poll_interval = 0.5
    waited = 0.0

    while True:
        endpoint_response = neon.endpoint(
            project_id=project_id, endpoint_id=endpoint_id
        )
        endpoint = endpoint_response.endpoint
        state = endpoint.current_state

        if state == "active" or str(state) == "EndpointState.active":
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
    )

    # Set DATABASE_URL (or configured env var) for the duration of the test module
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

    The engine is disposed after each test.

    Yields:
        SQLAlchemy Engine object

    Example:
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
