"""Pytest plugin providing Neon database branch fixtures.

This plugin provides a simple fixture for database testing using Neon's instant
branching feature. All tests share a single branch per session.

Main fixture:
    neon_branch: Session-scoped shared branch for all tests

Connection fixtures (require extras):
    neon_connection: psycopg2 connection (requires psycopg2 extra)
    neon_connection_psycopg: psycopg v3 connection (requires psycopg extra)
    neon_engine: SQLAlchemy engine (requires sqlalchemy extra)

Architecture:
    Parent Branch (configured or project default)
        └── Test Branch (session-scoped, 10-min expiry)
                ↑ migrations run here ONCE, all tests share this

Configuration:
    Set NEON_API_KEY and NEON_PROJECT_ID environment variables, or use
    --neon-api-key and --neon-project-id CLI options.

Test Isolation:
    Since all tests share the same branch, tests that modify data will see
    each other's changes. For test isolation, use one of these patterns:

    1. Transaction rollback:
        @pytest.fixture
        def db_transaction(neon_branch):
            import psycopg
            conn = psycopg.connect(neon_branch.connection_string)
            conn.execute("BEGIN")
            yield conn
            conn.execute("ROLLBACK")
            conn.close()

    2. Table truncation:
        @pytest.fixture(autouse=True)
        def clean_tables(neon_branch):
            yield
            with psycopg.connect(neon_branch.connection_string) as conn:
                conn.execute("TRUNCATE users, orders CASCADE")
                conn.commit()

For full documentation, see: https://github.com/ZainRizvi/pytest-neon
"""

from __future__ import annotations

import contextlib
import json
import os
import random
import time
import warnings
from collections.abc import Callable, Generator
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, TypeVar

import pytest
import requests
from filelock import FileLock
from neon_api import NeonAPI
from neon_api.exceptions import NeonAPIError
from neon_api.schema import EndpointState

T = TypeVar("T")

# Default branch expiry in seconds (10 minutes)
DEFAULT_BRANCH_EXPIRY_SECONDS = 600

# Rate limit retry configuration
# See: https://api-docs.neon.tech/reference/api-rate-limiting
# Neon limits: 700 requests/minute (~11/sec), burst up to 40/sec per route
_RATE_LIMIT_BASE_DELAY = 4.0  # seconds
_RATE_LIMIT_MAX_TOTAL_DELAY = 90.0  # 1.5 minutes total cap
_RATE_LIMIT_JITTER_FACTOR = 0.25  # +/- 25% jitter
_RATE_LIMIT_MAX_ATTEMPTS = 10  # Maximum number of retry attempts


class NeonRateLimitError(Exception):
    """Raised when Neon API rate limit is exceeded and retries are exhausted."""

    pass


def _calculate_retry_delay(
    attempt: int,
    base_delay: float = _RATE_LIMIT_BASE_DELAY,
    jitter_factor: float = _RATE_LIMIT_JITTER_FACTOR,
) -> float:
    """
    Calculate delay for a retry attempt with exponential backoff and jitter.

    Args:
        attempt: The retry attempt number (0-indexed)
        base_delay: Base delay in seconds
        jitter_factor: Jitter factor (0.25 means +/- 25%)

    Returns:
        Delay in seconds with jitter applied
    """
    # Exponential backoff: base_delay * 2^attempt
    delay = base_delay * (2**attempt)

    # Apply jitter: delay * (1 +/- jitter_factor)
    jitter = delay * jitter_factor * (2 * random.random() - 1)
    return delay + jitter


def _is_rate_limit_error(exc: Exception) -> bool:
    """
    Check if an exception indicates a rate limit (429) error.

    Handles both requests.HTTPError (with response object) and NeonAPIError
    (which only has the error text, not the response object).

    Args:
        exc: The exception to check

    Returns:
        True if this is a rate limit error, False otherwise
    """
    # Check NeonAPIError first - it inherits from HTTPError but doesn't have
    # a response object, so we need to check the error text
    if isinstance(exc, NeonAPIError):
        # NeonAPIError doesn't preserve the response object, only the text
        # Check for rate limit indicators in the error message
        # Note: We use "too many requests" specifically to avoid false positives
        # from errors like "too many connections" or "too many rows"
        error_text = str(exc).lower()
        return (
            "429" in error_text
            or "rate limit" in error_text
            or "too many requests" in error_text
        )
    if isinstance(exc, requests.HTTPError):
        return exc.response is not None and exc.response.status_code == 429
    return False


def _get_retry_after_from_error(exc: Exception) -> float | None:
    """
    Extract Retry-After header value from an exception if available.

    Args:
        exc: The exception to check

    Returns:
        The Retry-After value in seconds, or None if not available
    """
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
    return None


def _retry_on_rate_limit(
    operation: Callable[[], T],
    operation_name: str,
    base_delay: float = _RATE_LIMIT_BASE_DELAY,
    max_total_delay: float = _RATE_LIMIT_MAX_TOTAL_DELAY,
    jitter_factor: float = _RATE_LIMIT_JITTER_FACTOR,
    max_attempts: int = _RATE_LIMIT_MAX_ATTEMPTS,
) -> T:
    """
    Execute an operation with retry logic for rate limit (429) errors.

    Uses exponential backoff with jitter. Retries until the operation succeeds,
    the total delay exceeds max_total_delay, or max_attempts is reached.

    See: https://api-docs.neon.tech/reference/api-rate-limiting

    Args:
        operation: Callable that may raise requests.HTTPError or NeonAPIError
        operation_name: Human-readable name for error messages
        base_delay: Base delay in seconds for first retry
        max_total_delay: Maximum total delay across all retries
        jitter_factor: Jitter factor for randomization
        max_attempts: Maximum number of retry attempts

    Returns:
        The result of the operation

    Raises:
        NeonRateLimitError: If rate limit retries are exhausted
        requests.HTTPError: For non-429 HTTP errors
        NeonAPIError: For non-429 API errors
        Exception: For other errors from the operation
    """
    total_delay = 0.0
    attempt = 0

    while True:
        try:
            return operation()
        except (requests.HTTPError, NeonAPIError) as e:
            if _is_rate_limit_error(e):
                # Check for Retry-After header (may be added by Neon in future)
                retry_after = _get_retry_after_from_error(e)
                if retry_after is not None:
                    # Ensure minimum delay to prevent infinite loops if Retry-After is 0
                    delay = max(retry_after, 0.1)
                else:
                    delay = _calculate_retry_delay(attempt, base_delay, jitter_factor)

                # Check if we've exceeded max total delay
                if total_delay + delay > max_total_delay:
                    raise NeonRateLimitError(
                        f"Rate limit exceeded for {operation_name}. "
                        f"Max total delay ({max_total_delay:.1f}s) reached after "
                        f"{attempt + 1} attempts. "
                        f"See: https://api-docs.neon.tech/reference/api-rate-limiting"
                    ) from e

                # Check if we've exceeded max attempts
                attempt += 1
                if attempt >= max_attempts:
                    raise NeonRateLimitError(
                        f"Rate limit exceeded for {operation_name}. "
                        f"Max attempts ({max_attempts}) reached after "
                        f"{total_delay:.1f}s total delay. "
                        f"See: https://api-docs.neon.tech/reference/api-rate-limiting"
                    ) from e

                time.sleep(delay)
                total_delay += delay
            else:
                # Non-429 error, re-raise immediately
                raise


def _get_xdist_worker_id() -> str:
    """
    Get the pytest-xdist worker ID, or "main" if not running under xdist.

    When running tests in parallel with pytest-xdist, each worker process
    gets a unique ID (gw0, gw1, gw2, etc.). This is used to coordinate
    branch creation and migrations across workers.
    """
    return os.environ.get("PYTEST_XDIST_WORKER", "main")


def _sanitize_branch_name(name: str) -> str:
    """
    Sanitize a string for use in Neon branch names.

    Only allows alphanumeric characters, hyphens, and underscores.
    All other characters (including non-ASCII) are replaced with hyphens.
    """
    import re

    # Replace anything that's not alphanumeric, hyphen, or underscore with hyphen
    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "-", name)
    # Collapse multiple hyphens into one
    sanitized = re.sub(r"-+", "-", sanitized)
    # Remove leading/trailing hyphens
    sanitized = sanitized.strip("-")
    return sanitized


def _get_git_branch_name() -> str | None:
    """
    Get the current git branch name (sanitized), or None if not in a git repo.

    Used to include the git branch in Neon branch names, making it easier
    to identify which git branch/PR created orphaned test branches.

    The branch name is sanitized to replace special characters with hyphens.
    """
    import subprocess

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            branch = result.stdout.strip()
            return _sanitize_branch_name(branch) if branch else None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass
    return None


def _reveal_role_password(
    api_key: str, project_id: str, branch_id: str, role_name: str
) -> str:
    """
    Get the password for a role WITHOUT resetting it.

    Uses Neon's reveal_password API endpoint (GET request).

    Note: The neon-api library has a bug where it uses POST instead of GET,
    so we make the request directly.
    """
    url = (
        f"https://console.neon.tech/api/v2/projects/{project_id}"
        f"/branches/{branch_id}/roles/{role_name}/reveal_password"
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    response = requests.get(url, headers=headers, timeout=30)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        # Wrap in NeonAPIError for consistent error handling
        raise NeonAPIError(response.text) from None

    data = response.json()
    return data["password"]


@dataclass
class NeonBranch:
    """Information about a Neon test branch."""

    branch_id: str
    project_id: str
    connection_string: str
    host: str
    parent_id: str | None = None
    endpoint_id: str | None = None


@dataclass
class NeonConfig:
    """Configuration for Neon operations. Extracted from pytest config."""

    api_key: str
    project_id: str
    parent_branch_id: str | None
    database_name: str
    role_name: str
    keep_branches: bool
    branch_expiry: int
    env_var_name: str

    @classmethod
    def from_pytest_config(cls, config: pytest.Config) -> NeonConfig | None:
        """
        Extract NeonConfig from pytest configuration.

        Returns None if required values (api_key, project_id) are missing,
        allowing callers to skip tests gracefully.
        """
        api_key = _get_config_value(
            config, "neon_api_key", "NEON_API_KEY", "neon_api_key"
        )
        project_id = _get_config_value(
            config, "neon_project_id", "NEON_PROJECT_ID", "neon_project_id"
        )

        if not api_key or not project_id:
            return None

        parent_branch_id = _get_config_value(
            config, "neon_parent_branch", "NEON_PARENT_BRANCH_ID", "neon_parent_branch"
        )
        database_name = _get_config_value(
            config, "neon_database", "NEON_DATABASE", "neon_database", "neondb"
        )
        role_name = _get_config_value(
            config, "neon_role", "NEON_ROLE", "neon_role", "neondb_owner"
        )

        keep_branches = config.getoption("neon_keep_branches", default=None)
        if keep_branches is None:
            keep_branches = config.getini("neon_keep_branches")

        branch_expiry = config.getoption("neon_branch_expiry", default=None)
        if branch_expiry is None:
            branch_expiry = int(config.getini("neon_branch_expiry"))

        env_var_name = _get_config_value(
            config, "neon_env_var", "", "neon_env_var", "DATABASE_URL"
        )

        return cls(
            api_key=api_key,
            project_id=project_id,
            parent_branch_id=parent_branch_id,
            database_name=database_name or "neondb",
            role_name=role_name or "neondb_owner",
            keep_branches=bool(keep_branches),
            branch_expiry=branch_expiry or DEFAULT_BRANCH_EXPIRY_SECONDS,
            env_var_name=env_var_name or "DATABASE_URL",
        )


class NeonBranchManager:
    """
    Manages Neon branch lifecycle operations.

    This class encapsulates all Neon API interactions for branch management,
    making it easier to test and reason about branch operations.
    """

    def __init__(self, config: NeonConfig):
        self.config = config
        self._neon = NeonAPI(api_key=config.api_key)
        self._default_branch_id: str | None = None
        self._default_branch_id_fetched = False

    def get_default_branch_id(self) -> str | None:
        """Get the default/primary branch ID (cached)."""
        if not self._default_branch_id_fetched:
            self._default_branch_id = _get_default_branch_id(
                self._neon, self.config.project_id
            )
            self._default_branch_id_fetched = True
        return self._default_branch_id

    def create_branch(
        self,
        name_suffix: str = "",
        parent_branch_id: str | None = None,
        expiry_seconds: int | None = None,
    ) -> NeonBranch:
        """
        Create a new Neon branch with a read_write endpoint.

        Args:
            name_suffix: Suffix to add to branch name (e.g., "-test")
            parent_branch_id: Parent branch ID (defaults to config's parent)
            expiry_seconds: Branch expiry in seconds (0 or None for no expiry)

        Returns:
            NeonBranch with connection details
        """
        parent_id = parent_branch_id or self.config.parent_branch_id

        # Generate unique branch name
        random_suffix = os.urandom(2).hex()
        git_branch = _get_git_branch_name()
        if git_branch:
            git_prefix = git_branch[:15]
            branch_name = f"pytest-{git_prefix}-{random_suffix}{name_suffix}"
        else:
            branch_name = f"pytest-{random_suffix}{name_suffix}"

        # Build branch config
        branch_config: dict[str, Any] = {"name": branch_name}
        if parent_id:
            branch_config["parent_id"] = parent_id

        # Set expiry if specified
        if expiry_seconds and expiry_seconds > 0:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expiry_seconds)
            branch_config["expires_at"] = expires_at.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Create branch with read_write endpoint
        result = _retry_on_rate_limit(
            lambda: self._neon.branch_create(
                project_id=self.config.project_id,
                branch=branch_config,
                endpoints=[{"type": "read_write"}],
            ),
            operation_name="branch_create",
        )

        branch = result.branch
        endpoint_id = None
        for op in result.operations:
            if op.endpoint_id:
                endpoint_id = op.endpoint_id
                break

        if not endpoint_id:
            raise RuntimeError(f"No endpoint created for branch {branch.id}")

        # Wait for endpoint to be active
        host = self._wait_for_endpoint(endpoint_id)

        # Safety check: never operate on default branch
        default_branch_id = self.get_default_branch_id()
        if default_branch_id and branch.id == default_branch_id:
            raise RuntimeError(
                f"SAFETY CHECK FAILED: Attempted to operate on default branch "
                f"{branch.id}. Please report this bug."
            )

        # Get password
        connection_string = self._get_password_and_build_connection_string(
            branch.id, host
        )

        return NeonBranch(
            branch_id=branch.id,
            project_id=self.config.project_id,
            connection_string=connection_string,
            host=host,
            parent_id=branch.parent_id,
            endpoint_id=endpoint_id,
        )

    def delete_branch(self, branch_id: str) -> None:
        """Delete a branch (silently ignores errors)."""
        if self.config.keep_branches:
            return
        try:
            _retry_on_rate_limit(
                lambda: self._neon.branch_delete(
                    project_id=self.config.project_id, branch_id=branch_id
                ),
                operation_name="branch_delete",
            )
        except Exception as e:
            msg = f"Failed to delete Neon branch {branch_id}: {e}"
            warnings.warn(msg, stacklevel=2)

    def _wait_for_endpoint(self, endpoint_id: str, max_wait_seconds: float = 60) -> str:
        """Wait for endpoint to become active and return its host."""
        poll_interval = 0.5
        waited = 0.0

        while True:
            endpoint_response = _retry_on_rate_limit(
                lambda: self._neon.endpoint(
                    project_id=self.config.project_id, endpoint_id=endpoint_id
                ),
                operation_name="endpoint_status",
            )
            endpoint = endpoint_response.endpoint
            state = endpoint.current_state

            if state == EndpointState.active:
                return endpoint.host

            if waited >= max_wait_seconds:
                raise RuntimeError(
                    f"Timeout waiting for endpoint {endpoint_id} to become active "
                    f"(current state: {state})"
                )

            time.sleep(poll_interval)
            waited += poll_interval

    def _get_password_and_build_connection_string(
        self, branch_id: str, host: str
    ) -> str:
        """Get role password (without resetting) and build connection string."""
        password = _retry_on_rate_limit(
            lambda: _reveal_role_password(
                api_key=self.config.api_key,
                project_id=self.config.project_id,
                branch_id=branch_id,
                role_name=self.config.role_name,
            ),
            operation_name="role_password_reveal",
        )

        return (
            f"postgresql://{self.config.role_name}:{password}@{host}/"
            f"{self.config.database_name}?sslmode=require"
        )


class XdistCoordinator:
    """
    Coordinates branch sharing across pytest-xdist workers.

    Uses file locks and JSON cache files to ensure only one worker creates
    shared resources (like the test branch), while others reuse them.
    """

    def __init__(self, tmp_path_factory: pytest.TempPathFactory):
        self.worker_id = _get_xdist_worker_id()
        self.is_xdist = self.worker_id != "main"

        if self.is_xdist:
            root_tmp_dir = tmp_path_factory.getbasetemp().parent
            self._lock_dir = root_tmp_dir
        else:
            self._lock_dir = None

    def coordinate_resource(
        self,
        resource_name: str,
        create_fn: Callable[[], dict[str, Any]],
    ) -> tuple[dict[str, Any], bool]:
        """
        Coordinate creation of a shared resource across workers.

        Args:
            resource_name: Name of the resource (used for cache/lock files)
            create_fn: Function to create the resource, returns dict to cache

        Returns:
            Tuple of (cached_data, is_creator)
        """
        if not self.is_xdist:
            return create_fn(), True

        assert self._lock_dir is not None
        cache_file = self._lock_dir / f"neon_{resource_name}.json"
        lock_file = self._lock_dir / f"neon_{resource_name}.lock"

        with FileLock(str(lock_file)):
            if cache_file.exists():
                data = json.loads(cache_file.read_text())
                return data, False
            else:
                data = create_fn()
                cache_file.write_text(json.dumps(data))
                return data, True

    def wait_for_signal(self, signal_name: str, timeout: float = 60) -> None:
        """Wait for a signal file to be created by another worker."""
        if not self.is_xdist or self._lock_dir is None:
            return

        signal_file = self._lock_dir / f"neon_{signal_name}"
        waited = 0.0
        poll_interval = 0.5

        while not signal_file.exists():
            if waited >= timeout:
                raise RuntimeError(
                    f"Worker {self.worker_id} timed out waiting for signal "
                    f"'{signal_name}' after {timeout}s. This usually means the "
                    f"creator worker failed or is still processing."
                )
            time.sleep(poll_interval)
            waited += poll_interval

    def send_signal(self, signal_name: str) -> None:
        """Create a signal file for other workers."""
        if not self.is_xdist or self._lock_dir is None:
            return

        signal_file = self._lock_dir / f"neon_{signal_name}"
        signal_file.write_text("done")


class EnvironmentManager:
    """Manages DATABASE_URL environment variable lifecycle."""

    def __init__(self, env_var_name: str = "DATABASE_URL"):
        self.env_var_name = env_var_name
        self._original_value: str | None = None
        self._is_set = False

    def set(self, connection_string: str) -> None:
        """Set the environment variable, saving original value."""
        if not self._is_set:
            self._original_value = os.environ.get(self.env_var_name)
            self._is_set = True
        os.environ[self.env_var_name] = connection_string

    def restore(self) -> None:
        """Restore the original environment variable value."""
        if not self._is_set:
            return

        if self._original_value is None:
            os.environ.pop(self.env_var_name, None)
        else:
            os.environ[self.env_var_name] = self._original_value

        self._is_set = False

    @contextlib.contextmanager
    def temporary(self, connection_string: str) -> Generator[None, None, None]:
        """Context manager for temporary environment variable."""
        self.set(connection_string)
        try:
            yield
        finally:
            self.restore()


def _get_default_branch_id(neon: NeonAPI, project_id: str) -> str | None:
    """
    Get the default/primary branch ID for a project.

    This is used as a safety check to ensure we never accidentally
    perform destructive operations on the production branch.

    Returns:
        The branch ID of the default branch, or None if not found.
    """
    try:
        # Wrap in retry logic to handle rate limits
        # See: https://api-docs.neon.tech/reference/api-rate-limiting
        response = _retry_on_rate_limit(
            lambda: neon.branches(project_id=project_id),
            operation_name="list_branches",
        )
        for branch in response.branches:
            # Check both 'default' and 'primary' flags for compatibility
            if getattr(branch, "default", False) or getattr(branch, "primary", False):
                return branch.id
    except Exception:
        # If we can't fetch branches, don't block - the safety check
        # will be skipped but tests can still run
        pass
    return None


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


def _branch_to_dict(branch: NeonBranch) -> dict[str, Any]:
    """Convert NeonBranch to a JSON-serializable dict."""
    return asdict(branch)


def _dict_to_branch(data: dict[str, Any]) -> NeonBranch:
    """Convert a dict back to NeonBranch."""
    return NeonBranch(**data)


# Timeout for waiting for migrations to complete (seconds)
_MIGRATION_WAIT_TIMEOUT = 300  # 5 minutes


@pytest.fixture(scope="session")
def _neon_config(request: pytest.FixtureRequest) -> NeonConfig:
    """
    Session-scoped Neon configuration extracted from pytest config.

    Skips tests if required configuration (api_key, project_id) is missing.
    """
    config = NeonConfig.from_pytest_config(request.config)
    if config is None:
        pytest.skip(
            "Neon configuration missing. Set NEON_API_KEY and NEON_PROJECT_ID "
            "environment variables or use --neon-api-key and --neon-project-id."
        )
    return config


@pytest.fixture(scope="session")
def _neon_branch_manager(_neon_config: NeonConfig) -> NeonBranchManager:
    """Session-scoped branch manager for Neon operations."""
    return NeonBranchManager(_neon_config)


@pytest.fixture(scope="session")
def _neon_xdist_coordinator(
    tmp_path_factory: pytest.TempPathFactory,
) -> XdistCoordinator:
    """Session-scoped coordinator for xdist worker synchronization."""
    return XdistCoordinator(tmp_path_factory)


@pytest.fixture(scope="session")
def _neon_test_branch(
    _neon_config: NeonConfig,
    _neon_branch_manager: NeonBranchManager,
    _neon_xdist_coordinator: XdistCoordinator,
) -> Generator[tuple[NeonBranch, bool], None, None]:
    """
    Internal: Create test branch, coordinated across workers.

    This creates a single branch with expiry that all tests share.
    The first worker creates the branch, others reuse it.

    Yields:
        Tuple of (branch, is_creator) where is_creator indicates if this
        worker created the branch (and should run migrations/cleanup).
    """
    env_manager = EnvironmentManager(_neon_config.env_var_name)

    def create_branch() -> dict[str, Any]:
        b = _neon_branch_manager.create_branch(
            name_suffix="-test",
            expiry_seconds=_neon_config.branch_expiry,
        )
        return {"branch": _branch_to_dict(b)}

    data, is_creator = _neon_xdist_coordinator.coordinate_resource(
        "test_branch", create_branch
    )
    branch = _dict_to_branch(data["branch"])
    env_manager.set(branch.connection_string)

    try:
        yield branch, is_creator
    finally:
        env_manager.restore()
        if is_creator:
            _neon_branch_manager.delete_branch(branch.branch_id)


@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_test_branch: tuple[NeonBranch, bool]) -> Any:
    """
    Override this fixture to run migrations on the test database.

    The test branch is already created and DATABASE_URL is set.
    Migrations run once per test session, before any tests execute.

    pytest-xdist Support:
        When running with pytest-xdist, migrations only run on the first
        worker (the one that created the test branch). Other workers
        wait for migrations to complete before proceeding. This ensures
        migrations run exactly once, even with parallel workers.

    Example in conftest.py:

        @pytest.fixture(scope="session")
        def neon_apply_migrations(_neon_test_branch):
            import subprocess
            subprocess.run(["alembic", "upgrade", "head"], check=True)

    Or with Django:

        @pytest.fixture(scope="session")
        def neon_apply_migrations(_neon_test_branch):
            from django.core.management import call_command
            call_command("migrate", "--noinput")

    Or with raw SQL:

        @pytest.fixture(scope="session")
        def neon_apply_migrations(_neon_test_branch):
            import psycopg
            branch, is_creator = _neon_test_branch
            with psycopg.connect(branch.connection_string) as conn:
                with open("schema.sql") as f:
                    conn.execute(f.read())
                conn.commit()

    Args:
        _neon_test_branch: Tuple of (NeonBranch, is_creator).
            Use _neon_test_branch[0].connection_string to connect directly,
            or rely on DATABASE_URL which is already set.

    Returns:
        Any value (ignored). The default returns None.
    """
    return None


@pytest.fixture(scope="session")
def neon_branch(
    _neon_test_branch: tuple[NeonBranch, bool],
    _neon_xdist_coordinator: XdistCoordinator,
    neon_apply_migrations: Any,
) -> NeonBranch:
    """
    Provide a shared Neon database branch for all tests.

    This is a session-scoped branch that all tests share. Migrations run
    once before tests start, then all tests see the same database state.

    Since all tests share the branch:
    - Data written by one test IS visible to subsequent tests
    - Use transaction rollback or cleanup fixtures for test isolation
    - Tests run in parallel (xdist) share the same branch

    The connection string is automatically set in the DATABASE_URL environment
    variable (configurable via --neon-env-var).

    Requires either:
        - NEON_API_KEY and NEON_PROJECT_ID environment variables, or
        - --neon-api-key and --neon-project-id command line options

    Returns:
        NeonBranch: Object with branch_id, project_id, connection_string, and host.

    Example::

        def test_query_users(neon_branch):
            # DATABASE_URL is automatically set
            import psycopg
            with psycopg.connect(neon_branch.connection_string) as conn:
                result = conn.execute("SELECT * FROM users").fetchall()
                assert len(result) >= 0

    For test isolation, use a transaction fixture::

        @pytest.fixture
        def db_transaction(neon_branch):
            import psycopg
            conn = psycopg.connect(neon_branch.connection_string)
            conn.execute("BEGIN")
            yield conn
            conn.execute("ROLLBACK")
            conn.close()

        def test_with_isolation(db_transaction):
            db_transaction.execute("INSERT INTO users (name) VALUES ('test')")
            # Rolled back after test - next test won't see this
    """
    branch, is_creator = _neon_test_branch

    if is_creator:
        # Creator runs migrations (via dependency), then signals completion
        _neon_xdist_coordinator.send_signal("migrations_done")
    else:
        # Non-creators wait for migrations to complete
        _neon_xdist_coordinator.wait_for_signal(
            "migrations_done", timeout=_MIGRATION_WAIT_TIMEOUT
        )

    return branch


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
            "  Or use the 'neon_branch' fixture with your own driver:\n\n"
            "      def test_example(neon_branch):\n"
            "          import your_driver\n"
            "          conn = your_driver.connect(\n"
            "              neon_branch.connection_string)\n\n"
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
            "  Or use the 'neon_branch' fixture with your own driver:\n\n"
            "      def test_example(neon_branch):\n"
            "          import your_driver\n"
            "          conn = your_driver.connect(\n"
            "              neon_branch.connection_string)\n\n"
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
            "  Or use the 'neon_branch' fixture with your own driver:\n\n"
            "      def test_example(neon_branch):\n"
            "          from sqlalchemy import create_engine\n"
            "          engine = create_engine(\n"
            "              neon_branch.connection_string)\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
        )

    engine = create_engine(neon_branch.connection_string)
    yield engine
    engine.dispose()
