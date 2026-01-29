"""Pytest plugin providing Neon database branch fixtures.

This plugin provides fixtures for database testing using Neon's instant
branching feature. Multiple isolation levels are available:

Main fixtures:
    neon_branch_readonly: True read-only access via read_only endpoint (enforced)
    neon_branch_dirty: Session-scoped read-write, shared state across all tests
    neon_branch_isolated: Per-worker branch with reset after each test (recommended)
    neon_branch_readwrite: Deprecated, use neon_branch_isolated instead
    neon_branch: Deprecated alias for neon_branch_isolated
    neon_branch_shared: Shared branch without reset (module-scoped)

Connection fixtures (require extras):
    neon_connection: psycopg2 connection (requires psycopg2 extra)
    neon_connection_psycopg: psycopg v3 connection (requires psycopg extra)
    neon_engine: SQLAlchemy engine (requires sqlalchemy extra)

Architecture:
    Parent Branch (configured or project default)
        └── Migration Branch (session-scoped, read_write endpoint)
                │   ↑ migrations run here ONCE
                │
                ├── Read-only Endpoint (read_only endpoint ON migration branch)
                │       ↑ neon_branch_readonly uses this
                │
                ├── Dirty Branch (session-scoped child, shared across ALL workers)
                │       ↑ neon_branch_dirty uses this
                │
                └── Isolated Branch (one per xdist worker, lazily created)
                        ↑ neon_branch_isolated uses this, reset after each test

SQLAlchemy Users:
    If you create your own SQLAlchemy engine (not using neon_engine fixture),
    you MUST use pool_pre_ping=True when using neon_branch_isolated:

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

# Sentinel value to detect when neon_apply_migrations was not overridden
_MIGRATIONS_NOT_DEFINED = object()


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
    gets a unique ID (gw0, gw1, gw2, etc.). This is used to create separate
    branches per worker to avoid database state pollution between parallel tests.
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


def _extract_password_from_connection_string(connection_string: str) -> str:
    """Extract password from a PostgreSQL connection string."""
    # Format: postgresql://user:password@host/db?params
    from urllib.parse import urlparse

    parsed = urlparse(connection_string)
    if parsed.password:
        return parsed.password
    raise ValueError(f"No password found in connection string: {connection_string}")


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


def _get_schema_fingerprint(connection_string: str) -> tuple[tuple[Any, ...], ...]:
    """
    Get a fingerprint of the database schema for change detection.

    Queries information_schema for all tables, columns, and their properties
    in the public schema. Returns a hashable tuple that can be compared
    before/after migrations to detect if the schema actually changed.

    This is used to avoid creating unnecessary migration branches when
    no actual schema changes occurred.
    """
    try:
        import psycopg
    except ImportError:
        try:
            import psycopg2 as psycopg  # type: ignore[import-not-found]
        except ImportError:
            # No driver available - can't fingerprint, assume migrations changed things
            return ()

    with psycopg.connect(connection_string) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, column_name, data_type, is_nullable,
                   column_default, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)
        rows = cur.fetchall()
    return tuple(tuple(row) for row in rows)


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
            name_suffix: Suffix to add to branch name (e.g., "-migration", "-dirty")
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

    def create_readonly_endpoint(self, branch: NeonBranch) -> NeonBranch:
        """
        Create a read_only endpoint on an existing branch.

        This creates a true read-only endpoint that enforces no writes at the
        database level.

        Args:
            branch: The branch to create the endpoint on

        Returns:
            NeonBranch with the read_only endpoint's connection details
        """
        result = _retry_on_rate_limit(
            lambda: self._neon.endpoint_create(
                project_id=self.config.project_id,
                endpoint={
                    "branch_id": branch.branch_id,
                    "type": "read_only",
                },
            ),
            operation_name="endpoint_create_readonly",
        )

        endpoint_id = result.endpoint.id
        host = self._wait_for_endpoint(endpoint_id)

        # Reuse the password from the parent branch's connection string.
        # DO NOT call role_password_reset here - it would invalidate the
        # password used by the parent branch's read_write endpoint, breaking
        # any existing connections (especially in xdist where other workers
        # may be using the cached connection string).
        password = _extract_password_from_connection_string(branch.connection_string)
        connection_string = (
            f"postgresql://{self.config.role_name}:{password}@{host}/"
            f"{self.config.database_name}?sslmode=require"
        )

        return NeonBranch(
            branch_id=branch.branch_id,
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

    def delete_endpoint(self, endpoint_id: str) -> None:
        """Delete an endpoint (silently ignores errors)."""
        try:
            _retry_on_rate_limit(
                lambda: self._neon.endpoint_delete(
                    project_id=self.config.project_id, endpoint_id=endpoint_id
                ),
                operation_name="endpoint_delete",
            )
        except Exception as e:
            warnings.warn(
                f"Failed to delete Neon endpoint {endpoint_id}: {e}", stacklevel=2
            )

    def reset_branch(self, branch: NeonBranch) -> None:
        """Reset a branch to its parent's state."""
        if not branch.parent_id:
            msg = f"Branch {branch.branch_id} has no parent - cannot reset"
            raise RuntimeError(msg)

        _reset_branch_to_parent(branch, self.config.api_key)

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
    shared resources (like the migration branch), while others reuse them.
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
    perform destructive operations (like password reset) on the
    production branch.

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

    # Cache the default branch ID for safety checks (only fetch once per session)
    if not hasattr(config, "_neon_default_branch_id"):
        config._neon_default_branch_id = _get_default_branch_id(neon, project_id)  # type: ignore[attr-defined]

    # Generate unique branch name
    # Format: pytest-[git branch (first 15 chars)]-[random]-[suffix]
    # This helps identify orphaned branches by showing which git branch created them
    random_suffix = os.urandom(2).hex()  # 2 bytes = 4 hex chars
    git_branch = _get_git_branch_name()
    if git_branch:
        # Truncate git branch to 15 chars to keep branch names reasonable
        git_prefix = git_branch[:15]
        branch_name = f"pytest-{git_prefix}-{random_suffix}{branch_name_suffix}"
    else:
        branch_name = f"pytest-{random_suffix}{branch_name_suffix}"

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
    # Wrap in retry logic to handle rate limits
    # See: https://api-docs.neon.tech/reference/api-rate-limiting
    result = _retry_on_rate_limit(
        lambda: neon.branch_create(
            project_id=project_id,
            branch=branch_config,
            endpoints=[{"type": "read_write"}],
        ),
        operation_name="branch_create",
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
        # Wrap in retry logic to handle rate limits during polling
        # See: https://api-docs.neon.tech/reference/api-rate-limiting
        endpoint_response = _retry_on_rate_limit(
            lambda: neon.endpoint(project_id=project_id, endpoint_id=endpoint_id),
            operation_name="endpoint_status",
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

    # Get password using reveal (not reset) to avoid invalidating existing connections
    # See: https://api-docs.neon.tech/reference/getprojectbranchrolepassword
    password = _retry_on_rate_limit(
        lambda: _reveal_role_password(
            api_key=api_key,
            project_id=project_id,
            branch_id=branch.id,
            role_name=role_name,
        ),
        operation_name="role_password_reveal",
    )

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
        endpoint_id=endpoint_id,
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
                # Wrap in retry logic to handle rate limits
                # See: https://api-docs.neon.tech/reference/api-rate-limiting
                _retry_on_rate_limit(
                    lambda: neon.branch_delete(
                        project_id=project_id, branch_id=branch.id
                    ),
                    operation_name="branch_delete",
                )
            except Exception as e:
                # Log but don't fail tests due to cleanup issues
                warnings.warn(
                    f"Failed to delete Neon branch {branch.id}: {e}",
                    stacklevel=2,
                )


def _create_readonly_endpoint(
    branch: NeonBranch,
    api_key: str,
    database_name: str,
    role_name: str,
) -> NeonBranch:
    """
    Create a read_only endpoint on an existing branch.

    Returns a new NeonBranch object with the read_only endpoint's connection string.
    The read_only endpoint enforces that no writes can be made through this connection.

    Args:
        branch: The branch to create a read_only endpoint on
        api_key: Neon API key
        database_name: Database name for connection string
        role_name: Role name for connection string

    Returns:
        NeonBranch with read_only endpoint connection details
    """
    neon = NeonAPI(api_key=api_key)

    # Create read_only endpoint on the branch
    # See: https://api-docs.neon.tech/reference/createprojectendpoint
    result = _retry_on_rate_limit(
        lambda: neon.endpoint_create(
            project_id=branch.project_id,
            endpoint={
                "branch_id": branch.branch_id,
                "type": "read_only",
            },
        ),
        operation_name="endpoint_create_readonly",
    )

    endpoint = result.endpoint
    endpoint_id = endpoint.id

    # Wait for endpoint to be ready
    max_wait_seconds = 60
    poll_interval = 0.5
    waited = 0.0

    while True:
        endpoint_response = _retry_on_rate_limit(
            lambda: neon.endpoint(
                project_id=branch.project_id, endpoint_id=endpoint_id
            ),
            operation_name="endpoint_status_readonly",
        )
        endpoint = endpoint_response.endpoint
        state = endpoint.current_state

        if state == EndpointState.active:
            break

        if waited >= max_wait_seconds:
            raise RuntimeError(
                f"Timeout waiting for read_only endpoint {endpoint_id} "
                f"to become active (current state: {state})"
            )

        time.sleep(poll_interval)
        waited += poll_interval

    host = endpoint.host

    # Reuse the password from the parent branch's connection string.
    # DO NOT call role_password_reset here - it would invalidate the
    # password used by the parent branch's read_write endpoint.
    password = _extract_password_from_connection_string(branch.connection_string)

    # Build connection string for the read_only endpoint
    connection_string = (
        f"postgresql://{role_name}:{password}@{host}/{database_name}?sslmode=require"
    )

    return NeonBranch(
        branch_id=branch.branch_id,
        project_id=branch.project_id,
        connection_string=connection_string,
        host=host,
        parent_id=branch.parent_id,
        endpoint_id=endpoint_id,
    )


def _delete_endpoint(project_id: str, endpoint_id: str, api_key: str) -> None:
    """Delete a Neon endpoint."""
    neon = NeonAPI(api_key=api_key)
    try:
        _retry_on_rate_limit(
            lambda: neon.endpoint_delete(
                project_id=project_id, endpoint_id=endpoint_id
            ),
            operation_name="endpoint_delete",
        )
    except Exception as e:
        warnings.warn(
            f"Failed to delete Neon endpoint {endpoint_id}: {e}",
            stacklevel=2,
        )


def _reset_branch_to_parent(branch: NeonBranch, api_key: str) -> None:
    """Reset a branch to its parent's state using the Neon API.

    Uses exponential backoff retry logic with jitter to handle rate limit (429)
    errors. After initiating the restore, polls the operation status until it
    completes.

    See: https://api-docs.neon.tech/reference/api-rate-limiting

    Args:
        branch: The branch to reset
        api_key: Neon API key
    """
    if not branch.parent_id:
        raise RuntimeError(f"Branch {branch.branch_id} has no parent - cannot reset")

    base_url = "https://console.neon.tech/api/v2"
    project_id = branch.project_id
    branch_id = branch.branch_id
    restore_url = f"{base_url}/projects/{project_id}/branches/{branch_id}/restore"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def do_restore() -> dict[str, Any]:
        response = requests.post(
            restore_url,
            headers=headers,
            json={"source_branch_id": branch.parent_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    # Wrap in retry logic to handle rate limits
    # See: https://api-docs.neon.tech/reference/api-rate-limiting
    data = _retry_on_rate_limit(do_restore, operation_name="branch_restore")
    operations = data.get("operations", [])

    # The restore API returns operations that run asynchronously.
    # We must wait for operations to complete before the next test
    # starts, otherwise connections may fail during the restore.
    if operations:
        _wait_for_operations(
            project_id=branch.project_id,
            operations=operations,
            headers=headers,
            base_url=base_url,
        )


def _wait_for_operations(
    project_id: str,
    operations: list[dict[str, Any]],
    headers: dict[str, str],
    base_url: str,
    max_wait_seconds: float = 60,
    poll_interval: float = 0.5,
) -> None:
    """Wait for Neon operations to complete.

    Handles rate limit (429) errors with exponential backoff retry.
    See: https://api-docs.neon.tech/reference/api-rate-limiting

    Args:
        project_id: The Neon project ID
        operations: List of operation dicts from the API response
        headers: HTTP headers including auth
        base_url: Base URL for Neon API
        max_wait_seconds: Maximum time to wait (default: 60s)
        poll_interval: Time between polls (default: 0.5s)
    """
    # Get operation IDs that aren't already finished
    pending_op_ids = [
        op["id"] for op in operations if op.get("status") not in ("finished", "skipped")
    ]

    if not pending_op_ids:
        return  # All operations already complete

    waited = 0.0
    first_poll = True
    while pending_op_ids and waited < max_wait_seconds:
        # Poll immediately first time (operation usually completes instantly),
        # then wait between subsequent polls
        if first_poll:
            time.sleep(0.1)  # Tiny delay to let operation start
            waited += 0.1
            first_poll = False
        else:
            time.sleep(poll_interval)
            waited += poll_interval

        # Check status of each pending operation
        still_pending = []
        for op_id in pending_op_ids:
            op_url = f"{base_url}/projects/{project_id}/operations/{op_id}"

            def get_operation_status(url: str = op_url) -> dict[str, Any]:
                """Fetch operation status. Default arg captures url by value."""
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                return response.json()

            try:
                # Wrap in retry logic to handle rate limits
                # See: https://api-docs.neon.tech/reference/api-rate-limiting
                result = _retry_on_rate_limit(
                    get_operation_status,
                    operation_name=f"operation_status({op_id})",
                )
                op_data = result.get("operation", {})
                status = op_data.get("status")

                if status == "failed":
                    err = op_data.get("error", "unknown error")
                    raise RuntimeError(f"Operation {op_id} failed: {err}")
                if status not in ("finished", "skipped", "cancelled"):
                    still_pending.append(op_id)
            except requests.RequestException:
                # On network error (non-429), assume still pending and retry
                still_pending.append(op_id)

        pending_op_ids = still_pending

    if pending_op_ids:
        raise RuntimeError(
            f"Timeout waiting for operations to complete: {pending_op_ids}"
        )


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
def _neon_migration_branch(
    request: pytest.FixtureRequest,
    _neon_config: NeonConfig,
    _neon_branch_manager: NeonBranchManager,
    _neon_xdist_coordinator: XdistCoordinator,
) -> Generator[NeonBranch, None, None]:
    """
    Session-scoped branch where migrations are applied.

    This branch is ALWAYS created from the configured parent and serves as
    the parent for all test branches (dirty, isolated, readonly endpoint).
    Migrations run once per session on this branch.

    pytest-xdist Support:
        When running with pytest-xdist, the first worker to acquire the lock
        creates the migration branch. Other workers wait for migrations to
        complete, then reuse the same branch. This avoids redundant API calls
        and ensures migrations only run once. Only the creator cleans up the
        branch at session end.

    Note: The migration branch cannot have an expiry because Neon doesn't
    allow creating child branches from branches with expiration dates.
    Cleanup relies on the fixture teardown at session end.
    """
    env_manager = EnvironmentManager(_neon_config.env_var_name)
    branch: NeonBranch
    is_creator: bool

    def create_migration_branch() -> dict[str, Any]:
        b = _neon_branch_manager.create_branch(
            name_suffix="-migration",
            expiry_seconds=0,  # No expiry - child branches need this
        )
        return {"branch": _branch_to_dict(b)}

    # Coordinate branch creation across xdist workers
    data, is_creator = _neon_xdist_coordinator.coordinate_resource(
        "migration_branch", create_migration_branch
    )
    branch = _dict_to_branch(data["branch"])

    # Store creator status for other fixtures
    request.config._neon_is_migration_creator = is_creator  # type: ignore[attr-defined]

    # Set DATABASE_URL
    env_manager.set(branch.connection_string)

    # Non-creators wait for migrations to complete
    if not is_creator:
        _neon_xdist_coordinator.wait_for_signal(
            "migrations_done", timeout=_MIGRATION_WAIT_TIMEOUT
        )

    try:
        yield branch
    finally:
        env_manager.restore()
        # Only creator cleans up
        if is_creator:
            _neon_branch_manager.delete_branch(branch.branch_id)


@pytest.fixture(scope="session")
def neon_apply_migrations(_neon_migration_branch: NeonBranch) -> Any:
    """
    Override this fixture to run migrations on the test database.

    The migration branch is already created and DATABASE_URL is set.
    Migrations run once per test session, before any tests execute.

    pytest-xdist Support:
        When running with pytest-xdist, migrations only run on the first
        worker (the one that created the migration branch). Other workers
        wait for migrations to complete before proceeding. This ensures
        migrations run exactly once, even with parallel workers.

    Smart Migration Detection:
        The plugin automatically detects whether migrations actually modified
        the database schema. If no schema changes occurred (or this fixture
        isn't overridden), the plugin skips creating a separate migration
        branch, saving Neon costs and branch slots.

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

    Returns:
        Any value (ignored). The default returns a sentinel to indicate
        the fixture was not overridden.
    """
    return _MIGRATIONS_NOT_DEFINED


@pytest.fixture(scope="session")
def _neon_migrations_synchronized(
    request: pytest.FixtureRequest,
    _neon_migration_branch: NeonBranch,
    _neon_xdist_coordinator: XdistCoordinator,
    neon_apply_migrations: Any,
) -> Any:
    """
    Internal fixture that synchronizes migrations across xdist workers.

    This fixture ensures that:
    1. Only the creator worker runs migrations (non-creators wait in
       _neon_migration_branch BEFORE neon_apply_migrations runs)
    2. Creator signals completion after migrations finish
    3. The return value from neon_apply_migrations is preserved for detection

    Without xdist, this is a simple passthrough.
    """
    is_creator = getattr(request.config, "_neon_is_migration_creator", True)

    if is_creator:
        # Creator: migrations just ran via neon_apply_migrations dependency
        # Signal completion to other workers
        _neon_xdist_coordinator.send_signal("migrations_done")

    return neon_apply_migrations


@pytest.fixture(scope="session")
def _neon_dirty_branch(
    _neon_config: NeonConfig,
    _neon_branch_manager: NeonBranchManager,
    _neon_xdist_coordinator: XdistCoordinator,
    _neon_migration_branch: NeonBranch,
    _neon_migrations_synchronized: Any,  # Ensures migrations complete first
) -> Generator[NeonBranch, None, None]:
    """
    Session-scoped dirty branch shared across ALL xdist workers.

    This branch is a child of the migration branch. All tests using
    neon_branch_dirty share this single branch - writes persist and
    are visible to all tests (even across workers).

    This is the "dirty" branch because:
    - No reset between tests
    - Shared across all workers (concurrent writes possible)
    - Fast because no per-test overhead
    """
    env_manager = EnvironmentManager(_neon_config.env_var_name)
    branch: NeonBranch
    is_creator: bool

    def create_dirty_branch() -> dict[str, Any]:
        b = _neon_branch_manager.create_branch(
            name_suffix="-dirty",
            parent_branch_id=_neon_migration_branch.branch_id,
            expiry_seconds=_neon_config.branch_expiry,
        )
        return {"branch": _branch_to_dict(b)}

    # Coordinate dirty branch creation - shared across ALL workers
    data, is_creator = _neon_xdist_coordinator.coordinate_resource(
        "dirty_branch", create_dirty_branch
    )
    branch = _dict_to_branch(data["branch"])

    # Set DATABASE_URL
    env_manager.set(branch.connection_string)

    try:
        yield branch
    finally:
        env_manager.restore()
        # Only creator cleans up
        if is_creator:
            _neon_branch_manager.delete_branch(branch.branch_id)


@pytest.fixture(scope="session")
def _neon_readonly_endpoint(
    _neon_config: NeonConfig,
    _neon_branch_manager: NeonBranchManager,
    _neon_xdist_coordinator: XdistCoordinator,
    _neon_migration_branch: NeonBranch,
    _neon_migrations_synchronized: Any,  # Ensures migrations complete first
) -> Generator[NeonBranch, None, None]:
    """
    Session-scoped read_only endpoint on the migration branch.

    This is a true read-only endpoint - writes are blocked at the database
    level. All workers share this endpoint since it's read-only anyway.
    """
    env_manager = EnvironmentManager(_neon_config.env_var_name)
    branch: NeonBranch
    is_creator: bool

    def create_readonly_endpoint() -> dict[str, Any]:
        b = _neon_branch_manager.create_readonly_endpoint(_neon_migration_branch)
        return {"branch": _branch_to_dict(b)}

    # Coordinate endpoint creation - shared across ALL workers
    data, is_creator = _neon_xdist_coordinator.coordinate_resource(
        "readonly_endpoint", create_readonly_endpoint
    )
    branch = _dict_to_branch(data["branch"])

    # Set DATABASE_URL
    env_manager.set(branch.connection_string)

    try:
        yield branch
    finally:
        env_manager.restore()
        # Only creator cleans up the endpoint
        if is_creator and branch.endpoint_id:
            _neon_branch_manager.delete_endpoint(branch.endpoint_id)


@pytest.fixture(scope="session")
def _neon_isolated_branch(
    request: pytest.FixtureRequest,
    _neon_config: NeonConfig,
    _neon_branch_manager: NeonBranchManager,
    _neon_xdist_coordinator: XdistCoordinator,
    _neon_migration_branch: NeonBranch,
    _neon_migrations_synchronized: Any,  # Ensures migrations complete first
) -> Generator[NeonBranch, None, None]:
    """
    Session-scoped isolated branch, one per xdist worker.

    Each worker gets its own branch. Unlike the dirty branch, this is
    per-worker to allow reset operations without affecting other workers.

    The branch is reset after each test that uses neon_branch_isolated.
    """
    env_manager = EnvironmentManager(_neon_config.env_var_name)
    worker_id = _neon_xdist_coordinator.worker_id

    # Each worker creates its own isolated branch - no coordination needed
    # because each worker has a unique ID
    branch = _neon_branch_manager.create_branch(
        name_suffix=f"-isolated-{worker_id}",
        parent_branch_id=_neon_migration_branch.branch_id,
        expiry_seconds=_neon_config.branch_expiry,
    )

    # Store branch manager on config for reset operations
    request.config._neon_isolated_branch_manager = _neon_branch_manager  # type: ignore[attr-defined]

    # Set DATABASE_URL
    env_manager.set(branch.connection_string)

    try:
        yield branch
    finally:
        env_manager.restore()
        _neon_branch_manager.delete_branch(branch.branch_id)


@pytest.fixture(scope="session")
def neon_branch_readonly(
    _neon_config: NeonConfig,
    _neon_readonly_endpoint: NeonBranch,
) -> NeonBranch:
    """
    Provide a true read-only Neon database connection.

    This fixture uses a read_only endpoint on the migration branch, which
    enforces read-only access at the database level. Any attempt to write
    will result in a database error.

    This is the recommended fixture for tests that only read data (SELECT queries).
    It's session-scoped and shared across all tests and workers since it's read-only.

    Use this fixture when your tests only perform SELECT queries.
    For tests that INSERT, UPDATE, or DELETE data, use ``neon_branch_dirty``
    (for shared state) or ``neon_branch_isolated`` (for test isolation).

    The connection string is automatically set in the DATABASE_URL environment
    variable (configurable via --neon-env-var).

    Requires either:
        - NEON_API_KEY and NEON_PROJECT_ID environment variables, or
        - --neon-api-key and --neon-project-id command line options

    Returns:
        NeonBranch: Object with branch_id, project_id, connection_string, and host.

    Example::

        def test_query_users(neon_branch_readonly):
            # DATABASE_URL is automatically set
            conn_string = os.environ["DATABASE_URL"]

            # Read-only query
            with psycopg.connect(conn_string) as conn:
                result = conn.execute("SELECT * FROM users").fetchall()
                assert len(result) > 0

            # This would fail with a database error:
            # conn.execute("INSERT INTO users (name) VALUES ('test')")
    """
    # DATABASE_URL is already set by _neon_readonly_endpoint
    return _neon_readonly_endpoint


@pytest.fixture(scope="session")
def neon_branch_dirty(
    _neon_config: NeonConfig,
    _neon_dirty_branch: NeonBranch,
) -> NeonBranch:
    """
    Provide a session-scoped Neon database branch for read-write access.

    All tests share the same branch and writes persist across tests (no cleanup
    between tests). This is faster than neon_branch_isolated because there's no
    reset overhead.

    Use this fixture when:
    - Most tests can share database state without interference
    - You want maximum performance with minimal API calls
    - You manually manage test data cleanup if needed
    - You're using it alongside ``neon_branch_isolated`` for specific tests
      that need guaranteed clean state

    The connection string is automatically set in the DATABASE_URL environment
    variable (configurable via --neon-env-var).

    Warning:
        Data written by one test WILL be visible to subsequent tests AND to
        other xdist workers. This is truly shared - use ``neon_branch_isolated``
        for tests that require guaranteed clean state.

    pytest-xdist:
        ALL workers share the same dirty branch. Concurrent writes from different
        workers may conflict. This is "dirty" by design - for isolation, use
        ``neon_branch_isolated``.

    Requires either:
        - NEON_API_KEY and NEON_PROJECT_ID environment variables, or
        - --neon-api-key and --neon-project-id command line options

    Returns:
        NeonBranch: Object with branch_id, project_id, connection_string, and host.

    Example::

        def test_insert_user(neon_branch_dirty):
            # DATABASE_URL is automatically set
            import psycopg
            with psycopg.connect(neon_branch_dirty.connection_string) as conn:
                conn.execute("INSERT INTO users (name) VALUES ('test')")
                conn.commit()
            # Data persists - next test will see this user

        def test_count_users(neon_branch_dirty):
            # This test sees data from previous tests
            import psycopg
            with psycopg.connect(neon_branch_dirty.connection_string) as conn:
                result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
                # Count includes users from previous tests
    """
    # DATABASE_URL is already set by _neon_dirty_branch
    return _neon_dirty_branch


@pytest.fixture(scope="function")
def neon_branch_isolated(
    request: pytest.FixtureRequest,
    _neon_config: NeonConfig,
    _neon_isolated_branch: NeonBranch,
) -> Generator[NeonBranch, None, None]:
    """
    Provide an isolated Neon database branch with reset after each test.

    This is the recommended fixture for tests that modify database state and
    need isolation. Each xdist worker has its own branch, and the branch is
    reset to the migration state after each test.

    Use this fixture when:
    - Tests modify database state (INSERT, UPDATE, DELETE)
    - You need test isolation (each test starts with clean state)
    - You're using it alongside ``neon_branch_dirty`` for specific tests

    The connection string is automatically set in the DATABASE_URL environment
    variable (configurable via --neon-env-var).

    SQLAlchemy Users:
        If you create your own engine (not using the neon_engine fixture),
        you MUST use pool_pre_ping=True::

            engine = create_engine(DATABASE_URL, pool_pre_ping=True)

        Branch resets terminate server-side connections. Without pool_pre_ping,
        SQLAlchemy may reuse dead pooled connections, causing SSL errors.

    pytest-xdist:
        Each worker has its own isolated branch. Resets only affect that worker's
        branch, so workers don't interfere with each other.

    Requires either:
        - NEON_API_KEY and NEON_PROJECT_ID environment variables, or
        - --neon-api-key and --neon-project-id command line options

    Yields:
        NeonBranch: Object with branch_id, project_id, connection_string, and host.

    Example::

        def test_insert_user(neon_branch_isolated):
            # DATABASE_URL is automatically set
            conn_string = os.environ["DATABASE_URL"]

            # Insert data - branch will reset after this test
            with psycopg.connect(conn_string) as conn:
                conn.execute("INSERT INTO users (name) VALUES ('test')")
                conn.commit()
            # Next test starts with clean state
    """
    # DATABASE_URL is already set by _neon_isolated_branch
    yield _neon_isolated_branch

    # Reset branch to migration state after each test
    branch_manager = getattr(request.config, "_neon_isolated_branch_manager", None)
    if branch_manager is not None:
        try:
            branch_manager.reset_branch(_neon_isolated_branch)
        except Exception as e:
            pytest.fail(
                f"\n\nFailed to reset branch {_neon_isolated_branch.branch_id} "
                f"after test. Subsequent tests may see dirty state.\n\n"
                f"Error: {e}\n\n"
                f"To keep the branch for debugging, use --neon-keep-branches"
            )


@pytest.fixture(scope="function")
def neon_branch_readwrite(
    neon_branch_isolated: NeonBranch,
) -> Generator[NeonBranch, None, None]:
    """
    Deprecated: Use ``neon_branch_isolated`` instead.

    This fixture is now an alias for ``neon_branch_isolated``.

    .. deprecated:: 2.3.0
        Use ``neon_branch_isolated`` for tests that modify data with reset,
        ``neon_branch_dirty`` for shared state, or ``neon_branch_readonly``
        for read-only access.
    """
    warnings.warn(
        "neon_branch_readwrite is deprecated. Use neon_branch_isolated (for tests "
        "that modify data with isolation) or neon_branch_dirty (for shared state).",
        DeprecationWarning,
        stacklevel=2,
    )
    yield neon_branch_isolated


@pytest.fixture(scope="function")
def neon_branch(
    neon_branch_isolated: NeonBranch,
) -> Generator[NeonBranch, None, None]:
    """
    Deprecated: Use ``neon_branch_isolated``, ``neon_branch_dirty``, or
    ``neon_branch_readonly`` instead.

    This fixture is now an alias for ``neon_branch_isolated``.

    .. deprecated:: 1.1.0
        Use ``neon_branch_isolated`` for tests that modify data with reset,
        ``neon_branch_dirty`` for shared state, or ``neon_branch_readonly``
        for read-only access.
    """
    warnings.warn(
        "neon_branch is deprecated. Use neon_branch_isolated (for tests that "
        "modify data), neon_branch_dirty (for shared state), or "
        "neon_branch_readonly (for read-only tests).",
        DeprecationWarning,
        stacklevel=2,
    )
    yield neon_branch_isolated


@pytest.fixture(scope="module")
def neon_branch_shared(
    request: pytest.FixtureRequest,
    _neon_migration_branch: NeonBranch,
    _neon_migrations_synchronized: Any,  # Ensures migrations complete first
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
def neon_connection(neon_branch_isolated: NeonBranch):
    """
    Provide a psycopg2 connection to the test branch.

    Requires the psycopg2 optional dependency:
        pip install pytest-neon[psycopg2]

    The connection is rolled back and closed after each test.
    Uses neon_branch_isolated for test isolation.

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
            "  Or use the 'neon_branch_isolated' fixture with your own driver:\n\n"
            "      def test_example(neon_branch_isolated):\n"
            "          import your_driver\n"
            "          conn = your_driver.connect(\n"
            "              neon_branch_isolated.connection_string)\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
        )

    conn = psycopg2.connect(neon_branch_isolated.connection_string)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def neon_connection_psycopg(neon_branch_isolated: NeonBranch):
    """
    Provide a psycopg (v3) connection to the test branch.

    Requires the psycopg optional dependency:
        pip install pytest-neon[psycopg]

    The connection is rolled back and closed after each test.
    Uses neon_branch_isolated for test isolation.

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
            "  Or use the 'neon_branch_isolated' fixture with your own driver:\n\n"
            "      def test_example(neon_branch_isolated):\n"
            "          import your_driver\n"
            "          conn = your_driver.connect(\n"
            "              neon_branch_isolated.connection_string)\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
        )

    conn = psycopg.connect(neon_branch_isolated.connection_string)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def neon_engine(neon_branch_isolated: NeonBranch):
    """
    Provide a SQLAlchemy engine connected to the test branch.

    Requires the sqlalchemy optional dependency:
        pip install pytest-neon[sqlalchemy]

    The engine is disposed after each test, which handles stale connections
    after branch resets automatically. Uses neon_branch_isolated for test isolation.

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
            "  Or use the 'neon_branch_isolated' fixture with your own driver:\n\n"
            "      def test_example(neon_branch_isolated):\n"
            "          from sqlalchemy import create_engine\n"
            "          engine = create_engine(\n"
            "              neon_branch_isolated.connection_string)\n\n"
            "═══════════════════════════════════════════════════════════════════\n"
        )

    engine = create_engine(neon_branch_isolated.connection_string)
    yield engine
    engine.dispose()
