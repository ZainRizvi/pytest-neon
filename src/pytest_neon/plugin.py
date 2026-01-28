"""Pytest plugin providing Neon database branch fixtures.

This plugin provides fixtures for isolated database testing using Neon's
instant branching feature. Each test gets a clean database state via
branch reset after each test.

Main fixtures:
    neon_branch_readwrite: Read-write access with reset after each test (recommended)
    neon_branch_readonly: Read-only access, no reset (fastest for read-only tests)
    neon_branch: Deprecated alias for neon_branch_readwrite
    neon_branch_shared: Shared branch without reset (module-scoped)
    neon_connection: psycopg2 connection (requires psycopg2 extra)
    neon_connection_psycopg: psycopg v3 connection (requires psycopg extra)
    neon_engine: SQLAlchemy engine (requires sqlalchemy extra)

SQLAlchemy Users:
    If you create your own SQLAlchemy engine (not using neon_engine fixture),
    you MUST use pool_pre_ping=True when using neon_branch_readwrite:

        engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    This is required because branch resets terminate server-side connections.
    Without pool_pre_ping, SQLAlchemy may try to reuse dead pooled connections,
    causing "SSL connection has been closed unexpectedly" errors.

    Note: pool_pre_ping is not required for neon_branch_readonly since no
    resets occur.

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

    # SAFETY CHECK: Ensure we never reset password on the default/production branch
    # This should be impossible since we just created this branch, but we check
    # defensively to prevent catastrophic mistakes if there's ever a bug
    default_branch_id = getattr(config, "_neon_default_branch_id", None)
    if default_branch_id and branch.id == default_branch_id:
        raise RuntimeError(
            f"SAFETY CHECK FAILED: Attempted to reset password on default branch "
            f"{branch.id}. This should never happen - the plugin creates new "
            f"branches and should never operate on the default branch. "
            f"Please report this bug at https://github.com/ZainRizvi/pytest-neon/issues"
        )

    # Reset password to get the password value
    # (newly created branches don't expose password)
    # Wrap in retry logic to handle rate limits
    # See: https://api-docs.neon.tech/reference/api-rate-limiting
    password_response = _retry_on_rate_limit(
        lambda: neon.role_password_reset(
            project_id=project_id,
            branch_id=branch.id,
            role_name=role_name,
        ),
        operation_name="role_password_reset",
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
def _neon_migration_branch(
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[NeonBranch, None, None]:
    """
    Session-scoped branch where migrations are applied.

    This branch is created from the configured parent and serves as
    the parent for all test branches. Migrations run once per session
    on this branch.

    pytest-xdist Support:
        When running with pytest-xdist, the first worker to acquire the lock
        creates the migration branch. Other workers wait for migrations to
        complete, then reuse the same branch. This avoids redundant API calls
        and ensures migrations only run once. Only the creator cleans up the
        branch at session end.

    Note: The migration branch cannot have an expiry because Neon doesn't
    allow creating child branches from branches with expiration dates.
    Cleanup relies on the fixture teardown at session end.

    Smart Migration Detection:
        Before yielding, this fixture captures a schema fingerprint and stores
        it on request.config. After migrations run, _neon_branch_for_reset
        compares the fingerprint to detect if the schema actually changed.
    """
    config = request.config
    worker_id = _get_xdist_worker_id()
    is_xdist = worker_id != "main"

    # Get env var name for DATABASE_URL
    env_var_name = _get_config_value(
        config, "neon_env_var", "", "neon_env_var", "DATABASE_URL"
    )

    # For xdist, use shared temp directory and filelock
    # tmp_path_factory.getbasetemp().parent is shared across all workers
    if is_xdist:
        root_tmp_dir = tmp_path_factory.getbasetemp().parent
        cache_file = root_tmp_dir / "neon_migration_branch.json"
        lock_file = root_tmp_dir / "neon_migration_branch.lock"
    else:
        cache_file = None
        lock_file = None

    is_creator = False
    branch: NeonBranch
    branch_gen: Generator[NeonBranch, None, None] | None = None
    original_env_value: str | None = None

    if is_xdist:
        assert cache_file is not None and lock_file is not None
        with FileLock(str(lock_file)):
            if cache_file.exists():
                # Another worker already created the branch - reuse it
                data = json.loads(cache_file.read_text())
                branch = _dict_to_branch(data["branch"])
                pre_migration_fingerprint = tuple(
                    tuple(row) for row in data["pre_migration_fingerprint"]
                )
                config._neon_pre_migration_fingerprint = pre_migration_fingerprint  # type: ignore[attr-defined]

                # Set DATABASE_URL for this worker (not done by _create_neon_branch)
                original_env_value = os.environ.get(env_var_name)
                os.environ[env_var_name] = branch.connection_string
            else:
                # First worker - create branch and cache it
                is_creator = True
                branch_gen = _create_neon_branch(
                    request,
                    branch_expiry_override=0,
                    branch_name_suffix="-migrated",
                )
                branch = next(branch_gen)

                # Capture schema fingerprint BEFORE migrations run
                pre_migration_fingerprint = _get_schema_fingerprint(
                    branch.connection_string
                )
                config._neon_pre_migration_fingerprint = pre_migration_fingerprint  # type: ignore[attr-defined]

                # Cache for other workers (they'll read this after lock released)
                # Note: We cache now with pre-migration fingerprint. The branch
                # content will have migrations applied by neon_apply_migrations.
                cache_file.write_text(
                    json.dumps(
                        {
                            "branch": _branch_to_dict(branch),
                            "pre_migration_fingerprint": pre_migration_fingerprint,
                        }
                    )
                )
    else:
        # Not using xdist - create branch normally
        is_creator = True
        branch_gen = _create_neon_branch(
            request,
            branch_expiry_override=0,
            branch_name_suffix="-migrated",
        )
        branch = next(branch_gen)

        # Capture schema fingerprint BEFORE migrations run
        pre_migration_fingerprint = _get_schema_fingerprint(branch.connection_string)
        config._neon_pre_migration_fingerprint = pre_migration_fingerprint  # type: ignore[attr-defined]

    # Mark whether this worker is the creator (used by neon_apply_migrations)
    config._neon_is_migration_creator = is_creator  # type: ignore[attr-defined]

    try:
        yield branch
    finally:
        # Restore env var if we set it (non-creator workers)
        if original_env_value is not None:
            os.environ[env_var_name] = original_env_value
        elif not is_creator and env_var_name in os.environ:
            os.environ.pop(env_var_name, None)

        # Only the creator cleans up the branch
        if is_creator and branch_gen is not None:
            with contextlib.suppress(StopIteration):
                next(branch_gen)


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
    tmp_path_factory: pytest.TempPathFactory,
    _neon_migration_branch: NeonBranch,
    neon_apply_migrations: Any,
) -> Any:
    """
    Internal fixture that synchronizes migrations across xdist workers.

    This fixture ensures that:
    1. Only the creator worker runs migrations
    2. Other workers wait for migrations to complete before proceeding
    3. The return value from neon_apply_migrations is preserved for detection

    Without xdist, this is a simple passthrough.
    """
    config = request.config
    worker_id = _get_xdist_worker_id()
    is_xdist = worker_id != "main"
    is_creator = getattr(config, "_neon_is_migration_creator", True)

    if not is_xdist:
        # Not using xdist - migrations already ran, just return the value
        return neon_apply_migrations

    # For xdist, use a signal file to coordinate
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    migrations_done_file = root_tmp_dir / "neon_migrations_done"
    migrations_lock_file = root_tmp_dir / "neon_migrations.lock"

    if is_creator:
        # Creator: migrations just ran via neon_apply_migrations dependency
        # Signal completion to other workers
        with FileLock(str(migrations_lock_file)):
            migrations_done_file.write_text("done")
        return neon_apply_migrations
    else:
        # Non-creator: wait for migrations to complete
        # The neon_apply_migrations fixture still runs but on already-migrated DB
        # (most migration tools handle this gracefully as a no-op)
        waited = 0.0
        poll_interval = 0.5
        while not migrations_done_file.exists():
            if waited >= _MIGRATION_WAIT_TIMEOUT:
                raise RuntimeError(
                    f"Timeout waiting for migrations to complete after "
                    f"{_MIGRATION_WAIT_TIMEOUT}s. The creator worker may have "
                    f"failed or is still running migrations."
                )
            time.sleep(poll_interval)
            waited += poll_interval

        return neon_apply_migrations


@pytest.fixture(scope="session")
def _neon_branch_for_reset(
    request: pytest.FixtureRequest,
    _neon_migration_branch: NeonBranch,
    _neon_migrations_synchronized: Any,  # Ensures migrations complete; for detection
) -> Generator[NeonBranch, None, None]:
    """
    Internal fixture that creates a test branch from the migration branch.

    This is session-scoped so DATABASE_URL remains stable throughout the test
    session, avoiding issues with Python's module caching (e.g., SQLAlchemy
    engines created at import time would otherwise point to stale branches).

    Parallel Test Support (pytest-xdist):
        When running tests in parallel with pytest-xdist, each worker gets its
        own branch. This prevents database state pollution between tests running
        concurrently on different workers. The worker ID is included in the
        branch name suffix (e.g., "-test-gw0", "-test-gw1").

    Smart Migration Detection:
        This fixture implements a cost-optimization strategy:

        1. If neon_apply_migrations was not overridden (returns sentinel),
           skip creating a separate test branch - use the migration branch directly.

        2. If neon_apply_migrations was overridden, compare schema fingerprints
           before/after migrations. Only create a child branch if the schema
           actually changed.

        This avoids unnecessary Neon costs and branch slots when:
        - No migration fixture is defined
        - Migrations exist but are already applied (no schema changes)
    """
    # Check if migrations fixture was overridden
    # _neon_migrations_synchronized passes through the neon_apply_migrations value
    migrations_defined = _neon_migrations_synchronized is not _MIGRATIONS_NOT_DEFINED

    # Check if schema actually changed (if we have a pre-migration fingerprint)
    pre_fingerprint = getattr(request.config, "_neon_pre_migration_fingerprint", ())
    schema_changed = False

    if migrations_defined and pre_fingerprint:
        # Compare with current schema
        conn_str = _neon_migration_branch.connection_string
        post_fingerprint = _get_schema_fingerprint(conn_str)
        schema_changed = pre_fingerprint != post_fingerprint
    elif migrations_defined and not pre_fingerprint:
        # No fingerprint available (no psycopg/psycopg2 installed)
        # Assume migrations changed something to be safe
        schema_changed = True

    # Get worker ID for parallel test support
    # Each xdist worker gets its own branch to avoid state pollution
    worker_id = _get_xdist_worker_id()
    branch_suffix = f"-test-{worker_id}"

    # Only create a child branch if migrations actually modified the schema
    # OR if we're running under xdist (each worker needs its own branch)
    if schema_changed or worker_id != "main":
        yield from _create_neon_branch(
            request,
            parent_branch_id_override=_neon_migration_branch.branch_id,
            branch_name_suffix=branch_suffix,
        )
    else:
        # No schema changes and not parallel - reuse the migration branch directly
        # This saves creating an unnecessary branch
        yield _neon_migration_branch


@pytest.fixture(scope="function")
def neon_branch_readwrite(
    request: pytest.FixtureRequest,
    _neon_branch_for_reset: NeonBranch,
) -> Generator[NeonBranch, None, None]:
    """
    Provide a read-write Neon database branch with reset after each test.

    This is the recommended fixture for tests that modify database state.
    It creates one branch per test session, then resets it to the parent
    branch's state after each test. This provides test isolation with
    ~0.5s overhead per test.

    Use this fixture when your tests INSERT, UPDATE, or DELETE data.
    For read-only tests, use ``neon_branch_readonly`` instead for better
    performance (no reset overhead).

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

        def test_insert_user(neon_branch_readwrite):
            # DATABASE_URL is automatically set
            conn_string = os.environ["DATABASE_URL"]
            # or use directly
            conn_string = neon_branch_readwrite.connection_string

            # Insert data - branch will reset after this test
            with psycopg.connect(conn_string) as conn:
                conn.execute("INSERT INTO users (name) VALUES ('test')")
                conn.commit()
    """
    config = request.config
    api_key = _get_config_value(config, "neon_api_key", "NEON_API_KEY", "neon_api_key")

    # Validate that branch has a parent for reset functionality
    if not _neon_branch_for_reset.parent_id:
        pytest.fail(
            f"\n\nBranch {_neon_branch_for_reset.branch_id} has no parent. "
            f"The neon_branch_readwrite fixture requires a parent branch for "
            f"reset.\n\n"
            f"Use neon_branch_readonly if you don't need reset, or specify "
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


@pytest.fixture(scope="function")
def neon_branch_readonly(
    _neon_branch_for_reset: NeonBranch,
) -> NeonBranch:
    """
    Provide a read-only Neon database branch without reset.

    This is the recommended fixture for tests that only read data (SELECT queries).
    No branch reset occurs after each test, making it faster than
    ``neon_branch_readwrite`` (~0.5s saved per test).

    Use this fixture when your tests only perform SELECT queries and don't
    modify database state. For tests that INSERT, UPDATE, or DELETE data,
    use ``neon_branch_readwrite`` instead to ensure test isolation.

    Warning:
        If you accidentally write data using this fixture, subsequent tests
        will see those modifications. The fixture does not enforce read-only
        access at the database level - it simply skips the reset step.

    The connection string is automatically set in the DATABASE_URL environment
    variable (configurable via --neon-env-var).

    Requires either:
        - NEON_API_KEY and NEON_PROJECT_ID environment variables, or
        - --neon-api-key and --neon-project-id command line options

    Yields:
        NeonBranch: Object with branch_id, project_id, connection_string, and host.

    Example::

        def test_query_users(neon_branch_readonly):
            # DATABASE_URL is automatically set
            conn_string = os.environ["DATABASE_URL"]

            # Read-only query - no reset needed after this test
            with psycopg.connect(conn_string) as conn:
                result = conn.execute("SELECT * FROM users").fetchall()
                assert len(result) > 0
    """
    return _neon_branch_for_reset


@pytest.fixture(scope="function")
def neon_branch(
    request: pytest.FixtureRequest,
    neon_branch_readwrite: NeonBranch,
) -> Generator[NeonBranch, None, None]:
    """
    Deprecated: Use ``neon_branch_readwrite`` or ``neon_branch_readonly`` instead.

    This fixture is an alias for ``neon_branch_readwrite`` and will be removed
    in a future version. Please migrate to the explicit fixture names:

    - ``neon_branch_readwrite``: For tests that modify data (INSERT/UPDATE/DELETE)
    - ``neon_branch_readonly``: For tests that only read data (SELECT)

    .. deprecated:: 1.1.0
        Use ``neon_branch_readwrite`` for read-write access with reset,
        or ``neon_branch_readonly`` for read-only access without reset.
    """
    warnings.warn(
        "neon_branch is deprecated. Use neon_branch_readwrite (for tests that "
        "modify data) or neon_branch_readonly (for read-only tests) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    yield neon_branch_readwrite


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
