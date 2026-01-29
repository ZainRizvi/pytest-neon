"""Shared test fixtures and configuration for pytest-neon tests."""

import os

import pytest

# Enable pytester fixture for testing pytest plugins
pytest_plugins = ["pytester"]


@pytest.fixture
def mock_neon_branch_fixture_code():
    """Returns code for mock neon_branch fixtures to use in pytester tests."""
    return '''
import os
import pytest
from pytest_neon.plugin import NeonBranch

@pytest.fixture(scope="function")
def neon_branch_isolated(request):
    """Mock neon_branch_isolated fixture for testing."""
    env_var_name = request.config.getoption("neon_env_var", default=None)
    if env_var_name is None:
        env_var_name = "DATABASE_URL"

    branch_info = NeonBranch(
        branch_id="br-mock-123",
        project_id="proj-mock",
        connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
        host="mock.neon.tech",
    )

    original_env_value = os.environ.get(env_var_name)
    os.environ[env_var_name] = branch_info.connection_string

    try:
        yield branch_info
    finally:
        if original_env_value is None:
            os.environ.pop(env_var_name, None)
        else:
            os.environ[env_var_name] = original_env_value

@pytest.fixture(scope="module")
def neon_branch(request):
    """Mock neon_branch fixture for testing (deprecated alias)."""
    keep_branches = request.config.getoption("neon_keep_branches", default=None)
    if keep_branches is None:
        keep_branches = False

    env_var_name = request.config.getoption("neon_env_var", default=None)
    if env_var_name is None:
        env_var_name = "DATABASE_URL"

    branch_info = NeonBranch(
        branch_id="br-mock-123",
        project_id="proj-mock",
        connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
        host="mock.neon.tech",
    )

    original_env_value = os.environ.get(env_var_name)
    os.environ[env_var_name] = branch_info.connection_string

    try:
        yield branch_info
    finally:
        if original_env_value is None:
            os.environ.pop(env_var_name, None)
        else:
            os.environ[env_var_name] = original_env_value

        if not keep_branches:
            # In real fixture, this would call neon.branch_delete()
            pass
'''


@pytest.fixture
def clean_env():
    """Ensure Neon env vars are not set, restore after test."""
    original_api_key = os.environ.pop("NEON_API_KEY", None)
    original_project_id = os.environ.pop("NEON_PROJECT_ID", None)
    original_db_url = os.environ.pop("DATABASE_URL", None)

    yield

    # Restore
    if original_api_key is not None:
        os.environ["NEON_API_KEY"] = original_api_key
    if original_project_id is not None:
        os.environ["NEON_PROJECT_ID"] = original_project_id
    if original_db_url is not None:
        os.environ["DATABASE_URL"] = original_db_url
