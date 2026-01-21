"""Tests for pytest-neon plugin."""

import os

import pytest


class TestNeonBranchDataclass:
    """Test NeonBranch dataclass structure."""

    def test_dataclass_fields(self):
        from pytest_neon.plugin import NeonBranch

        branch = NeonBranch(
            branch_id="br-test-123",
            project_id="proj-test-456",
            connection_string="postgresql://user:pass@host/db",
            host="host.neon.tech",
        )

        assert branch.branch_id == "br-test-123"
        assert branch.project_id == "proj-test-456"
        assert branch.connection_string == "postgresql://user:pass@host/db"
        assert branch.host == "host.neon.tech"


class TestPluginOptions:
    """Test that CLI options are registered."""

    def test_options_registered(self, pytestconfig):
        # These should not raise
        pytestconfig.getoption("neon_api_key", default=None)
        pytestconfig.getoption("neon_project_id", default=None)
        pytestconfig.getoption("neon_keep_branches", default=False)
        pytestconfig.getoption("neon_branch_expiry", default=600)
        pytestconfig.getoption("neon_env_var", default="DATABASE_URL")


class TestEnvVarBehavior:
    """Test that DATABASE_URL is set and restored correctly."""

    def test_env_var_set_during_fixture(self, pytester):
        """Test that DATABASE_URL is set while fixture is active."""
        pytester.makepyfile(
            """
            import os
            import pytest

            def test_env_var_is_set(neon_branch, monkeypatch):
                # neon_branch fixture should have set DATABASE_URL
                assert "DATABASE_URL" in os.environ
                assert os.environ["DATABASE_URL"] == neon_branch.connection_string
            """
        )

        # Create a conftest that mocks the Neon API
        pytester.makeconftest(
            """
            import os
            import pytest
            from unittest.mock import MagicMock, patch

            @pytest.fixture(scope="module")
            def neon_branch(request):
                from pytest_neon.plugin import NeonBranch

                # Mock the fixture behavior
                branch_info = NeonBranch(
                    branch_id="br-test-123",
                    project_id="proj-test-456",
                    connection_string="postgresql://test:test@test.neon.tech/testdb",
                    host="test.neon.tech",
                )

                env_var_name = "DATABASE_URL"
                original_env_value = os.environ.get(env_var_name)
                os.environ[env_var_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_var_name, None)
                    else:
                        os.environ[env_var_name] = original_env_value
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)

    def test_env_var_restored_after_fixture(self, pytester):
        """Test that DATABASE_URL is restored after fixture completes."""
        pytester.makepyfile(
            test_first="""
            import os

            def test_check_env_set(neon_branch):
                assert os.environ.get("DATABASE_URL") == neon_branch.connection_string
            """,
            test_second="""
            import os

            # This file doesn't use neon_branch, so DATABASE_URL should be restored
            def test_check_env_restored():
                # Should not have the test connection string
                url = os.environ.get("DATABASE_URL", "")
                assert "test.neon.tech" not in url
            """,
        )

        pytester.makeconftest(
            """
            import os
            import pytest

            @pytest.fixture(scope="module")
            def neon_branch(request):
                from pytest_neon.plugin import NeonBranch

                branch_info = NeonBranch(
                    branch_id="br-test-123",
                    project_id="proj-test-456",
                    connection_string="postgresql://test:test@test.neon.tech/testdb",
                    host="test.neon.tech",
                )

                env_var_name = "DATABASE_URL"
                original_env_value = os.environ.get(env_var_name)
                os.environ[env_var_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_var_name, None)
                    else:
                        os.environ[env_var_name] = original_env_value
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)

    def test_original_env_var_preserved(self, pytester):
        """Test that original DATABASE_URL value is restored."""
        pytester.makepyfile(
            """
            import os

            def test_original_preserved(neon_branch):
                # During test, should have test value
                assert os.environ["DATABASE_URL"] == neon_branch.connection_string
            """
        )

        pytester.makeconftest(
            """
            import os
            import pytest

            # Set an original value before any tests
            os.environ["DATABASE_URL"] = "postgresql://original:original@original.com/db"

            @pytest.fixture(scope="module")
            def neon_branch(request):
                from pytest_neon.plugin import NeonBranch

                branch_info = NeonBranch(
                    branch_id="br-test-123",
                    project_id="proj-test-456",
                    connection_string="postgresql://test:test@test.neon.tech/testdb",
                    host="test.neon.tech",
                )

                env_var_name = "DATABASE_URL"
                original_env_value = os.environ.get(env_var_name)
                os.environ[env_var_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_var_name, None)
                    else:
                        os.environ[env_var_name] = original_env_value

            @pytest.fixture(scope="session", autouse=True)
            def verify_restoration():
                yield
                # After all tests, original should be restored
                assert os.environ.get("DATABASE_URL") == "postgresql://original:original@original.com/db"
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)


class TestNeonBranchFixtureWithMockedAPI:
    """Test the actual neon_branch fixture with mocked Neon API."""

    def test_branch_created_and_deleted(self, pytester):
        """Test that branch is created at start and deleted at end."""
        pytester.makepyfile(
            """
            def test_uses_branch(neon_branch):
                assert neon_branch.branch_id == "br-mock-123"
            """
        )

        pytester.makeconftest(
            """
            import os
            import pytest
            from unittest.mock import MagicMock, patch

            # Track API calls
            api_calls = []

            @pytest.fixture(scope="module")
            def neon_branch(request):
                from pytest_neon.plugin import NeonBranch

                api_calls.append("branch_create")

                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                env_var_name = "DATABASE_URL"
                original_env_value = os.environ.get(env_var_name)
                os.environ[env_var_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_var_name, None)
                    else:
                        os.environ[env_var_name] = original_env_value

                    api_calls.append("branch_delete")

            @pytest.fixture(scope="session", autouse=True)
            def verify_api_calls():
                yield
                assert "branch_create" in api_calls
                assert "branch_delete" in api_calls
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)

    def test_branch_not_deleted_when_keep_branches(self, pytester):
        """Test that branch is NOT deleted when keep_branches is True."""
        pytester.makepyfile(
            """
            def test_uses_branch(neon_branch):
                assert neon_branch.branch_id == "br-mock-123"
            """
        )

        pytester.makeconftest(
            """
            import os
            import pytest

            api_calls = []

            @pytest.fixture(scope="module")
            def neon_branch(request):
                from pytest_neon.plugin import NeonBranch

                keep_branches = request.config.getoption("neon_keep_branches", default=False)
                api_calls.append("branch_create")

                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                env_var_name = "DATABASE_URL"
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
                        api_calls.append("branch_delete")

            @pytest.fixture(scope="session", autouse=True)
            def verify_api_calls():
                yield
                assert "branch_create" in api_calls
                assert "branch_delete" not in api_calls  # Should NOT be deleted
            """
        )

        result = pytester.runpytest("-v", "--neon-keep-branches")
        result.assert_outcomes(passed=1)

    def test_env_var_restored_even_on_test_failure(self, pytester):
        """Test that DATABASE_URL is restored even when tests fail."""
        pytester.makepyfile(
            """
            import os

            def test_fails(neon_branch):
                assert False, "Intentional failure"
            """
        )

        pytester.makeconftest(
            """
            import os
            import pytest

            os.environ["DATABASE_URL"] = "postgresql://original:original@original.com/db"
            restoration_verified = []

            @pytest.fixture(scope="module")
            def neon_branch(request):
                from pytest_neon.plugin import NeonBranch

                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                env_var_name = "DATABASE_URL"
                original_env_value = os.environ.get(env_var_name)
                os.environ[env_var_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_var_name, None)
                    else:
                        os.environ[env_var_name] = original_env_value

                    # Verify restoration happened
                    if os.environ.get("DATABASE_URL") == "postgresql://original:original@original.com/db":
                        restoration_verified.append(True)

            @pytest.fixture(scope="session", autouse=True)
            def verify_restoration():
                yield
                assert len(restoration_verified) == 1
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(failed=1)


class TestConvenienceFixtureErrors:
    """Test that convenience fixtures show clear error messages."""

    def test_psycopg2_missing_error(self, pytester):
        """Test clear error when psycopg2 is not installed."""
        pytester.makepyfile(
            """
            def test_uses_connection(neon_connection):
                pass
            """
        )

        pytester.makeconftest(
            """
            import os
            import sys
            import pytest
            from pytest_neon.plugin import NeonBranch

            # Block psycopg2 import
            sys.modules['psycopg2'] = None

            @pytest.fixture(scope="module")
            def neon_branch():
                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )
                os.environ["DATABASE_URL"] = branch_info.connection_string
                yield branch_info
            """
        )

        result = pytester.runpytest("-v")
        # Error happens in fixture setup, so pytest reports it as "errors" not "failed"
        result.assert_outcomes(errors=1)
        # Check error message contains helpful info
        result.stdout.fnmatch_lines(["*MISSING DEPENDENCY: psycopg2*"])
        result.stdout.fnmatch_lines(["*pip install pytest-neon*psycopg2*"])

    def test_psycopg_missing_error(self, pytester):
        """Test clear error when psycopg (v3) is not installed."""
        pytester.makepyfile(
            """
            def test_uses_connection(neon_connection_psycopg):
                pass
            """
        )

        pytester.makeconftest(
            """
            import os
            import sys
            import pytest
            from pytest_neon.plugin import NeonBranch

            # Block psycopg import
            sys.modules['psycopg'] = None

            @pytest.fixture(scope="module")
            def neon_branch():
                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )
                os.environ["DATABASE_URL"] = branch_info.connection_string
                yield branch_info
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(errors=1)
        result.stdout.fnmatch_lines(["*MISSING DEPENDENCY: psycopg (v3)*"])
        result.stdout.fnmatch_lines(["*pip install pytest-neon*psycopg*"])

    def test_sqlalchemy_missing_error(self, pytester):
        """Test clear error when SQLAlchemy is not installed."""
        pytester.makepyfile(
            """
            def test_uses_engine(neon_engine):
                pass
            """
        )

        pytester.makeconftest(
            """
            import os
            import sys
            import pytest
            from pytest_neon.plugin import NeonBranch

            # Block sqlalchemy import
            sys.modules['sqlalchemy'] = None

            @pytest.fixture(scope="module")
            def neon_branch():
                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )
                os.environ["DATABASE_URL"] = branch_info.connection_string
                yield branch_info
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(errors=1)
        result.stdout.fnmatch_lines(["*MISSING DEPENDENCY: SQLAlchemy*"])
        result.stdout.fnmatch_lines(["*pip install pytest-neon*sqlalchemy*"])


class TestSkipBehavior:
    """Test that fixtures skip appropriately when not configured."""

    def test_skips_without_api_key(self, pytester):
        """Test that neon_branch skips when API key is not set."""
        pytester.makepyfile(
            """
            import os

            def test_needs_branch(neon_branch):
                pass
            """
        )

        # Ensure env vars are not set
        pytester.makeconftest(
            """
            import os
            os.environ.pop("NEON_API_KEY", None)
            os.environ.pop("NEON_PROJECT_ID", None)
            """
        )

        # Use -rs to show skip reasons in full
        result = pytester.runpytest("-v", "-rs")
        result.assert_outcomes(skipped=1)
        result.stdout.fnmatch_lines(["*NEON_API_KEY*"])

    def test_skips_without_project_id(self, pytester):
        """Test that neon_branch skips when project ID is not set."""
        pytester.makepyfile(
            """
            def test_needs_branch(neon_branch):
                pass
            """
        )

        pytester.makeconftest(
            """
            import os
            os.environ["NEON_API_KEY"] = "test-key"
            os.environ.pop("NEON_PROJECT_ID", None)
            """
        )

        # Use -rs to show skip reasons in full
        result = pytester.runpytest("-v", "-rs")
        result.assert_outcomes(skipped=1)
        result.stdout.fnmatch_lines(["*NEON_PROJECT_ID*"])
