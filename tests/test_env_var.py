"""Tests for DATABASE_URL environment variable handling."""


class TestEnvVarRestoration:
    """Test that DATABASE_URL is properly restored after fixture."""

    def test_env_var_removed_when_not_originally_set(self, pytester):
        """Test that DATABASE_URL is removed if it wasn't set before."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="module")
            def neon_branch(request):
                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                env_name = "DATABASE_URL"
                original_env_value = os.environ.get(env_name)
                os.environ[env_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_name, None)
                    else:
                        os.environ[env_name] = original_env_value

            @pytest.fixture(scope="session", autouse=True)
            def verify_cleanup():
                yield
                assert "DATABASE_URL" not in os.environ, "DATABASE_URL not removed"
            """
        )

        pytester.makepyfile(
            """
            import os

            def test_uses_branch(neon_branch):
                assert os.environ["DATABASE_URL"] == neon_branch.connection_string
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)

    def test_original_value_restored(self, pytester):
        """Test that original DATABASE_URL value is restored."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            ORIGINAL_URL = "postgresql://original:original@original.com/originaldb"
            os.environ["DATABASE_URL"] = ORIGINAL_URL

            @pytest.fixture(scope="module")
            def neon_branch(request):
                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                env_name = "DATABASE_URL"
                original_env_value = os.environ.get(env_name)
                os.environ[env_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_name, None)
                    else:
                        os.environ[env_name] = original_env_value

            @pytest.fixture(scope="session", autouse=True)
            def verify_restoration():
                yield
                assert os.environ.get("DATABASE_URL") == ORIGINAL_URL
            """
        )

        pytester.makepyfile(
            """
            import os

            def test_uses_branch(neon_branch):
                assert "mock.neon.tech" in os.environ["DATABASE_URL"]
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)

    def test_restored_even_on_test_failure(self, pytester):
        """Test that DATABASE_URL is restored even when tests fail."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            ORIGINAL_URL = "postgresql://original:original@original.com/db"
            os.environ["DATABASE_URL"] = ORIGINAL_URL

            @pytest.fixture(scope="module")
            def neon_branch(request):
                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                env_name = "DATABASE_URL"
                original_env_value = os.environ.get(env_name)
                os.environ[env_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_name, None)
                    else:
                        os.environ[env_name] = original_env_value

            @pytest.fixture(scope="session", autouse=True)
            def verify_restoration():
                yield
                assert os.environ.get("DATABASE_URL") == ORIGINAL_URL
            """
        )

        pytester.makepyfile(
            """
            def test_fails(neon_branch):
                assert False, "Intentional failure"
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(failed=1)


class TestCustomEnvVar:
    """Test using a custom environment variable name."""

    def test_custom_env_name(self, pytester):
        """Test that --neon-env-var sets a custom env var instead of DATABASE_URL."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            @pytest.fixture(scope="module")
            def neon_branch(request):
                env_name = request.config.getoption(
                    "neon_env_var", default="DATABASE_URL"
                )

                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                original_env_value = os.environ.get(env_name)
                os.environ[env_name] = branch_info.connection_string

                try:
                    yield branch_info
                finally:
                    if original_env_value is None:
                        os.environ.pop(env_name, None)
                    else:
                        os.environ[env_name] = original_env_value
            """
        )

        pytester.makepyfile(
            """
            import os

            def test_custom_env_var(neon_branch):
                assert "CUSTOM_DB_URL" in os.environ
                assert os.environ["CUSTOM_DB_URL"] == neon_branch.connection_string
            """
        )

        result = pytester.runpytest("-v", "--neon-env-var=CUSTOM_DB_URL")
        result.assert_outcomes(passed=1)
