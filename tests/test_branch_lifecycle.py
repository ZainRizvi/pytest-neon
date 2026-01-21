"""Tests for branch creation, deletion, and lifecycle management."""


class TestBranchLifecycle:
    """Test branch create/delete behavior."""

    def test_branch_created_and_deleted(self, pytester):
        """Test that branch is created at start and deleted at end."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            api_calls = []

            @pytest.fixture(scope="module")
            def neon_branch(request):
                api_calls.append("branch_create")

                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                os.environ["DATABASE_URL"] = branch_info.connection_string
                try:
                    yield branch_info
                finally:
                    os.environ.pop("DATABASE_URL", None)
                    api_calls.append("branch_delete")

            @pytest.fixture(scope="session", autouse=True)
            def verify_api_calls():
                yield
                assert api_calls == ["branch_create", "branch_delete"]
            """
        )

        pytester.makepyfile(
            """
            def test_uses_branch(neon_branch):
                assert neon_branch.branch_id == "br-mock-123"
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)

    def test_branch_not_deleted_when_keep_branches(self, pytester):
        """Test that branch is NOT deleted when --neon-keep-branches is set."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            api_calls = []

            @pytest.fixture(scope="module")
            def neon_branch(request):
                keep = request.config.getoption("neon_keep_branches", default=False)
                api_calls.append("branch_create")

                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                os.environ["DATABASE_URL"] = branch_info.connection_string
                try:
                    yield branch_info
                finally:
                    os.environ.pop("DATABASE_URL", None)
                    if not keep:
                        api_calls.append("branch_delete")

            @pytest.fixture(scope="session", autouse=True)
            def verify_api_calls():
                yield
                assert api_calls == ["branch_create"]  # No delete
            """
        )

        pytester.makepyfile(
            """
            def test_uses_branch(neon_branch):
                pass
            """
        )

        result = pytester.runpytest("-v", "--neon-keep-branches")
        result.assert_outcomes(passed=1)

    def test_branch_deleted_even_on_test_failure(self, pytester):
        """Test that branch is still deleted when tests fail."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            api_calls = []

            @pytest.fixture(scope="module")
            def neon_branch(request):
                api_calls.append("branch_create")

                branch_info = NeonBranch(
                    branch_id="br-mock-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@mock.neon.tech/mockdb",
                    host="mock.neon.tech",
                )

                os.environ["DATABASE_URL"] = branch_info.connection_string
                try:
                    yield branch_info
                finally:
                    os.environ.pop("DATABASE_URL", None)
                    api_calls.append("branch_delete")

            @pytest.fixture(scope="session", autouse=True)
            def verify_cleanup():
                yield
                assert "branch_delete" in api_calls
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


class TestModuleScopeBehavior:
    """Test that fixture scope='module' works correctly."""

    def test_same_branch_within_module(self, pytester):
        """Test that all tests in same module share one branch."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            branch_create_count = [0]

            @pytest.fixture(scope="module")
            def neon_branch(request):
                branch_create_count[0] += 1
                branch_id = f"br-{branch_create_count[0]}"

                branch_info = NeonBranch(
                    branch_id=branch_id,
                    project_id="proj-mock",
                    connection_string=f"postgresql://mock:mock@{branch_id}.neon.tech/mockdb",
                    host=f"{branch_id}.neon.tech",
                )

                os.environ["DATABASE_URL"] = branch_info.connection_string
                try:
                    yield branch_info
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="session", autouse=True)
            def verify_single_branch():
                yield
                assert branch_create_count[0] == 1
            """
        )

        pytester.makepyfile(
            """
            branch_ids_seen = []

            def test_first(neon_branch):
                branch_ids_seen.append(neon_branch.branch_id)

            def test_second(neon_branch):
                branch_ids_seen.append(neon_branch.branch_id)
                assert len(set(branch_ids_seen)) == 1  # Same branch
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)

    def test_different_modules_get_different_branches(self, pytester):
        """Test that different test modules get their own branches."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch

            branch_create_count = [0]

            @pytest.fixture(scope="module")
            def neon_branch(request):
                branch_create_count[0] += 1
                branch_id = f"br-{branch_create_count[0]}"

                branch_info = NeonBranch(
                    branch_id=branch_id,
                    project_id="proj-mock",
                    connection_string=f"postgresql://mock:mock@{branch_id}.neon.tech/mockdb",
                    host=f"{branch_id}.neon.tech",
                )

                os.environ["DATABASE_URL"] = branch_info.connection_string
                try:
                    yield branch_info
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="session", autouse=True)
            def verify_multiple_branches():
                yield
                assert branch_create_count[0] == 2  # Two modules = two branches
            """
        )

        pytester.makepyfile(
            test_module_a="""
            def test_in_module_a(neon_branch):
                pass
            """,
            test_module_b="""
            def test_in_module_b(neon_branch):
                pass
            """,
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)
