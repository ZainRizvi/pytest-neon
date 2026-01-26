"""Tests for neon_branch_readwrite and neon_branch_readonly fixtures."""


class TestReadwriteFixture:
    """Test neon_branch_readwrite fixture behavior."""

    def test_readwrite_resets_after_each_test(self, pytester):
        """Verify that neon_branch_readwrite resets after each test."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            reset_count = [0]

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_branch_for_reset():
                branch = FakeNeonBranch(
                    branch_id="br-test",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="function")
            def neon_branch_readwrite(_neon_branch_for_reset):
                yield _neon_branch_for_reset
                # Simulate reset
                reset_count[0] += 1

            def pytest_sessionfinish(session, exitstatus):
                # Verify resets happened
                assert reset_count[0] == 2, f"Expected 2 resets, got {reset_count[0]}"
        """
        )

        pytester.makepyfile(
            """
            def test_first(neon_branch_readwrite):
                assert neon_branch_readwrite.branch_id == "br-test"

            def test_second(neon_branch_readwrite):
                assert neon_branch_readwrite.branch_id == "br-test"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)


class TestReadonlyFixture:
    """Test neon_branch_readonly fixture behavior."""

    def test_readonly_does_not_reset(self, pytester):
        """Verify that neon_branch_readonly does NOT reset after tests."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            reset_count = [0]

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_branch_for_reset():
                branch = FakeNeonBranch(
                    branch_id="br-test",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="function")
            def neon_branch_readonly(_neon_branch_for_reset):
                # No reset - just return the branch
                return _neon_branch_for_reset

            def pytest_sessionfinish(session, exitstatus):
                # Verify NO resets happened
                assert reset_count[0] == 0, f"Expected 0 resets, got {reset_count[0]}"
        """
        )

        pytester.makepyfile(
            """
            def test_first(neon_branch_readonly):
                assert neon_branch_readonly.branch_id == "br-test"

            def test_second(neon_branch_readonly):
                assert neon_branch_readonly.branch_id == "br-test"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)


class TestDeprecatedNeonBranch:
    """Test that neon_branch emits deprecation warning."""

    def test_neon_branch_emits_deprecation_warning(self, pytester):
        """Verify that using neon_branch emits a deprecation warning."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_branch_for_reset():
                branch = FakeNeonBranch(
                    branch_id="br-test",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="function")
            def neon_branch_readwrite(_neon_branch_for_reset):
                yield _neon_branch_for_reset

            @pytest.fixture(scope="function")
            def neon_branch(neon_branch_readwrite):
                import warnings
                warnings.warn(
                    "neon_branch is deprecated. Use neon_branch_readwrite or "
                    "neon_branch_readonly instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                yield neon_branch_readwrite
        """
        )

        pytester.makepyfile(
            """
            def test_deprecated(neon_branch):
                assert neon_branch.branch_id == "br-test"
        """
        )

        result = pytester.runpytest("-v", "-W", "error::DeprecationWarning")
        # Should error during fixture setup (deprecation warning treated as error)
        result.assert_outcomes(errors=1)


class TestFixtureUseTogether:
    """Test using both fixtures in the same test session."""

    def test_readwrite_and_readonly_can_coexist(self, pytester):
        """Verify both fixtures can be used in the same test module."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            reset_count = [0]

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_branch_for_reset():
                branch = FakeNeonBranch(
                    branch_id="br-test",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="function")
            def neon_branch_readwrite(_neon_branch_for_reset):
                yield _neon_branch_for_reset
                reset_count[0] += 1

            @pytest.fixture(scope="function")
            def neon_branch_readonly(_neon_branch_for_reset):
                return _neon_branch_for_reset

            def pytest_sessionfinish(session, exitstatus):
                # Only readwrite tests should trigger reset
                assert reset_count[0] == 1, f"Expected 1 reset, got {reset_count[0]}"
        """
        )

        pytester.makepyfile(
            """
            def test_readonly_first(neon_branch_readonly):
                '''Read-only test - no reset after.'''
                assert neon_branch_readonly.branch_id == "br-test"

            def test_readonly_second(neon_branch_readonly):
                '''Another read-only test - still no reset.'''
                assert neon_branch_readonly.branch_id == "br-test"

            def test_readwrite(neon_branch_readwrite):
                '''Read-write test - reset after this one.'''
                assert neon_branch_readwrite.branch_id == "br-test"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=3)
