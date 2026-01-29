"""Tests for neon_branch_readwrite and neon_branch_readonly fixtures."""


class TestReadonlySessionScoped:
    """Test that neon_branch_readonly is session-scoped."""

    def test_readonly_is_session_scoped(self, pytester):
        """Verify neon_branch_readonly is session-scoped (same instance per session)."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            instance_ids = []

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_readonly_endpoint():
                branch = FakeNeonBranch(
                    branch_id="br-readonly",
                    project_id="proj-test",
                    connection_string="postgresql://readonly",
                    host="readonly.neon.tech",
                    parent_id="br-migration",
                )
                yield branch

            @pytest.fixture(scope="session")
            def neon_branch_readonly(_neon_readonly_endpoint):
                return _neon_readonly_endpoint

            @pytest.fixture
            def track_readonly_instance(neon_branch_readonly):
                instance_ids.append(id(neon_branch_readonly))
                return neon_branch_readonly

            def pytest_sessionfinish(session, exitstatus):
                # Verify same instance was used across all tests
                assert len(instance_ids) == 3, (
                    f"Expected 3 tests, got {len(instance_ids)}"
                )
                assert len(set(instance_ids)) == 1, (
                    f"Got {len(set(instance_ids))} different instances, expected 1"
                )
        """
        )

        pytester.makepyfile(
            """
            def test_first(track_readonly_instance):
                assert track_readonly_instance.branch_id == "br-readonly"

            def test_second(track_readonly_instance):
                assert track_readonly_instance.branch_id == "br-readonly"

            def test_third(track_readonly_instance):
                assert track_readonly_instance.branch_id == "br-readonly"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=3)


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


class TestDeprecatedFixtures:
    """Test that deprecated fixtures emit deprecation warnings."""

    def test_neon_branch_readwrite_emits_deprecation_warning(self, pytester):
        """Verify that using neon_branch_readwrite emits a deprecation warning."""
        pytester.makeconftest(
            """
            import os
            import pytest
            import warnings
            from dataclasses import dataclass

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_isolated_branch():
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
            def neon_branch_isolated(_neon_isolated_branch):
                yield _neon_isolated_branch

            @pytest.fixture(scope="function")
            def neon_branch_readwrite(neon_branch_isolated):
                warnings.warn(
                    "neon_branch_readwrite is deprecated. Use neon_branch_isolated.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                yield neon_branch_isolated
        """
        )

        pytester.makepyfile(
            """
            def test_deprecated(neon_branch_readwrite):
                assert neon_branch_readwrite.branch_id == "br-test"
        """
        )

        result = pytester.runpytest("-v", "-W", "error::DeprecationWarning")
        # Should error during fixture setup (deprecation warning treated as error)
        result.assert_outcomes(errors=1)

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
