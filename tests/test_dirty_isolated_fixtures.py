"""Tests for neon_branch_dirty and neon_branch_isolated fixtures."""


class TestDirtyFixture:
    """Test neon_branch_dirty fixture behavior."""

    def test_dirty_is_session_scoped(self, pytester):
        """Verify neon_branch_dirty is session-scoped (same branch across tests)."""
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

            @pytest.fixture(scope="session")
            def neon_branch_dirty(_neon_branch_for_reset):
                return _neon_branch_for_reset
        """
        )

        pytester.makepyfile(
            """
            def test_first(neon_branch_dirty):
                assert neon_branch_dirty.branch_id == "br-test"

            def test_second(neon_branch_dirty):
                # Same branch as first test
                assert neon_branch_dirty.branch_id == "br-test"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)

    def test_dirty_shares_state_across_tests(self, pytester):
        """Verify that neon_branch_dirty shares state (no reset between tests)."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            # Counter to track "writes" - simulates shared state
            shared_state = {"writes": 0}

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

            @pytest.fixture(scope="session")
            def neon_branch_dirty(_neon_branch_for_reset):
                return _neon_branch_for_reset

            @pytest.fixture
            def shared_counter():
                return shared_state
        """
        )

        pytester.makepyfile(
            """
            def test_first_write(neon_branch_dirty, shared_counter):
                '''First test writes data.'''
                shared_counter["writes"] += 1
                assert shared_counter["writes"] == 1

            def test_second_sees_first_write(neon_branch_dirty, shared_counter):
                '''Second test should see data from first (no reset).'''
                # If there was a reset, this would be 0
                assert shared_counter["writes"] == 1
                shared_counter["writes"] += 1
                assert shared_counter["writes"] == 2

            def test_third_sees_all_writes(neon_branch_dirty, shared_counter):
                '''Third test sees accumulated state.'''
                assert shared_counter["writes"] == 2
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=3)


class TestIsolatedFixture:
    """Test neon_branch_isolated fixture behavior."""

    def test_isolated_creates_new_branch_per_test(self, pytester):
        """Verify that neon_branch_isolated creates a new branch for each test."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            branch_ids = []
            branch_counter = [0]

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_migration_branch():
                branch = FakeNeonBranch(
                    branch_id="br-migration",
                    project_id="proj-test",
                    connection_string="postgresql://migration",
                    host="migration.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="session")
            def _neon_migrations_synchronized():
                return None

            @pytest.fixture(scope="function")
            def neon_branch_isolated(
                request, _neon_migration_branch, _neon_migrations_synchronized
            ):
                # Simulate creating a new branch per test
                branch_counter[0] += 1
                branch_id = f"br-isolated-{branch_counter[0]}"
                branch_ids.append(branch_id)

                branch = FakeNeonBranch(
                    branch_id=branch_id,
                    project_id="proj-test",
                    connection_string=f"postgresql://isolated-{branch_counter[0]}",
                    host="isolated.neon.tech",
                    parent_id=_neon_migration_branch.branch_id,
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    # Simulate branch deletion
                    os.environ.pop("DATABASE_URL", None)

            def pytest_sessionfinish(session, exitstatus):
                # Verify each test got a unique branch
                assert len(branch_ids) == 3, (
                    f"Expected 3 branches, got {len(branch_ids)}"
                )
                assert len(set(branch_ids)) == 3, (
                    f"Branches should be unique: {branch_ids}"
                )
        """
        )

        pytester.makepyfile(
            """
            def test_first(neon_branch_isolated):
                assert neon_branch_isolated.branch_id == "br-isolated-1"
                assert neon_branch_isolated.parent_id == "br-migration"

            def test_second(neon_branch_isolated):
                assert neon_branch_isolated.branch_id == "br-isolated-2"
                assert neon_branch_isolated.parent_id == "br-migration"

            def test_third(neon_branch_isolated):
                assert neon_branch_isolated.branch_id == "br-isolated-3"
                assert neon_branch_isolated.parent_id == "br-migration"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=3)

    def test_isolated_cleans_up_env_var(self, pytester):
        """Verify that neon_branch_isolated restores DATABASE_URL after test."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            original_db_url = "original://database"

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_migration_branch():
                branch = FakeNeonBranch(
                    branch_id="br-migration",
                    project_id="proj-test",
                    connection_string="postgresql://migration",
                    host="migration.neon.tech",
                    parent_id="br-parent",
                )
                yield branch

            @pytest.fixture(scope="session")
            def _neon_migrations_synchronized():
                return None

            @pytest.fixture(scope="function")
            def neon_branch_isolated(
                request, _neon_migration_branch, _neon_migrations_synchronized
            ):
                # Store original
                original = os.environ.get("DATABASE_URL")

                branch = FakeNeonBranch(
                    branch_id="br-isolated",
                    project_id="proj-test",
                    connection_string="postgresql://isolated",
                    host="isolated.neon.tech",
                    parent_id=_neon_migration_branch.branch_id,
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    # Restore original
                    if original is None:
                        os.environ.pop("DATABASE_URL", None)
                    else:
                        os.environ["DATABASE_URL"] = original
        """
        )

        pytester.makepyfile(
            """
            import os

            def test_isolated_sets_env(neon_branch_isolated):
                assert os.environ["DATABASE_URL"] == "postgresql://isolated"

            def test_after_isolated_env_is_cleared():
                # After isolated fixture, DATABASE_URL should be cleared
                # (since there was no original value)
                assert "DATABASE_URL" not in os.environ
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)


class TestDirtyAndIsolatedTogether:
    """Test using both neon_branch_dirty and neon_branch_isolated together."""

    def test_can_mix_dirty_and_isolated(self, pytester):
        """Verify dirty and isolated fixtures can be used in the same session."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            branch_counter = [0]
            dirty_branch_id = [None]

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_branch_for_reset():
                dirty_branch_id[0] = "br-dirty"
                branch = FakeNeonBranch(
                    branch_id="br-dirty",
                    project_id="proj-test",
                    connection_string="postgresql://dirty",
                    host="dirty.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="session")
            def _neon_migration_branch():
                branch = FakeNeonBranch(
                    branch_id="br-migration",
                    project_id="proj-test",
                    connection_string="postgresql://migration",
                    host="migration.neon.tech",
                    parent_id="br-parent",
                )
                yield branch

            @pytest.fixture(scope="session")
            def _neon_migrations_synchronized():
                return None

            @pytest.fixture(scope="session")
            def neon_branch_dirty(_neon_branch_for_reset):
                return _neon_branch_for_reset

            @pytest.fixture(scope="function")
            def neon_branch_isolated(
                request, _neon_migration_branch, _neon_migrations_synchronized
            ):
                branch_counter[0] += 1
                branch = FakeNeonBranch(
                    branch_id=f"br-isolated-{branch_counter[0]}",
                    project_id="proj-test",
                    connection_string=f"postgresql://isolated-{branch_counter[0]}",
                    host="isolated.neon.tech",
                    parent_id=_neon_migration_branch.branch_id,
                )
                original = os.environ.get("DATABASE_URL")
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    if original is None:
                        os.environ.pop("DATABASE_URL", None)
                    else:
                        os.environ["DATABASE_URL"] = original
        """
        )

        pytester.makepyfile(
            """
            def test_dirty_first(neon_branch_dirty):
                '''Use dirty fixture.'''
                assert neon_branch_dirty.branch_id == "br-dirty"

            def test_isolated_middle(neon_branch_isolated):
                '''Use isolated fixture - gets its own branch.'''
                assert neon_branch_isolated.branch_id == "br-isolated-1"
                assert neon_branch_isolated.parent_id == "br-migration"

            def test_dirty_again(neon_branch_dirty):
                '''Back to dirty fixture - same branch as before.'''
                assert neon_branch_dirty.branch_id == "br-dirty"

            def test_another_isolated(neon_branch_isolated):
                '''Another isolated test - new branch.'''
                assert neon_branch_isolated.branch_id == "br-isolated-2"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=4)
