"""Tests for migration support."""


class TestMigrationFixtureOrder:
    """Test that migrations run before tests execute."""

    def test_migrations_run_before_tests(self, pytester):
        """Verify neon_apply_migrations is called before tests run."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            execution_order = []

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str = None

            @pytest.fixture(scope="session")
            def _neon_test_branch():
                execution_order.append("test_branch_created")
                branch = FakeNeonBranch(
                    branch_id="br-test",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                yield branch, True  # is_creator=True

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_test_branch):
                execution_order.append("migrations_applied")
                # User would run migrations here

            @pytest.fixture(scope="session")
            def neon_branch(_neon_test_branch, neon_apply_migrations):
                execution_order.append("neon_branch_ready")
                branch, is_creator = _neon_test_branch
                yield branch

            def pytest_sessionfinish(session, exitstatus):
                # Verify order: branch -> migrations -> ready
                assert execution_order == [
                    "test_branch_created",
                    "migrations_applied",
                    "neon_branch_ready",
                ], f"Wrong order: {execution_order}"
        """
        )

        pytester.makepyfile(
            """
            def test_uses_branch(neon_branch):
                assert neon_branch.branch_id == "br-test"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)

    def test_user_migration_override_is_called(self, pytester):
        """Verify user's neon_apply_migrations override runs."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            migration_ran = [False]

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str = None

            @pytest.fixture(scope="session")
            def _neon_test_branch():
                branch = FakeNeonBranch(
                    branch_id="br-test",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                yield branch, True

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_test_branch):
                # User migration - this should be called
                migration_ran[0] = True

            @pytest.fixture(scope="session")
            def neon_branch(_neon_test_branch, neon_apply_migrations):
                branch, is_creator = _neon_test_branch
                yield branch

            def pytest_sessionfinish(session, exitstatus):
                assert migration_ran[0], "User migration should have run"
        """
        )

        pytester.makepyfile(
            """
            def test_migration_ran(neon_branch):
                pass
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)


class TestSharedBranchBehavior:
    """Test that all tests share the same branch."""

    def test_all_tests_share_branch(self, pytester):
        """Verify all tests in a session share the same branch."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            branch_create_count = [0]

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str = None

            @pytest.fixture(scope="session")
            def _neon_test_branch():
                branch_create_count[0] += 1
                branch = FakeNeonBranch(
                    branch_id=f"br-{branch_create_count[0]}",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                yield branch, True

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_test_branch):
                pass

            @pytest.fixture(scope="session")
            def neon_branch(_neon_test_branch, neon_apply_migrations):
                branch, is_creator = _neon_test_branch
                yield branch

            def pytest_sessionfinish(session, exitstatus):
                # Should only create ONE branch for entire session
                assert branch_create_count[0] == 1, f"Created {branch_create_count[0]} branches"
        """
        )

        pytester.makepyfile(
            test_module_a="""
            branch_ids_seen = []

            def test_first(neon_branch):
                branch_ids_seen.append(neon_branch.branch_id)

            def test_second(neon_branch):
                branch_ids_seen.append(neon_branch.branch_id)
                # All tests see same branch
                assert len(set(branch_ids_seen)) == 1
        """,
            test_module_b="""
            def test_in_another_module(neon_branch):
                # Same branch as module_a
                assert neon_branch.branch_id == "br-1"
        """,
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=3)
