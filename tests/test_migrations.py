"""Tests for migration support."""


class TestMigrationFixtureOrder:
    """Test that migrations run before test branches are created."""

    def test_migrations_run_before_test_branch_created(self, pytester):
        """Verify neon_apply_migrations is called before test branch exists."""
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
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_migration_branch():
                execution_order.append("migration_branch_created")
                branch = FakeNeonBranch(
                    branch_id="br-migration",
                    project_id="proj-test",
                    connection_string="postgresql://migration",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                yield branch

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_migration_branch):
                execution_order.append("migrations_applied")
                # User would run migrations here

            @pytest.fixture(scope="module")
            def _neon_branch_for_reset(_neon_migration_branch, neon_apply_migrations):
                execution_order.append("test_branch_created")
                branch = FakeNeonBranch(
                    branch_id="br-test",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id=_neon_migration_branch.branch_id,
                )
                yield branch

            @pytest.fixture(scope="function")
            def neon_branch(_neon_branch_for_reset):
                yield _neon_branch_for_reset

            def pytest_sessionfinish(session, exitstatus):
                # Verify order: migration branch -> migrations -> test branch
                assert execution_order == [
                    "migration_branch_created",
                    "migrations_applied",
                    "test_branch_created",
                ], f"Wrong order: {execution_order}"
        """
        )

        pytester.makepyfile(
            """
            def test_uses_branch(neon_branch):
                assert neon_branch.parent_id == "br-migration"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)
