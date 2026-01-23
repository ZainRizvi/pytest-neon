"""Tests for migration support."""

import inspect

from pytest_neon.plugin import _MIGRATIONS_NOT_DEFINED, neon_apply_migrations


class TestSmartMigrationDetection:
    """Test the sentinel-based detection for skipping unnecessary branches."""

    def test_sentinel_returned_when_migrations_not_overridden(self):
        """Default neon_apply_migrations returns sentinel to signal no override."""
        # Get the default implementation's return behavior from source
        source = inspect.getsource(neon_apply_migrations)
        assert "_MIGRATIONS_NOT_DEFINED" in source

    def test_sentinel_is_unique_object(self):
        """Sentinel should be a unique object that won't match normal returns."""
        assert _MIGRATIONS_NOT_DEFINED is not None
        assert _MIGRATIONS_NOT_DEFINED is not False
        assert _MIGRATIONS_NOT_DEFINED != ()

    def test_user_override_does_not_return_sentinel(self, pytester):
        """When user overrides neon_apply_migrations, it returns None (not sentinel)."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass
            from pytest_neon.plugin import _MIGRATIONS_NOT_DEFINED

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="session")
            def _neon_migration_branch(request):
                branch = FakeNeonBranch(
                    branch_id="br-migration",
                    project_id="proj-test",
                    connection_string="postgresql://fake",
                    host="test.neon.tech",
                    parent_id="br-parent",
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                request.config._neon_pre_migration_fingerprint = ()
                yield branch

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_migration_branch):
                # User override - returns None implicitly, not the sentinel
                pass

            @pytest.fixture(scope="session")
            def _neon_branch_for_reset(_neon_migration_branch, neon_apply_migrations):
                # Verify the detection logic
                sentinel = _MIGRATIONS_NOT_DEFINED
                migrations_defined = neon_apply_migrations is not sentinel
                assert migrations_defined, "User override should NOT return sentinel"
                yield _neon_migration_branch

            @pytest.fixture(scope="function")
            def neon_branch(_neon_branch_for_reset):
                yield _neon_branch_for_reset
        """
        )

        pytester.makepyfile(
            """
            def test_migration_override_detected(neon_branch):
                assert neon_branch.branch_id == "br-migration"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)


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
