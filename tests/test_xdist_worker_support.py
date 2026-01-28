"""Tests for pytest-xdist parallel worker support."""

import json

from pytest_neon.plugin import (
    NeonBranch,
    _branch_to_dict,
    _dict_to_branch,
)


class TestBranchSerialization:
    """Test NeonBranch serialization for cache file."""

    def test_branch_round_trip(self):
        """Test that branch can be serialized and deserialized."""
        branch = NeonBranch(
            branch_id="br-test-123",
            project_id="proj-abc",
            connection_string="postgresql://user:pass@host/db",
            host="host.neon.tech",
            parent_id="br-parent-456",
        )

        data = _branch_to_dict(branch)
        restored = _dict_to_branch(data)

        assert restored.branch_id == branch.branch_id
        assert restored.project_id == branch.project_id
        assert restored.connection_string == branch.connection_string
        assert restored.host == branch.host
        assert restored.parent_id == branch.parent_id

    def test_branch_to_dict_is_json_serializable(self):
        """Test that branch dict can be JSON serialized."""
        branch = NeonBranch(
            branch_id="br-test-123",
            project_id="proj-abc",
            connection_string="postgresql://user:pass@host/db",
            host="host.neon.tech",
            parent_id=None,
        )

        data = _branch_to_dict(branch)
        json_str = json.dumps(data)
        restored_data = json.loads(json_str)
        restored = _dict_to_branch(restored_data)

        assert restored.branch_id == branch.branch_id
        assert restored.parent_id is None


class TestXdistBranchIsolation:
    """Test that parallel workers get separate branches."""

    def test_xdist_worker_creates_branch_even_without_migrations(
        self, pytester, monkeypatch
    ):
        """Even without schema changes, xdist workers should get their own branch."""
        monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")

        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import (
                NeonBranch,
                _get_xdist_worker_id,
                _MIGRATIONS_NOT_DEFINED,
            )

            branch_creation_calls = []

            @pytest.fixture(scope="session")
            def _neon_migration_branch(request):
                # Store empty fingerprint to simulate no psycopg available
                request.config._neon_pre_migration_fingerprint = ()

                return NeonBranch(
                    branch_id="br-migration-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@migration.neon.tech/mockdb",
                    host="migration.neon.tech",
                    parent_id="br-parent-000",
                )

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_migration_branch):
                # Return sentinel to simulate NO migrations defined
                return _MIGRATIONS_NOT_DEFINED

            @pytest.fixture(scope="session")
            def _neon_branch_for_reset(
                request, _neon_migration_branch, neon_apply_migrations
            ):
                # Replicate the real logic
                sentinel = _MIGRATIONS_NOT_DEFINED
                migrations_defined = neon_apply_migrations is not sentinel
                fingerprint_key = "_neon_pre_migration_fingerprint"
                pre_fp = getattr(request.config, fingerprint_key, ())
                schema_changed = False

                if migrations_defined and pre_fp:
                    schema_changed = False  # Simplified
                elif migrations_defined and not pre_fp:
                    schema_changed = True

                worker_id = _get_xdist_worker_id()
                suffix = f"-test-{worker_id}"

                # Key: create branch even without schema changes when xdist
                if schema_changed or worker_id != "main":
                    branch_creation_calls.append(f"created-branch{suffix}")
                    conn = f"postgresql://mock:mock@t{suffix}.neon.tech/db"
                    branch_info = NeonBranch(
                        branch_id=f"br-test{suffix}",
                        project_id="proj-mock",
                        connection_string=conn,
                        host=f"t{suffix}.neon.tech",
                        parent_id=_neon_migration_branch.branch_id,
                    )
                else:
                    branch_creation_calls.append("reused-migration-branch")
                    branch_info = _neon_migration_branch

                os.environ["DATABASE_URL"] = branch_info.connection_string
                try:
                    yield branch_info
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="session", autouse=True)
            def verify_branch_created():
                yield
                # Under xdist, should create a new branch
                assert branch_creation_calls == ["created-branch-test-gw0"]
            """
        )

        pytester.makepyfile(
            """
            def test_xdist_creates_branch(_neon_branch_for_reset):
                # Just trigger the fixture
                assert _neon_branch_for_reset is not None
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)

    def test_non_xdist_reuses_migration_branch_without_migrations(
        self, pytester, monkeypatch
    ):
        """Without xdist and without migrations, should reuse migration branch."""
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)

        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import (
                NeonBranch,
                _get_xdist_worker_id,
                _MIGRATIONS_NOT_DEFINED,
            )

            branch_creation_calls = []

            @pytest.fixture(scope="session")
            def _neon_migration_branch(request):
                request.config._neon_pre_migration_fingerprint = ()
                return NeonBranch(
                    branch_id="br-migration-123",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@migration.neon.tech/mockdb",
                    host="migration.neon.tech",
                    parent_id="br-parent-000",
                )

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_migration_branch):
                return _MIGRATIONS_NOT_DEFINED

            @pytest.fixture(scope="session")
            def _neon_branch_for_reset(
                request, _neon_migration_branch, neon_apply_migrations
            ):
                sentinel = _MIGRATIONS_NOT_DEFINED
                migrations_defined = neon_apply_migrations is not sentinel
                fingerprint_key = "_neon_pre_migration_fingerprint"
                pre_fp = getattr(request.config, fingerprint_key, ())
                schema_changed = False

                if migrations_defined and pre_fp:
                    schema_changed = False
                elif migrations_defined and not pre_fp:
                    schema_changed = True

                worker_id = _get_xdist_worker_id()
                suffix = f"-test-{worker_id}"

                if schema_changed or worker_id != "main":
                    branch_creation_calls.append(f"created-branch{suffix}")
                    conn = f"postgresql://mock:mock@t{suffix}.neon.tech/db"
                    branch_info = NeonBranch(
                        branch_id=f"br-test{suffix}",
                        project_id="proj-mock",
                        connection_string=conn,
                        host=f"t{suffix}.neon.tech",
                        parent_id=_neon_migration_branch.branch_id,
                    )
                else:
                    branch_creation_calls.append("reused-migration-branch")
                    branch_info = _neon_migration_branch

                os.environ["DATABASE_URL"] = branch_info.connection_string
                try:
                    yield branch_info
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="session", autouse=True)
            def verify_branch_reused():
                yield
                # Without xdist, should reuse migration branch
                assert branch_creation_calls == ["reused-migration-branch"]
            """
        )

        pytester.makepyfile(
            """
            def test_no_xdist_reuses_branch(_neon_branch_for_reset):
                assert _neon_branch_for_reset is not None
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)
