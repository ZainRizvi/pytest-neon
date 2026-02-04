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


class TestXdistBranchSharing:
    """Test that parallel workers share the same branch."""

    def test_xdist_workers_share_branch(self, pytester, monkeypatch):
        """Xdist workers should share ONE branch."""
        monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")

        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch, _get_xdist_worker_id

            branch_creation_calls = []

            @pytest.fixture(scope="session")
            def _neon_test_branch(request):
                worker_id = _get_xdist_worker_id()

                # In simplified model, all workers share one branch
                branch_creation_calls.append(f"worker-{worker_id}")

                return (NeonBranch(
                    branch_id="br-shared",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@shared.neon.tech/mockdb",
                    host="shared.neon.tech",
                    parent_id="br-parent-000",
                ), True)  # is_creator tuple

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_test_branch):
                pass

            @pytest.fixture(scope="session")
            def neon_branch(_neon_test_branch, neon_apply_migrations):
                branch, is_creator = _neon_test_branch
                return branch

            @pytest.fixture(scope="session", autouse=True)
            def verify_branch_shared():
                yield
                # Only one branch should be created across all workers
                assert len(branch_creation_calls) == 1
            """
        )

        pytester.makepyfile(
            """
            def test_xdist_shares_branch(neon_branch):
                # All workers should see the same branch
                assert neon_branch.branch_id == "br-shared"
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)

    def test_non_xdist_creates_branch(self, pytester, monkeypatch):
        """Without xdist, should still create a branch."""
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)

        pytester.makeconftest(
            """
            import os
            import pytest
            from pytest_neon.plugin import NeonBranch, _get_xdist_worker_id

            branch_creation_calls = []

            @pytest.fixture(scope="session")
            def _neon_test_branch(request):
                worker_id = _get_xdist_worker_id()
                branch_creation_calls.append(f"worker-{worker_id}")

                return (NeonBranch(
                    branch_id="br-test",
                    project_id="proj-mock",
                    connection_string="postgresql://mock:mock@test.neon.tech/mockdb",
                    host="test.neon.tech",
                    parent_id="br-parent-000",
                ), True)

            @pytest.fixture(scope="session")
            def neon_apply_migrations(_neon_test_branch):
                pass

            @pytest.fixture(scope="session")
            def neon_branch(_neon_test_branch, neon_apply_migrations):
                branch, is_creator = _neon_test_branch
                return branch

            @pytest.fixture(scope="session", autouse=True)
            def verify_branch_created():
                yield
                # Should create branch for main process
                assert branch_creation_calls == ["worker-main"]
            """
        )

        pytester.makepyfile(
            """
            def test_no_xdist_creates_branch(neon_branch):
                assert neon_branch is not None
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=1)
