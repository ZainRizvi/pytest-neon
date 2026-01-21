"""Integration tests that run against real Neon API.

These tests are skipped unless NEON_API_KEY and NEON_PROJECT_ID are set.
They create actual branches and verify the full workflow.
"""

import os

import pytest

# Skip all tests in this module if credentials not available
pytestmark = pytest.mark.skipif(
    not os.environ.get("NEON_API_KEY") or not os.environ.get("NEON_PROJECT_ID"),
    reason="NEON_API_KEY and NEON_PROJECT_ID required for integration tests",
)


class TestRealNeonIntegration:
    """Test actual branch creation and database connectivity."""

    def test_branch_created_with_valid_connection(self, neon_branch):
        """Test that a real branch is created and has valid connection info."""
        assert neon_branch.branch_id.startswith("br-")
        assert neon_branch.project_id
        assert "neon.tech" in neon_branch.host
        assert neon_branch.connection_string.startswith("postgresql://")

    def test_can_execute_query(self, neon_branch):
        """Test that we can actually connect and run queries."""
        try:
            import psycopg
        except ImportError:
            pytest.skip("psycopg not installed")

        with psycopg.connect(neon_branch.connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS result")
                assert cur.fetchone()[0] == 1
