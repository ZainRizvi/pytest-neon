"""Tests for convenience fixture error messages when dependencies are missing."""


class TestMissingDependencyErrors:
    """Test that missing optional deps produce clear, actionable error messages."""

    def test_missing_dependency_shows_install_command(
        self, pytester, mock_neon_branch_fixture_code
    ):
        """Test that missing optional dependency shows how to fix it."""
        pytester.makeconftest(
            mock_neon_branch_fixture_code
            + """
import sys
sys.modules['psycopg2'] = None
"""
        )

        pytester.makepyfile(
            """
            def test_uses_connection(neon_connection):
                pass
            """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(errors=1)
        # Should show install command and suggest neon_branch alternative
        result.stdout.fnmatch_lines(["*pip install pytest-neon*"])
        result.stdout.fnmatch_lines(["*neon_branch*"])
