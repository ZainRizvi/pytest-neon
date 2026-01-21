"""Tests for skip behavior when credentials are not configured."""


class TestSkipWithoutCredentials:
    """Test that neon_branch skips gracefully when not configured."""

    def test_skips_without_api_key(self, pytester):
        """Test that neon_branch skips when NEON_API_KEY is not set."""
        pytester.makeconftest(
            """
            import os
            os.environ.pop("NEON_API_KEY", None)
            os.environ.pop("NEON_PROJECT_ID", None)
            """
        )

        pytester.makepyfile(
            """
            def test_needs_branch(neon_branch):
                pass
            """
        )

        result = pytester.runpytest("-v", "-rs")
        result.assert_outcomes(skipped=1)
        result.stdout.fnmatch_lines(["*NEON_API_KEY*"])

    def test_skips_without_project_id(self, pytester):
        """Test that neon_branch skips when NEON_PROJECT_ID is not set."""
        pytester.makeconftest(
            """
            import os
            os.environ["NEON_API_KEY"] = "test-key"
            os.environ.pop("NEON_PROJECT_ID", None)
            """
        )

        pytester.makepyfile(
            """
            def test_needs_branch(neon_branch):
                pass
            """
        )

        result = pytester.runpytest("-v", "-rs")
        result.assert_outcomes(skipped=1)
        result.stdout.fnmatch_lines(["*NEON_PROJECT_ID*"])
