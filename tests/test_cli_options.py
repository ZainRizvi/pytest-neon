"""Tests for CLI option handling."""


class TestCliOptionPassing:
    """Test that CLI options are correctly passed to fixtures."""

    def test_keep_branches_flag_accessible(self, pytester):
        """Test that --neon-keep-branches flag is accessible in fixture."""
        pytester.makepyfile(
            """
            def test_keep_branches_accessible(request):
                value = request.config.getoption("neon_keep_branches")
                assert value is True
            """
        )

        result = pytester.runpytest("-v", "--neon-keep-branches")
        result.assert_outcomes(passed=1)
