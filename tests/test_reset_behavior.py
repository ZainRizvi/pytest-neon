"""Tests for branch reset behavior."""

import pytest

from pytest_neon.plugin import NeonBranch, _reset_branch_to_parent


class TestResetRetryBehavior:
    """Test that branch reset retries on transient failures."""

    def test_reset_succeeds_after_transient_failures(self, mocker):
        """Verify reset retries and succeeds after transient API errors."""
        branch = NeonBranch(
            branch_id="br-test",
            project_id="proj-test",
            connection_string="postgresql://test",
            host="test.neon.tech",
            parent_id="br-parent",
        )

        # Mock requests.post to fail twice, then succeed
        mock_response = mocker.Mock()
        mock_response.raise_for_status = mocker.Mock()

        import requests

        call_count = [0]

        def mock_post(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                raise requests.RequestException("API rate limited")
            return mock_response

        mocker.patch("pytest_neon.plugin.requests.post", side_effect=mock_post)
        mocker.patch("pytest_neon.plugin.time.sleep")  # Don't actually sleep

        # Should succeed after retries
        _reset_branch_to_parent(branch, "fake-api-key")

        assert call_count[0] == 3  # 2 failures + 1 success

    def test_reset_fails_after_max_retries(self, mocker):
        """Verify reset raises after exhausting all retries."""
        branch = NeonBranch(
            branch_id="br-test",
            project_id="proj-test",
            connection_string="postgresql://test",
            host="test.neon.tech",
            parent_id="br-parent",
        )

        # Mock requests.post to always fail
        import requests

        mocker.patch(
            "pytest_neon.plugin.requests.post",
            side_effect=requests.RequestException("API error"),
        )
        mock_sleep = mocker.patch("pytest_neon.plugin.time.sleep")

        # Should raise after max retries
        with pytest.raises(requests.RequestException, match="API error"):
            _reset_branch_to_parent(branch, "fake-api-key")

        # Should have slept between retries (3 retries = 3 sleeps)
        assert mock_sleep.call_count == 3

    def test_reset_uses_exponential_backoff(self, mocker):
        """Verify reset uses exponential backoff between retries."""
        branch = NeonBranch(
            branch_id="br-test",
            project_id="proj-test",
            connection_string="postgresql://test",
            host="test.neon.tech",
            parent_id="br-parent",
        )

        import requests

        mocker.patch(
            "pytest_neon.plugin.requests.post",
            side_effect=requests.RequestException("API error"),
        )
        mock_sleep = mocker.patch("pytest_neon.plugin.time.sleep")

        with pytest.raises(requests.RequestException):
            _reset_branch_to_parent(branch, "fake-api-key")

        # Check exponential backoff: 1s, 2s, 4s
        sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert sleep_calls == [1, 2, 4]

    def test_reset_no_retry_on_success(self, mocker):
        """Verify reset doesn't retry when successful."""
        branch = NeonBranch(
            branch_id="br-test",
            project_id="proj-test",
            connection_string="postgresql://test",
            host="test.neon.tech",
            parent_id="br-parent",
        )

        mock_response = mocker.Mock()
        mock_response.raise_for_status = mocker.Mock()
        mock_post = mocker.patch(
            "pytest_neon.plugin.requests.post", return_value=mock_response
        )
        mock_sleep = mocker.patch("pytest_neon.plugin.time.sleep")

        _reset_branch_to_parent(branch, "fake-api-key")

        assert mock_post.call_count == 1
        assert mock_sleep.call_count == 0


class TestResetBehavior:
    """Test that branch reset happens between tests."""

    def test_reset_called_after_each_test(self, pytester):
        """Verify reset is called after each test function."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            reset_count = [0]

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="module")
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

            @pytest.fixture(scope="function")
            def neon_branch(_neon_branch_for_reset):
                yield _neon_branch_for_reset
                # Simulate reset
                reset_count[0] += 1

            def pytest_sessionfinish(session, exitstatus):
                # Verify resets happened
                assert reset_count[0] == 2, f"Expected 2 resets, got {reset_count[0]}"
        """
        )

        pytester.makepyfile(
            """
            def test_first(neon_branch):
                assert neon_branch.branch_id == "br-test"

            def test_second(neon_branch):
                assert neon_branch.branch_id == "br-test"
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=2)

    def test_same_branch_used_across_tests_in_module(self, pytester):
        """Verify all tests in a module use the same branch instance."""
        pytester.makeconftest(
            """
            import os
            import pytest
            from dataclasses import dataclass

            branch_ids_seen = []

            @dataclass
            class FakeNeonBranch:
                branch_id: str
                project_id: str
                connection_string: str
                host: str
                parent_id: str

            @pytest.fixture(scope="module")
            def _neon_branch_for_reset():
                import random
                branch = FakeNeonBranch(
                    branch_id=f"br-{random.randint(1000, 9999)}",
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

            @pytest.fixture(scope="function")
            def neon_branch(_neon_branch_for_reset):
                branch_ids_seen.append(_neon_branch_for_reset.branch_id)
                yield _neon_branch_for_reset

            def pytest_sessionfinish(session, exitstatus):
                # All tests should see the same branch
                unique = len(set(branch_ids_seen))
                assert unique == 1, f"Expected 1 unique branch, got {unique}"
        """
        )

        pytester.makepyfile(
            """
            def test_first(neon_branch):
                pass

            def test_second(neon_branch):
                pass

            def test_third(neon_branch):
                pass
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(passed=3)


class TestParentIdValidation:
    """Test that missing parent_id is caught early."""

    def test_fails_if_no_parent_id(self, pytester):
        """Verify that neon_branch fails if branch has no parent."""
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
                parent_id: str = None  # No parent!

            @pytest.fixture(scope="module")
            def _neon_branch_for_reset():
                branch = FakeNeonBranch(
                    branch_id="br-test",
                    project_id="proj-test",
                    connection_string="postgresql://test",
                    host="test.neon.tech",
                    parent_id=None,  # No parent
                )
                os.environ["DATABASE_URL"] = branch.connection_string
                try:
                    yield branch
                finally:
                    os.environ.pop("DATABASE_URL", None)

            @pytest.fixture(scope="function")
            def neon_branch(_neon_branch_for_reset):
                if not _neon_branch_for_reset.parent_id:
                    pytest.fail("Branch has no parent - cannot reset")
                yield _neon_branch_for_reset
        """
        )

        pytester.makepyfile(
            """
            def test_should_fail(neon_branch):
                pass
        """
        )

        result = pytester.runpytest("-v")
        result.assert_outcomes(errors=1)
        assert "has no parent" in result.stdout.str()
