"""Tests for git branch name in Neon branch names."""

import contextlib
from unittest.mock import MagicMock, patch

from neon_api.schema import EndpointState


class TestSanitizeBranchName:
    """Tests for _sanitize_branch_name helper."""

    def test_replaces_slashes(self):
        """Replaces forward slashes with hyphens."""
        from pytest_neon.plugin import _sanitize_branch_name

        assert _sanitize_branch_name("feature/my-branch") == "feature-my-branch"

    def test_replaces_multiple_special_chars(self):
        """Replaces various special characters with hyphens."""
        from pytest_neon.plugin import _sanitize_branch_name

        assert _sanitize_branch_name("feat@user#123") == "feat-user-123"

    def test_collapses_multiple_hyphens(self):
        """Collapses multiple consecutive hyphens into one."""
        from pytest_neon.plugin import _sanitize_branch_name

        assert _sanitize_branch_name("feature//branch") == "feature-branch"
        assert _sanitize_branch_name("a---b") == "a-b"

    def test_strips_leading_trailing_hyphens(self):
        """Removes leading and trailing hyphens."""
        from pytest_neon.plugin import _sanitize_branch_name

        assert _sanitize_branch_name("/feature/") == "feature"
        assert _sanitize_branch_name("--branch--") == "branch"

    def test_preserves_valid_chars(self):
        """Preserves alphanumeric chars, hyphens, and underscores."""
        from pytest_neon.plugin import _sanitize_branch_name

        assert _sanitize_branch_name("my-branch_v1") == "my-branch_v1"

    def test_replaces_dots(self):
        """Replaces dots with hyphens."""
        from pytest_neon.plugin import _sanitize_branch_name

        assert _sanitize_branch_name("v1.0.0") == "v1-0-0"

    def test_replaces_non_ascii(self):
        """Replaces non-ASCII characters with hyphens."""
        from pytest_neon.plugin import _sanitize_branch_name

        assert _sanitize_branch_name("feature-über") == "feature-ber"
        assert _sanitize_branch_name("日本語branch") == "branch"
        assert _sanitize_branch_name("test™") == "test"


class TestGetGitBranchName:
    """Tests for _get_git_branch_name helper."""

    def test_returns_branch_name_in_git_repo(self):
        """Returns git branch name when in a git repo."""
        from pytest_neon.plugin import _get_git_branch_name

        # We're running in a git repo, so this should return something
        result = _get_git_branch_name()
        assert result is not None
        assert isinstance(result, str)
        assert len(result) > 0

    def test_returns_none_when_git_fails(self):
        """Returns None when git command fails."""
        from pytest_neon.plugin import _get_git_branch_name

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="")
            result = _get_git_branch_name()
            assert result is None

    def test_returns_none_when_git_not_found(self):
        """Returns None when git is not installed."""
        from pytest_neon.plugin import _get_git_branch_name

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError()
            result = _get_git_branch_name()
            assert result is None

    def test_returns_none_on_timeout(self):
        """Returns None when git command times out."""
        import subprocess

        from pytest_neon.plugin import _get_git_branch_name

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="git", timeout=5)
            result = _get_git_branch_name()
            assert result is None


class TestBranchNameWithGitBranch:
    """Tests for branch name generation with git branch."""

    def test_branch_name_includes_git_branch(self):
        """Branch name includes git branch when in a repo."""
        from pytest_neon.plugin import _create_neon_branch

        mock_request = MagicMock()
        mock_config = MagicMock()
        mock_request.config = mock_config

        def mock_getoption(name, default=None):
            if name == "neon_api_key":
                return "test-api-key"
            if name == "neon_project_id":
                return "test-project"
            if name == "neon_keep_branches":
                return True
            if name == "neon_branch_expiry":
                return 0
            return default

        def mock_getini(name):
            if name == "neon_database":
                return "neondb"
            if name == "neon_role":
                return "neondb_owner"
            if name == "neon_env_var":
                return "DATABASE_URL"
            return None

        mock_config.getoption.side_effect = mock_getoption
        mock_config.getini.side_effect = mock_getini

        with (
            patch("pytest_neon.plugin.NeonAPI") as mock_neon_cls,
            patch("pytest_neon.plugin._get_git_branch_name") as mock_git,
        ):
            # _get_git_branch_name returns sanitized value (slashes -> hyphens)
            mock_git.return_value = "feature-my-branch"

            mock_api = MagicMock()
            mock_neon_cls.return_value = mock_api

            captured_branch_name = None

            def capture_branch_create(**kwargs):
                nonlocal captured_branch_name
                branch_config = kwargs.get("branch", {})
                captured_branch_name = branch_config.get("name")

                mock_result = MagicMock()
                mock_result.branch.id = "test-branch-id"
                mock_result.branch.parent_id = "parent-id"
                mock_result.operations = [MagicMock(endpoint_id="ep-123")]
                return mock_result

            mock_api.branch_create.side_effect = capture_branch_create

            mock_endpoint_response = MagicMock()
            mock_endpoint_response.endpoint.current_state = EndpointState.active
            mock_endpoint_response.endpoint.host = "test.neon.tech"
            mock_api.endpoint.return_value = mock_endpoint_response

            mock_password = MagicMock()
            mock_password.role.password = "testpass"
            mock_api.role_password_reset.return_value = mock_password

            gen = _create_neon_branch(mock_request, branch_name_suffix="-migrated")
            with contextlib.suppress(StopIteration):
                next(gen)

            assert captured_branch_name is not None
            # Git branch "feature/my-branch" sanitized to "feature-my-branch"
            assert captured_branch_name.startswith("pytest-feature-my-bran-")
            assert captured_branch_name.endswith("-migrated")

    def test_branch_name_truncates_long_git_branch(self):
        """Git branch name is truncated to 15 characters."""
        from pytest_neon.plugin import _create_neon_branch

        mock_request = MagicMock()
        mock_config = MagicMock()
        mock_request.config = mock_config

        def mock_getoption(name, default=None):
            if name == "neon_api_key":
                return "test-api-key"
            if name == "neon_project_id":
                return "test-project"
            if name == "neon_keep_branches":
                return True
            if name == "neon_branch_expiry":
                return 0
            return default

        def mock_getini(name):
            if name == "neon_database":
                return "neondb"
            if name == "neon_role":
                return "neondb_owner"
            if name == "neon_env_var":
                return "DATABASE_URL"
            return None

        mock_config.getoption.side_effect = mock_getoption
        mock_config.getini.side_effect = mock_getini

        with (
            patch("pytest_neon.plugin.NeonAPI") as mock_neon_cls,
            patch("pytest_neon.plugin._get_git_branch_name") as mock_git,
        ):
            # _get_git_branch_name returns sanitized value
            mock_git.return_value = "feature-very-long-branch-name-truncated"

            mock_api = MagicMock()
            mock_neon_cls.return_value = mock_api

            captured_branch_name = None

            def capture_branch_create(**kwargs):
                nonlocal captured_branch_name
                branch_config = kwargs.get("branch", {})
                captured_branch_name = branch_config.get("name")

                mock_result = MagicMock()
                mock_result.branch.id = "test-branch-id"
                mock_result.branch.parent_id = "parent-id"
                mock_result.operations = [MagicMock(endpoint_id="ep-123")]
                return mock_result

            mock_api.branch_create.side_effect = capture_branch_create

            mock_endpoint_response = MagicMock()
            mock_endpoint_response.endpoint.current_state = EndpointState.active
            mock_endpoint_response.endpoint.host = "test.neon.tech"
            mock_api.endpoint.return_value = mock_endpoint_response

            mock_password = MagicMock()
            mock_password.role.password = "testpass"
            mock_api.role_password_reset.return_value = mock_password

            gen = _create_neon_branch(mock_request, branch_name_suffix="-migrated")
            with contextlib.suppress(StopIteration):
                next(gen)

            assert captured_branch_name is not None
            # Long branch sanitized and truncated to first 15 chars
            assert captured_branch_name.startswith("pytest-feature-very-lo-")
            assert captured_branch_name.endswith("-migrated")

    def test_branch_name_without_git(self):
        """Branch name uses old format when not in a git repo."""
        from pytest_neon.plugin import _create_neon_branch

        mock_request = MagicMock()
        mock_config = MagicMock()
        mock_request.config = mock_config

        def mock_getoption(name, default=None):
            if name == "neon_api_key":
                return "test-api-key"
            if name == "neon_project_id":
                return "test-project"
            if name == "neon_keep_branches":
                return True
            if name == "neon_branch_expiry":
                return 0
            return default

        def mock_getini(name):
            if name == "neon_database":
                return "neondb"
            if name == "neon_role":
                return "neondb_owner"
            if name == "neon_env_var":
                return "DATABASE_URL"
            return None

        mock_config.getoption.side_effect = mock_getoption
        mock_config.getini.side_effect = mock_getini

        with (
            patch("pytest_neon.plugin.NeonAPI") as mock_neon_cls,
            patch("pytest_neon.plugin._get_git_branch_name") as mock_git,
        ):
            mock_git.return_value = None  # Not in a git repo

            mock_api = MagicMock()
            mock_neon_cls.return_value = mock_api

            captured_branch_name = None

            def capture_branch_create(**kwargs):
                nonlocal captured_branch_name
                branch_config = kwargs.get("branch", {})
                captured_branch_name = branch_config.get("name")

                mock_result = MagicMock()
                mock_result.branch.id = "test-branch-id"
                mock_result.branch.parent_id = "parent-id"
                mock_result.operations = [MagicMock(endpoint_id="ep-123")]
                return mock_result

            mock_api.branch_create.side_effect = capture_branch_create

            mock_endpoint_response = MagicMock()
            mock_endpoint_response.endpoint.current_state = EndpointState.active
            mock_endpoint_response.endpoint.host = "test.neon.tech"
            mock_api.endpoint.return_value = mock_endpoint_response

            mock_password = MagicMock()
            mock_password.role.password = "testpass"
            mock_api.role_password_reset.return_value = mock_password

            gen = _create_neon_branch(mock_request, branch_name_suffix="-migrated")
            with contextlib.suppress(StopIteration):
                next(gen)

            assert captured_branch_name is not None
            # Without git: pytest-[4 hex chars]-migrated
            assert captured_branch_name.startswith("pytest-")
            assert captured_branch_name.endswith("-migrated")
            # Format: pytest-abcd-migrated (no git branch in the middle)
            parts = captured_branch_name.split("-")
            assert len(parts) == 3  # ['pytest', 'abcd', 'migrated']
            assert len(parts[1]) == 4  # 2 bytes = 4 hex chars
