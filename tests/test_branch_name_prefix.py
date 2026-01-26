"""Tests for branch name prefix configuration."""

import os
from unittest.mock import MagicMock, patch

from neon_api.schema import EndpointState


class TestBranchNamePrefix:
    """Tests for the neon_branch_name_prefix option."""

    def test_cli_option_registered(self, pytester):
        """Branch name prefix CLI option is registered."""
        result = pytester.runpytest("--help")
        result.stdout.fnmatch_lines(["*--neon-branch-name-prefix*"])

    def test_ini_option_registered(self, pytester):
        """Branch name prefix ini option is registered."""
        result = pytester.runpytest("--help")
        result.stdout.fnmatch_lines(["*neon_branch_name_prefix*"])


class TestBranchNameGeneration:
    """Tests for branch name generation with prefix."""

    def test_branch_name_with_prefix(self):
        """Branch name includes prefix when configured."""
        from pytest_neon.plugin import _create_neon_branch

        mock_request = MagicMock()
        mock_config = MagicMock()
        mock_request.config = mock_config

        # Configure mock to return values
        def mock_getoption(name, default=None):
            if name == "neon_api_key":
                return "test-api-key"
            if name == "neon_project_id":
                return "test-project"
            if name == "neon_branch_name_prefix":
                return "myproject"
            if name == "neon_keep_branches":
                return True  # So we don't try to delete
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
            if name == "neon_branch_name_prefix":
                return None
            return None

        mock_config.getoption.side_effect = mock_getoption
        mock_config.getini.side_effect = mock_getini

        # Mock the NeonAPI and capture the branch name
        with patch("pytest_neon.plugin.NeonAPI") as mock_neon_cls:
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

            # Create the generator and advance it
            gen = _create_neon_branch(mock_request, branch_name_suffix="-test")
            try:
                next(gen)
            except StopIteration:
                pass

            # Verify the branch name format
            assert captured_branch_name is not None
            assert captured_branch_name.startswith("pytest-myproject-")
            assert captured_branch_name.endswith("-test")
            # Format: pytest-myproject-[8 hex chars]-test
            parts = captured_branch_name.split("-")
            # ['pytest', 'myproject', 'hexhexhe', 'test']
            assert len(parts) == 4
            assert len(parts[2]) == 4  # 2 bytes = 4 hex chars

    def test_branch_name_prefix_truncated(self):
        """Branch name prefix is truncated to 15 characters."""
        from pytest_neon.plugin import _create_neon_branch

        mock_request = MagicMock()
        mock_config = MagicMock()
        mock_request.config = mock_config

        def mock_getoption(name, default=None):
            if name == "neon_api_key":
                return "test-api-key"
            if name == "neon_project_id":
                return "test-project"
            if name == "neon_branch_name_prefix":
                return "verylongprojectnamethatneedstruncation"
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
            if name == "neon_branch_name_prefix":
                return None
            return None

        mock_config.getoption.side_effect = mock_getoption
        mock_config.getini.side_effect = mock_getini

        with patch("pytest_neon.plugin.NeonAPI") as mock_neon_cls:
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
            try:
                next(gen)
            except StopIteration:
                pass

            assert captured_branch_name is not None
            # First 15 chars of "verylongprojectnamethatneedstruncation" = "verylongproject"
            assert captured_branch_name.startswith("pytest-verylongproject-")
            assert captured_branch_name.endswith("-migrated")

    def test_branch_name_without_prefix(self):
        """Branch name uses original format when no prefix configured."""
        from pytest_neon.plugin import _create_neon_branch

        mock_request = MagicMock()
        mock_config = MagicMock()
        mock_request.config = mock_config

        def mock_getoption(name, default=None):
            if name == "neon_api_key":
                return "test-api-key"
            if name == "neon_project_id":
                return "test-project"
            if name == "neon_branch_name_prefix":
                return None  # No prefix
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
            if name == "neon_branch_name_prefix":
                return None
            return None

        mock_config.getoption.side_effect = mock_getoption
        mock_config.getini.side_effect = mock_getini

        with patch("pytest_neon.plugin.NeonAPI") as mock_neon_cls:
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
            try:
                next(gen)
            except StopIteration:
                pass

            assert captured_branch_name is not None
            # Without prefix: pytest-[8 hex chars]-migrated
            assert captured_branch_name.startswith("pytest-")
            assert captured_branch_name.endswith("-migrated")
            # Format: pytest-hexhexhe-migrated
            parts = captured_branch_name.split("-")
            # ['pytest', 'hexhexhe', 'migrated']
            assert len(parts) == 3
            assert len(parts[1]) == 4  # 2 bytes = 4 hex chars

    def test_env_var_for_prefix(self):
        """Branch name prefix can be set via environment variable."""
        from pytest_neon.plugin import _create_neon_branch

        mock_request = MagicMock()
        mock_config = MagicMock()
        mock_request.config = mock_config

        def mock_getoption(name, default=None):
            if name == "neon_api_key":
                return "test-api-key"
            if name == "neon_project_id":
                return "test-project"
            if name == "neon_branch_name_prefix":
                return None  # Not set via CLI
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
            if name == "neon_branch_name_prefix":
                return None  # Not set via ini
            return None

        mock_config.getoption.side_effect = mock_getoption
        mock_config.getini.side_effect = mock_getini

        # Set the env var
        old_env = os.environ.get("NEON_BRANCH_NAME_PREFIX")
        os.environ["NEON_BRANCH_NAME_PREFIX"] = "envprefix"

        try:
            with patch("pytest_neon.plugin.NeonAPI") as mock_neon_cls:
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

                gen = _create_neon_branch(mock_request, branch_name_suffix="-test")
                try:
                    next(gen)
                except StopIteration:
                    pass

                assert captured_branch_name is not None
                assert captured_branch_name.startswith("pytest-envprefix-")
        finally:
            if old_env is None:
                os.environ.pop("NEON_BRANCH_NAME_PREFIX", None)
            else:
                os.environ["NEON_BRANCH_NAME_PREFIX"] = old_env
