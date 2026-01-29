"""Tests for service classes - EnvironmentManager and XdistCoordinator.

These test the service classes in isolation. The NeonBranchManager is better
tested through the existing pytester-based integration tests since it primarily
wraps the Neon API.
"""

import os
from unittest.mock import MagicMock, patch

from pytest_neon.plugin import EnvironmentManager, XdistCoordinator


class TestEnvironmentManager:
    """Test EnvironmentManager class."""

    def test_set_and_restore_with_existing_value(self):
        """set() and restore() correctly handle an existing env var."""
        env_manager = EnvironmentManager("TEST_VAR")

        with patch.dict(os.environ, {"TEST_VAR": "original"}, clear=False):
            env_manager.set("new_value")
            assert os.environ["TEST_VAR"] == "new_value"

            env_manager.restore()
            assert os.environ["TEST_VAR"] == "original"

    def test_set_and_restore_without_existing_value(self):
        """set() and restore() correctly handle a missing env var."""
        env_manager = EnvironmentManager("NONEXISTENT_VAR")

        with patch.dict(os.environ, {}, clear=True):
            env_manager.set("new_value")
            assert os.environ["NONEXISTENT_VAR"] == "new_value"

            env_manager.restore()
            assert "NONEXISTENT_VAR" not in os.environ

    def test_temporary_context_manager(self):
        """temporary() works as a context manager."""
        env_manager = EnvironmentManager("TEST_VAR")

        with patch.dict(os.environ, {"TEST_VAR": "original"}, clear=False):
            with env_manager.temporary("temporary_value"):
                assert os.environ["TEST_VAR"] == "temporary_value"

            assert os.environ["TEST_VAR"] == "original"


class TestXdistCoordinator:
    """Test XdistCoordinator class."""

    def test_detects_non_xdist_mode(self):
        """Correctly identifies non-xdist mode."""
        mock_tmp_path_factory = MagicMock()

        with patch.dict(os.environ, {}, clear=True):
            coordinator = XdistCoordinator(mock_tmp_path_factory)

        assert coordinator.worker_id == "main"
        assert coordinator.is_xdist is False

    def test_detects_xdist_mode(self):
        """Correctly identifies xdist mode."""
        mock_tmp_path_factory = MagicMock()
        mock_tmp_path_factory.getbasetemp.return_value.parent = "/tmp/pytest-xdist"

        with patch.dict(os.environ, {"PYTEST_XDIST_WORKER": "gw0"}, clear=False):
            coordinator = XdistCoordinator(mock_tmp_path_factory)

        assert coordinator.worker_id == "gw0"
        assert coordinator.is_xdist is True

    def test_coordinate_resource_without_xdist(self):
        """coordinate_resource() calls create_fn directly without xdist."""
        mock_tmp_path_factory = MagicMock()

        with patch.dict(os.environ, {}, clear=True):
            coordinator = XdistCoordinator(mock_tmp_path_factory)

        create_fn = MagicMock(return_value={"key": "value"})
        data, is_creator = coordinator.coordinate_resource("test", create_fn)

        assert data == {"key": "value"}
        assert is_creator is True
        create_fn.assert_called_once()

    def test_coordinate_resource_caches_with_xdist(self, tmp_path):
        """coordinate_resource() uses file cache with xdist."""
        mock_tmp_path_factory = MagicMock()
        mock_tmp_path_factory.getbasetemp.return_value.parent = tmp_path

        with patch.dict(os.environ, {"PYTEST_XDIST_WORKER": "gw0"}, clear=False):
            coordinator = XdistCoordinator(mock_tmp_path_factory)

        # First call creates
        create_fn = MagicMock(return_value={"key": "value"})
        data1, is_creator1 = coordinator.coordinate_resource("resource", create_fn)
        assert is_creator1 is True
        create_fn.assert_called_once()

        # Second call reuses cache
        create_fn2 = MagicMock(return_value={"key": "different"})
        data2, is_creator2 = coordinator.coordinate_resource("resource", create_fn2)
        assert data2 == {"key": "value"}  # From cache, not create_fn2
        assert is_creator2 is False
        create_fn2.assert_not_called()

    def test_signal_coordination(self, tmp_path):
        """send_signal() and wait_for_signal() work together."""
        mock_tmp_path_factory = MagicMock()
        mock_tmp_path_factory.getbasetemp.return_value.parent = tmp_path

        with patch.dict(os.environ, {"PYTEST_XDIST_WORKER": "gw0"}, clear=False):
            coordinator = XdistCoordinator(mock_tmp_path_factory)

        coordinator.send_signal("migrations_done")
        # Should not raise (signal exists)
        coordinator.wait_for_signal("migrations_done", timeout=1)
