"""Tests for default branch safety check."""

from unittest.mock import MagicMock

from pytest_neon.plugin import _get_default_branch_id


class TestGetDefaultBranchId:
    """Test _get_default_branch_id helper function."""

    def test_returns_default_branch_id(self):
        """Test that default branch ID is returned when found."""
        mock_neon = MagicMock()
        mock_branch_default = MagicMock()
        mock_branch_default.id = "br-default-123"
        mock_branch_default.default = True
        mock_branch_default.primary = False

        mock_branch_other = MagicMock()
        mock_branch_other.id = "br-other-456"
        mock_branch_other.default = False
        mock_branch_other.primary = False

        mock_response = MagicMock()
        mock_response.branches = [mock_branch_other, mock_branch_default]
        mock_neon.branches.return_value = mock_response

        result = _get_default_branch_id(mock_neon, "proj-123")
        assert result == "br-default-123"

    def test_returns_primary_branch_id_as_fallback(self):
        """Test that primary branch ID is returned when default flag not set."""
        mock_neon = MagicMock()
        mock_branch_primary = MagicMock()
        mock_branch_primary.id = "br-primary-123"
        mock_branch_primary.default = False
        mock_branch_primary.primary = True

        mock_response = MagicMock()
        mock_response.branches = [mock_branch_primary]
        mock_neon.branches.return_value = mock_response

        result = _get_default_branch_id(mock_neon, "proj-123")
        assert result == "br-primary-123"

    def test_returns_none_when_no_default_or_primary(self):
        """Test that None is returned when no default/primary branch exists."""
        mock_neon = MagicMock()
        mock_branch = MagicMock()
        mock_branch.id = "br-other-123"
        mock_branch.default = False
        mock_branch.primary = False

        mock_response = MagicMock()
        mock_response.branches = [mock_branch]
        mock_neon.branches.return_value = mock_response

        result = _get_default_branch_id(mock_neon, "proj-123")
        assert result is None

    def test_returns_none_on_api_error(self):
        """Test that None is returned on API failure so tests can still run."""
        mock_neon = MagicMock()
        mock_neon.branches.side_effect = Exception("API error")

        result = _get_default_branch_id(mock_neon, "proj-123")
        assert result is None

    def test_handles_missing_attributes_gracefully(self):
        """Test graceful handling when branch object lacks expected attributes."""
        mock_neon = MagicMock()
        # Branch with no default/primary attributes at all
        mock_branch = MagicMock(spec=["id"])
        mock_branch.id = "br-123"

        mock_response = MagicMock()
        mock_response.branches = [mock_branch]
        mock_neon.branches.return_value = mock_response

        # Should not raise, getattr with default handles this
        result = _get_default_branch_id(mock_neon, "proj-123")
        assert result is None
