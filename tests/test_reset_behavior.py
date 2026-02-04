"""Tests for rate limit retry behavior."""

import pytest
import requests

from pytest_neon.plugin import (
    NeonRateLimitError,
    _calculate_retry_delay,
    _is_rate_limit_error,
    _retry_on_rate_limit,
)


class TestRateLimitRetryHelper:
    """Test the rate limit retry helper function."""

    def test_succeeds_on_first_try(self):
        """Verify no retry when operation succeeds immediately."""
        call_count = [0]

        def operation():
            call_count[0] += 1
            return "success"

        result = _retry_on_rate_limit(operation, "test_operation")
        assert result == "success"
        assert call_count[0] == 1

    def test_retries_on_429_and_succeeds(self, monkeypatch):
        """Verify retry on 429 error and eventual success."""
        # Mock time.sleep to avoid actual delays
        sleep_calls = []
        monkeypatch.setattr(
            "pytest_neon.plugin.time.sleep", lambda x: sleep_calls.append(x)
        )
        # Mock random for deterministic jitter
        monkeypatch.setattr("pytest_neon.plugin.random.random", lambda: 0.5)

        call_count = [0]

        def operation():
            call_count[0] += 1
            if call_count[0] < 3:
                # Simulate 429 error
                response = requests.Response()
                response.status_code = 429
                error = requests.HTTPError(response=response)
                raise error
            return "success"

        result = _retry_on_rate_limit(operation, "test_operation")
        assert result == "success"
        assert call_count[0] == 3  # 2 failures + 1 success
        assert len(sleep_calls) == 2  # 2 retries

    def test_raises_rate_limit_error_when_exhausted(self, monkeypatch):
        """Verify NeonRateLimitError raised when max delay exceeded."""
        # Mock time.sleep to avoid actual delays
        sleep_calls = []
        monkeypatch.setattr(
            "pytest_neon.plugin.time.sleep", lambda x: sleep_calls.append(x)
        )
        # Mock random for deterministic jitter
        monkeypatch.setattr("pytest_neon.plugin.random.random", lambda: 0.5)

        def operation():
            response = requests.Response()
            response.status_code = 429
            error = requests.HTTPError(response=response)
            raise error

        with pytest.raises(NeonRateLimitError) as exc_info:
            _retry_on_rate_limit(
                operation,
                "test_operation",
                base_delay=10.0,
                max_total_delay=25.0,  # Will exhaust after ~2 retries
            )

        assert "Rate limit exceeded" in str(exc_info.value)
        assert "test_operation" in str(exc_info.value)
        assert "api-docs.neon.tech" in str(exc_info.value)

    def test_does_not_retry_on_non_429_http_error(self):
        """Verify non-429 HTTP errors are raised immediately."""
        call_count = [0]

        def operation():
            call_count[0] += 1
            response = requests.Response()
            response.status_code = 500
            error = requests.HTTPError(response=response)
            raise error

        with pytest.raises(requests.HTTPError):
            _retry_on_rate_limit(operation, "test_operation")

        assert call_count[0] == 1  # No retries

    def test_respects_retry_after_header(self, monkeypatch):
        """Verify Retry-After header is used when present."""
        sleep_calls = []
        monkeypatch.setattr(
            "pytest_neon.plugin.time.sleep", lambda x: sleep_calls.append(x)
        )

        call_count = [0]

        def operation():
            call_count[0] += 1
            if call_count[0] < 2:
                response = requests.Response()
                response.status_code = 429
                response.headers["Retry-After"] = "5"
                error = requests.HTTPError(response=response)
                raise error
            return "success"

        result = _retry_on_rate_limit(operation, "test_operation")
        assert result == "success"
        assert sleep_calls == [5.0]  # Used Retry-After value

    def test_retry_after_zero_uses_minimum_delay(self, monkeypatch):
        """Verify Retry-After: 0 uses minimum 0.1s delay to prevent infinite loops."""
        sleep_calls = []
        monkeypatch.setattr(
            "pytest_neon.plugin.time.sleep", lambda x: sleep_calls.append(x)
        )

        call_count = [0]

        def operation():
            call_count[0] += 1
            if call_count[0] < 2:
                response = requests.Response()
                response.status_code = 429
                response.headers["Retry-After"] = "0"
                error = requests.HTTPError(response=response)
                raise error
            return "success"

        result = _retry_on_rate_limit(operation, "test_operation")
        assert result == "success"
        assert sleep_calls == [0.1]  # Minimum delay enforced

    def test_raises_rate_limit_error_when_max_attempts_exhausted(self, monkeypatch):
        """Verify NeonRateLimitError raised when max attempts reached."""
        sleep_calls = []
        monkeypatch.setattr(
            "pytest_neon.plugin.time.sleep", lambda x: sleep_calls.append(x)
        )
        monkeypatch.setattr("pytest_neon.plugin.random.random", lambda: 0.5)

        def operation():
            response = requests.Response()
            response.status_code = 429
            error = requests.HTTPError(response=response)
            raise error

        with pytest.raises(NeonRateLimitError) as exc_info:
            _retry_on_rate_limit(
                operation,
                "test_operation",
                base_delay=0.1,  # Small delay
                max_total_delay=1000.0,  # Large total delay (won't be hit)
                max_attempts=3,  # Should exhaust after 3 attempts
            )

        assert "Max attempts (3) reached" in str(exc_info.value)
        assert len(sleep_calls) == 2  # 3 attempts = 2 sleeps (before retry 2 and 3)


class TestCalculateRetryDelay:
    """Test the delay calculation with exponential backoff and jitter."""

    def test_exponential_backoff(self, monkeypatch):
        """Verify exponential backoff without jitter."""
        # No jitter for deterministic test
        monkeypatch.setattr("pytest_neon.plugin.random.random", lambda: 0.5)

        # With jitter_factor=0.25 and random=0.5, jitter = delay * 0.25 * 0 = 0
        delay0 = _calculate_retry_delay(0, base_delay=4.0, jitter_factor=0.0)
        delay1 = _calculate_retry_delay(1, base_delay=4.0, jitter_factor=0.0)
        delay2 = _calculate_retry_delay(2, base_delay=4.0, jitter_factor=0.0)
        delay3 = _calculate_retry_delay(3, base_delay=4.0, jitter_factor=0.0)

        assert delay0 == 4.0  # 4 * 2^0 = 4
        assert delay1 == 8.0  # 4 * 2^1 = 8
        assert delay2 == 16.0  # 4 * 2^2 = 16
        assert delay3 == 32.0  # 4 * 2^3 = 32

    def test_jitter_adds_randomness(self, monkeypatch):
        """Verify jitter adds randomness to delay."""
        # random() = 1.0 -> jitter = delay * 0.25 * (2*1 - 1) = delay * 0.25
        monkeypatch.setattr("pytest_neon.plugin.random.random", lambda: 1.0)
        delay_high = _calculate_retry_delay(0, base_delay=4.0, jitter_factor=0.25)
        assert delay_high == 5.0  # 4 + 4*0.25 = 5

        # random() = 0.0 -> jitter = delay * 0.25 * (2*0 - 1) = -delay * 0.25
        monkeypatch.setattr("pytest_neon.plugin.random.random", lambda: 0.0)
        delay_low = _calculate_retry_delay(0, base_delay=4.0, jitter_factor=0.25)
        assert delay_low == 3.0  # 4 - 4*0.25 = 3


class TestIsRateLimitError:
    """Test rate limit error detection."""

    def test_detects_429_http_error(self):
        """Verify 429 HTTPError is detected."""
        response = requests.Response()
        response.status_code = 429
        error = requests.HTTPError(response=response)
        assert _is_rate_limit_error(error) is True

    def test_does_not_detect_other_http_errors(self):
        """Verify non-429 HTTPError is not detected as rate limit."""
        response = requests.Response()
        response.status_code = 500
        error = requests.HTTPError(response=response)
        assert _is_rate_limit_error(error) is False

    def test_detects_neon_api_error_with_429(self):
        """Verify NeonAPIError with 429 in message is detected."""
        from neon_api.exceptions import NeonAPIError

        error = NeonAPIError("429 Too Many Requests")
        assert _is_rate_limit_error(error) is True

    def test_detects_neon_api_error_with_rate_limit(self):
        """Verify NeonAPIError with 'rate limit' in message is detected."""
        from neon_api.exceptions import NeonAPIError

        error = NeonAPIError("Rate limit exceeded")
        assert _is_rate_limit_error(error) is True

    def test_detects_neon_api_error_with_too_many_requests(self):
        """Verify NeonAPIError with 'too many requests' in message is detected."""
        from neon_api.exceptions import NeonAPIError

        error = NeonAPIError("Too many requests")
        assert _is_rate_limit_error(error) is True

    def test_does_not_detect_too_many_connections(self):
        """Verify 'too many connections' is NOT detected as rate limit."""
        from neon_api.exceptions import NeonAPIError

        error = NeonAPIError("Too many connections to database")
        assert _is_rate_limit_error(error) is False

    def test_does_not_detect_other_neon_api_errors(self):
        """Verify other NeonAPIError messages are not detected."""
        from neon_api.exceptions import NeonAPIError

        error = NeonAPIError("Internal server error")
        assert _is_rate_limit_error(error) is False
