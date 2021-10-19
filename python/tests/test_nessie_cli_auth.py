# -*- coding: utf-8 -*-
"""Authentication tests for Nessi CLI."""
import pytest

from .conftest import _cli


@pytest.fixture(scope="module")
def vcr_config() -> dict:
    """Configures VCR to also match on HTTP headers."""
    return {"match_on": ["method", "scheme", "host", "port", "path", "query", "headers"]}


@pytest.mark.vcr
def test_bearer_auth() -> None:
    """Test the "remote show" command with bearer authentication."""
    assert "main" in _cli(["--json", "--auth-token", "test_token_123", "remote", "show"])


@pytest.mark.vcr
def test_bearer_auth_invalid() -> None:
    """Test any command with an invalid bearer authentication token."""
    result = _cli(["--json", "--auth-token", "invalid", "remote", "show"], ret_val=1)
    assert "401 Client Error: Unauthorized" in result
