# -*- coding: utf-8 -*-
"""Authentication tests for Nessi CLI."""
import pytest
from click.testing import CliRunner

from .test_nessie_cli import _run


@pytest.fixture(scope="module")
def vcr_config() -> dict:
    """Configures VCR to also match on HTTP headers."""
    return {"match_on": ["method", "scheme", "host", "port", "path", "query", "headers"]}


@pytest.mark.vcr
def test_bearer_auth() -> None:
    """Test the "remote show" command with bearer authentication."""
    runner = CliRunner()
    result = _run(runner, ["--json", "--auth-token", "test_token_123", "remote", "show"])
    assert "main" in result.output


@pytest.mark.vcr
def test_bearer_auth_invalid() -> None:
    """Test any command with an invalid bearer authentication token."""
    runner = CliRunner()
    result = _run(runner, ["--json", "--auth-token", "invalid", "remote", "show"], ret_val=1)
    assert "401 Client Error: Unauthorized" in result.output
