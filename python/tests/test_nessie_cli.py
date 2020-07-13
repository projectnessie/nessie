#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `nessie_client` package."""
import pytest
import requests_mock
import simplejson as json
from click.testing import CliRunner

from nessie_client import __version__
from nessie_client import cli
from nessie_client.model import BranchSchema


def test_command_line_interface(requests_mock: requests_mock) -> None:
    """Test the CLI."""
    runner = CliRunner()
    requests_mock.post("http://localhost:19120/api/v1/login", text=json.dumps({"token": "12345"}))
    result = runner.invoke(cli.cli)
    assert result.exit_code == 0
    assert "Usage: cli" in result.output
    help_result = runner.invoke(cli.cli, ["--help"])
    assert help_result.exit_code == 0
    assert "Usage: cli" in help_result.output
    help_result = runner.invoke(cli.cli, ["--version"])
    assert help_result.exit_code == 0
    assert __version__ in help_result.output
    requests_mock.post("http://localhost:19120/api/v1/login", text=json.dumps({"token": "12345"}))
    requests_mock.get("http://localhost:19120/api/v1/objects", text=json.dumps([{"name": "master", "id": "hash"}]))
    help_result = runner.invoke(cli.cli, ["list-branches"])
    assert help_result.exit_code == 0
    branches = BranchSchema().loads(help_result.output, many=True)
    assert len(branches) == 1
    assert branches[0].name == "master"
    assert branches[0].id == "hash"


@pytest.mark.e2e
def test_command_line_interface_e2e() -> None:
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.cli)
    assert result.exit_code == 0
    assert "Usage: cli" in result.output
    help_result = runner.invoke(cli.cli, ["--help"])
    assert help_result.exit_code == 0
    assert "Usage: cli" in help_result.output
    help_result = runner.invoke(cli.cli, ["--version"])
    assert help_result.exit_code == 0
    assert __version__ in help_result.output
    help_result = runner.invoke(cli.cli, ["list-branches"])
    assert help_result.exit_code == 0
    branches = BranchSchema().loads(help_result.output, many=True)
    assert len(branches) == 1
    assert branches[0].name == "master"
