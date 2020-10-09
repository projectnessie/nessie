#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `pynessie` package."""
import itertools

import pytest
import requests_mock
import simplejson as json
from click.testing import CliRunner

from pynessie import __version__
from pynessie import cli
from pynessie.model import ReferenceSchema


def test_command_line_interface(requests_mock: requests_mock) -> None:
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
    requests_mock.get(
        "http://localhost:19120/api/v1/trees",
        text=json.dumps([{"name": "main", "type": "BRANCH", "hash": "1234567890abcdef"}]),
    )
    help_result = runner.invoke(cli.cli, ["branch", "-l", "--json"])
    assert help_result.exit_code == 0
    references = ReferenceSchema().loads(help_result.output, many=True)
    assert len(references) == 1
    assert references[0].name == "main"
    assert references[0].kind == "BRANCH"
    assert references[0].hash_ == "1234567890abcdef"


def test_config_options() -> None:
    """Ensure config cli option is consistent."""
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["config"])
    assert result.exit_code == 0
    assert "Usage: cli" in result.output
    vars = ["--add x", "--get x", "--list", "--unset x"]
    for i in itertools.permutations(vars, 2):
        result = runner.invoke(cli.cli, ["config"] + [*i[0].split(" "), *i[1].split(" ")])
        assert result.exit_code == 2
        assert "Error: Illegal usage: " in result.output

    result = runner.invoke(cli.cli, ["config", "x", "--add", "x"])
    assert result.exit_code == 0


@pytest.mark.e2e
def test_all_help_options() -> None:
    """Write out all help options to std out."""
    runner = CliRunner()
    args = ["", "config", "branch", "tag", "remote", "log", "merge", "cherry-pick"]
    all_args = "\n"
    for i in args:
        result = runner.invoke(cli.cli, [x for x in [i] if x] + ["--help"])
        assert result.exit_code == 0
        all_args += result.output
        all_args += "\n\n\n"
    print(all_args)


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
    help_result = runner.invoke(cli.cli, ["branch", "-l", "--json"])
    assert help_result.exit_code == 0
    branches = ReferenceSchema().loads(help_result.output, many=True)
    assert len(branches) == 1
    assert branches[0].name == "main"
