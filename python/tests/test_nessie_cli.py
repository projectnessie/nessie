#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `pynessie` package."""
import itertools
import os
from pathlib import Path

import pytest
import simplejson
from click.testing import CliRunner

from pynessie import __version__
from pynessie import cli
from pynessie.model import Branch
from pynessie.model import ReferenceSchema


@pytest.mark.vcr
def test_command_line_interface() -> None:
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
    help_result = runner.invoke(cli.cli, ["--json", "branch", "-l"])
    assert help_result.exit_code == 0
    references = ReferenceSchema().loads(help_result.output, many=True)
    assert len(references) == 1
    assert references[0].name == "main"
    assert isinstance(references[0], Branch)
    assert references[0].hash_ == "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d"


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


def test_set_unset() -> None:
    """Test config set/unset/list."""
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["config", "--add", "test.data", "123", "--type", "int"])
    assert result.exit_code == 0
    result = runner.invoke(cli.cli, ["config", "test.data", "--type", "int"])
    assert result.exit_code == 0
    assert result.output == "123\n"
    result = runner.invoke(cli.cli, ["config", "--unset", "test.data"])
    assert result.exit_code == 0
    result = runner.invoke(cli.cli, ["config", "--list"])
    assert result.exit_code == 0
    assert "123" not in result.output


@pytest.mark.vcr
def test_log() -> None:
    """Test log and log filtering."""
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["--json", "log"])
    assert result.exit_code == 0
    logs = simplejson.loads(result.output)
    assert len(logs) == 4
    result = runner.invoke(cli.cli, ["--json", "log", "a8e1c2b99026bc7c1bdf409f1ce538bbde7f898443ef7aed5c999e9769b68689"])
    assert result.exit_code == 0
    logs = simplejson.loads(result.output)
    assert len(logs) == 3
    result = runner.invoke(cli.cli, ["--json", "log", "f5bd419dc54585d38082123120b0b268361f97efff5d64ddb1cc707282e2c63b"])
    assert result.exit_code == 0
    logs = simplejson.loads(result.output)
    assert len(logs) == 2


@pytest.mark.vcr
def test_ref() -> None:
    """Test create and assign refs."""
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["--json", "branch"])
    assert result.exit_code == 0
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 1
    result = runner.invoke(cli.cli, ["branch", "dev"])
    assert result.exit_code == 0
    result = runner.invoke(cli.cli, ["--json", "branch"])
    assert result.exit_code == 0
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 2
    result = runner.invoke(cli.cli, ["branch", "etl", "main"])
    assert result.exit_code == 0
    result = runner.invoke(cli.cli, ["--json", "branch"])
    assert result.exit_code == 0
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 3
    result = runner.invoke(cli.cli, ["branch", "-d", "etl"])
    assert result.exit_code == 0
    result = runner.invoke(cli.cli, ["branch", "-d", "dev"])
    assert result.exit_code == 0
    result = runner.invoke(cli.cli, ["--json", "branch"])
    assert result.exit_code == 0
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 1


@pytest.mark.doc
def test_all_help_options() -> None:
    """Write out all help options to std out."""
    runner = CliRunner()
    args = ["", "config", "branch", "tag", "remote", "log", "merge", "cherry-pick", "contents"]

    for i in args:
        result = runner.invoke(cli.cli, [x for x in [i] if x] + ["--help"])
        assert result.exit_code == 0
        cwd = os.getcwd()
        with open(Path(Path(cwd), "docs", "{}.rst".format(i if i else "main")), "w") as f:
            f.write(".. code-block:: bash\n\n\t")
            for line in result.output.split("\n"):
                f.write(line + "\n\t")
            f.write("\n\n")
