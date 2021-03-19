#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `pynessie` package."""
import itertools
import os
from pathlib import Path
from typing import List
from typing import Optional

import confuse
import pytest
import simplejson
from click.testing import CliRunner
from click.testing import Result

from pynessie import __version__
from pynessie import cli
from pynessie.model import Branch
from pynessie.model import ContentsSchema
from pynessie.model import EntrySchema
from pynessie.model import IcebergTable
from pynessie.model import ReferenceSchema


def _run(runner: CliRunner, args: List[str], input: Optional[str] = None, ret_val: int = 0) -> Result:
    result = runner.invoke(cli.cli, args, input=input)
    if result.exit_code != ret_val:
        print(result.output)
    assert result.exit_code == ret_val
    return result


@pytest.mark.vcr
def test_command_line_interface() -> None:
    """Test the CLI."""
    runner = CliRunner()
    result = _run(runner, list())
    assert "Usage: cli" in result.output
    help_result = _run(runner, ["--help"])
    assert "Usage: cli" in help_result.output
    help_result = _run(runner, ["--version"])
    assert __version__ in help_result.output
    help_result = _run(runner, ["--json", "branch", "-l"])
    references = ReferenceSchema().loads(help_result.output, many=True)
    assert len(references) == 1
    assert references[0].name == "main"
    assert isinstance(references[0], Branch)


def test_config_options() -> None:
    """Ensure config cli option is consistent."""
    runner = CliRunner()
    result = _run(runner, ["config"])
    assert "Usage: cli" in result.output
    vars = ["--add x", "--get x", "--list", "--unset x"]
    for i in itertools.permutations(vars, 2):
        result = _run(runner, ["config"] + [*i[0].split(" "), *i[1].split(" ")], ret_val=2)
        assert "Error: Illegal usage: " in result.output

    _run(runner, ["config", "x", "--add", "x"])


def test_set_unset() -> None:
    """Test config set/unset/list."""
    runner = CliRunner()
    _run(runner, ["config", "--add", "test.data", "123", "--type", "int"])
    result = _run(runner, ["config", "test.data", "--type", "int"])
    assert result.output == "123\n"
    _run(runner, ["config", "--unset", "test.data"])
    result = _run(runner, ["config", "--list"])
    assert "123" not in result.output


@pytest.mark.vcr
def test_remote() -> None:
    """Test setting and viewing remote."""
    runner = CliRunner()
    _run(runner, ["remote", "add", "http://test.url"])
    _run(runner, ["remote", "add", "http://localhost:19120/api/v1"])
    result = _run(runner, ["--json", "remote", "show"])
    assert "main" in result.output
    _run(runner, ["remote", "set-head", "dev"])
    result = _run(runner, ["config", "default_branch"])
    assert result.output == "dev\n"
    _run(runner, ["remote", "set-head", "dev", "-d"])
    result = _run(runner, ["config", "default_branch"], ret_val=1)
    assert result.output == ""
    assert isinstance(result.exception, confuse.exceptions.ConfigTypeError)
    _run(runner, ["remote", "set-head", "main"])


@pytest.mark.vcr
def test_log() -> None:
    """Test log and log filtering."""
    runner = CliRunner()
    result = _run(runner, ["--json", "log"])
    logs = simplejson.loads(result.output)
    assert len(logs) == 0
    refs = ReferenceSchema().loads(_run(runner, ["--json", "branch", "-l", "main"]).output, many=True)
    empty_hash = refs[0].hash_
    _run(
        runner,
        [
            "contents",
            "--set",
            "foo.bar",
            "--ref",
            "main",
            "-m",
            "test_message",
            "-c",
            empty_hash,
        ],
        input=ContentsSchema().dumps(IcebergTable("/a/b/c")),
    )
    result = _run(runner, ["--json", "contents", "foo.bar"])
    tables = ContentsSchema().loads(result.output, many=True)
    assert len(tables) == 1
    assert tables[0] == IcebergTable("/a/b/c")
    result = _run(runner, ["--json", "log"])
    logs = simplejson.loads(result.output)
    assert len(logs) == 1
    result = _run(runner, ["--json", "log", logs[0]["hash"]])
    logs = simplejson.loads(result.output)
    assert len(logs) == 1
    result = _run(runner, ["--json", "contents", "--list"])
    entries = EntrySchema().loads(result.output, many=True)
    assert len(entries) == 1
    _run(runner, ["--json", "contents", "--delete", "foo.bar", "--ref", "main", "-m", "delete_message", "-c", logs[0]["hash"]])
    result = _run(runner, ["--json", "log"])
    logs = simplejson.loads(result.output)
    assert len(logs) == 2
    result = _run(runner, ["--json", "log", "{}..{}".format(logs[0]["hash"], logs[1]["hash"])])
    logs = simplejson.loads(result.output)
    assert len(logs) == 1


@pytest.mark.vcr
def test_ref() -> None:
    """Test create and assign refs."""
    runner = CliRunner()
    result = _run(runner, ["--json", "branch"])
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 1
    _run(runner, ["branch", "dev"])
    result = _run(runner, ["--json", "branch"])
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 2
    _run(runner, ["branch", "etl", "main"])
    result = _run(runner, ["--json", "branch"])
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 3
    _run(runner, ["branch", "-d", "etl"])
    _run(runner, ["branch", "-d", "dev"])
    result = _run(runner, ["--json", "branch"])
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 1


@pytest.mark.vcr
def test_tag() -> None:
    """Test create and assign refs."""
    runner = CliRunner()
    result = _run(runner, ["--json", "tag"])
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 0
    _run(runner, ["tag", "dev", "main"])
    result = _run(runner, ["--json", "tag"])
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 1
    _run(runner, ["tag", "etl", "main"])
    result = _run(runner, ["--json", "tag"])
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 2
    _run(runner, ["tag", "-d", "etl"])
    _run(runner, ["tag", "-d", "dev"])
    result = _run(runner, ["--json", "tag"])
    references = ReferenceSchema().loads(result.output, many=True)
    assert len(references) == 0
    _run(runner, ["tag", "v1.0"], ret_val=1)


@pytest.mark.vcr
def test_assign() -> None:
    """Test assign operation."""
    runner = CliRunner()
    _run(runner, ["branch", "dev"])
    refs = ReferenceSchema().loads(_run(runner, ["--json", "branch", "-l", "dev"]).output, many=True)
    empty_hash = next(i.hash_ for i in refs if i.name == "dev")
    _run(
        runner,
        [
            "contents",
            "--set",
            "foo.bar",
            "--ref",
            "dev",
            "-m",
            "test_message",
            "-c",
            empty_hash,
        ],
        input=ContentsSchema().dumps(IcebergTable("/a/b/c")),
    )
    _run(runner, ["branch", "main", "dev", "--force"])
    result = _run(runner, ["--json", "branch"])
    branches = ReferenceSchema().loads(result.output, many=True)
    refs = {i.name: i.hash_ for i in branches}
    assert refs["main"] == refs["dev"]
    _run(runner, ["tag", "v1.0", "main"])
    result = _run(runner, ["--json", "tag"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(result.output, many=True)}
    assert tags["v1.0"] == refs["main"]
    _run(runner, ["tag", "v1.0", "dev", "--force"])
    result = _run(runner, ["--json", "tag"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(result.output, many=True)}
    assert tags["v1.0"] == refs["dev"]
    _run(runner, ["branch", "dev", "--delete"])
    _run(runner, ["tag", "v1.0", "--delete"])
    result = _run(runner, ["--json", "log"])
    logs = simplejson.loads(result.output)
    _run(runner, ["--json", "contents", "--delete", "foo.bar", "--ref", "main", "-m", "delete_message", "-c", logs[0]["hash"]])
    _run(runner, ["branch", "main", "--delete"])
    _run(runner, ["branch", "main"])


@pytest.mark.vcr
def test_merge() -> None:
    """Test merge operation."""
    runner = CliRunner()
    _run(runner, ["branch", "dev"])
    refs = ReferenceSchema().loads(_run(runner, ["--json", "branch", "-l", "dev"]).output, many=True)
    empty_hash = next(i.hash_ for i in refs if i.name == "dev")
    _run(
        runner,
        [
            "contents",
            "--set",
            "foo.bar",
            "--ref",
            "dev",
            "-m",
            "test_message",
            "-c",
            empty_hash,
        ],
        input=ContentsSchema().dumps(IcebergTable("/a/b/c")),
    )
    refs = ReferenceSchema().loads(_run(runner, ["--json", "branch", "-l", "main"]).output, many=True)
    main_hash = next(i.hash_ for i in refs if i.name == "main")
    _run(runner, ["merge", "dev", "-c", main_hash])
    result = _run(runner, ["--json", "branch"])
    branches = ReferenceSchema().loads(result.output, many=True)
    refs = {i.name: i.hash_ for i in branches}
    assert refs["main"] == refs["dev"]
    _run(runner, ["branch", "dev", "--delete"])
    result = _run(runner, ["--json", "log"])
    logs = simplejson.loads(result.output)
    _run(runner, ["--json", "contents", "--delete", "foo.bar", "--ref", "main", "-m", "delete_message", "-c", logs[0]["hash"]])
    _run(runner, ["branch", "main", "--delete"])
    _run(runner, ["branch", "main"])


@pytest.mark.vcr
def test_transplant() -> None:
    """Test transplant operation."""
    runner = CliRunner()
    _run(runner, ["branch", "dev"])
    refs = ReferenceSchema().loads(_run(runner, ["--json", "branch", "-l", "dev"]).output, many=True)
    empty_hash = next(i.hash_ for i in refs if i.name == "dev")
    _run(
        runner,
        [
            "contents",
            "--set",
            "foo.bar",
            "--ref",
            "dev",
            "-m",
            "test_message",
            "-c",
            empty_hash,
        ],
        input=ContentsSchema().dumps(IcebergTable("/a/b/c")),
    )
    _run(
        runner,
        [
            "contents",
            "--set",
            "bar.bar",
            "--ref",
            "dev",
            "-m",
            "test_message2",
            "-c",
            empty_hash,
        ],
        input=ContentsSchema().dumps(IcebergTable("/a/b/c")),
    )
    _run(
        runner,
        [
            "contents",
            "--set",
            "foo.baz",
            "--ref",
            "main",
            "-m",
            "test_message3",
            "-c",
            empty_hash,
        ],
        input=ContentsSchema().dumps(IcebergTable("/a/b/c")),
    )
    refs = ReferenceSchema().loads(_run(runner, ["--json", "branch", "-l"]).output, many=True)
    main_hash = next(i.hash_ for i in refs if i.name == "main")
    result = _run(runner, ["--json", "log", "dev"])
    logs = simplejson.loads(result.output)
    first_hash = [i["hash"] for i in logs]
    _run(runner, ["cherry-pick", "-c", main_hash, first_hash[1], first_hash[0]])

    result = _run(runner, ["--json", "log"])
    logs = simplejson.loads(result.output)
    assert len(logs) == 3
    _run(runner, ["--json", "contents", "--delete", "foo.bar", "--ref", "main", "-m", "delete_message", "-c", logs[0]["hash"]])
    _run(runner, ["branch", "dev", "--delete"])
    _run(runner, ["branch", "main", "--delete"])
    _run(runner, ["branch", "main"])


@pytest.mark.doc
def test_all_help_options() -> None:
    """Write out all help options to std out."""
    runner = CliRunner()
    args = ["", "config", "branch", "tag", "remote", "log", "merge", "cherry-pick", "contents"]

    for i in args:
        result = _run(runner, [x for x in [i] if x] + ["--help"])
        cwd = os.getcwd()
        with open(Path(Path(cwd), "docs", "{}.rst".format(i if i else "main")), "w") as f:
            f.write(".. code-block:: bash\n\n\t")
            for line in result.output.split("\n"):
                f.write(line + "\n\t")
            f.write("\n\n")
