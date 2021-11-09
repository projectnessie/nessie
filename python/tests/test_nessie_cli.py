#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests for `pynessie` package."""
import itertools

import confuse
import pytest
import simplejson
from assertpy import assert_that

from pynessie import __version__
from pynessie.model import Branch
from pynessie.model import ContentSchema
from pynessie.model import EntrySchema
from pynessie.model import IcebergTable
from pynessie.model import ReferenceSchema
from .conftest import execute_cli_command, make_commit


@pytest.mark.vcr
def test_command_line_interface() -> None:
    """Test the CLI."""
    assert "Usage: nessie" in execute_cli_command([])
    assert "Usage: nessie" in execute_cli_command(["--help"])
    assert __version__ in execute_cli_command(["--version"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l"]), many=True)
    assert len(references) == 1
    assert references[0].name == "main"
    assert isinstance(references[0], Branch)


def test_config_options() -> None:
    """Ensure config cli option is consistent."""
    assert "Usage: nessie" in execute_cli_command(["config"])
    vars_to_add = ["--add x", "--get x", "--list", "--unset x"]
    for i in itertools.permutations(vars_to_add, 2):
        assert "Error: Illegal usage: " in execute_cli_command(["config"] + [*i[0].split(" "), *i[1].split(" ")], ret_val=2)

    execute_cli_command(["config", "x", "--add", "x"])


def test_set_unset() -> None:
    """Test config set/unset/list."""
    execute_cli_command(["config", "--add", "test.data", "123", "--type", "int"])
    assert execute_cli_command(["config", "test.data", "--type", "int"]) == "123\n"
    execute_cli_command(["config", "--unset", "test.data"])
    assert "123" not in execute_cli_command(["config", "--list"])


@pytest.mark.vcr
def test_remote() -> None:
    """Test setting and viewing remote."""
    execute_cli_command(["remote", "add", "http://test.url"])
    execute_cli_command(["remote", "add", "http://localhost:19120/api/v1"])
    assert "main" in execute_cli_command(["--json", "remote", "show"])
    execute_cli_command(["remote", "set-head", "dev"])
    assert execute_cli_command(["config", "default_branch"]) == "dev\n"
    execute_cli_command(["remote", "set-head", "dev", "-d"])
    result = execute_cli_command(["config", "default_branch"], ret_val=1, output_string=False)
    assert result.output == ""
    assert isinstance(result.exception, confuse.exceptions.ConfigTypeError)


def _new_table(table_id: str) -> IcebergTable:
    return IcebergTable(table_id, "/a/b/c", "xyz")


@pytest.mark.vcr
def test_log() -> None:
    """Test log and log filtering."""
    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    assert len(logs) == 0
    execute_cli_command(["branch", "dev_test_log"])
    table = _new_table("test_log_dev")
    make_commit("log.foo.dev", table, "dev_test_log", author="nessie_user1")
    table = _new_table("test_log")
    make_commit("log.foo.bar", table, "main", author="nessie_user1")
    tables = ContentSchema().loads(execute_cli_command(["--json", "content", "view", "log.foo.bar"]), many=True)
    assert len(tables) == 1
    assert tables[0] == table
    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    assert len(logs) == 1
    logs = simplejson.loads(execute_cli_command(["--json", "log", "--revision-range", logs[0]["hash"]]))
    assert len(logs) == 1
    entries = EntrySchema().loads(execute_cli_command(["--json", "content", "list"]), many=True)
    assert len(entries) == 1
    execute_cli_command(
        [
            "--json",
            "content",
            "commit",
            "log.foo.bar",
            "-R",
            "--ref",
            "main",
            "-m",
            "delete_message",
            "-c",
            logs[0]["hash"],
            "--author",
            "nessie_user2",
        ],
    )
    logs = simplejson.loads(execute_cli_command(["--json", "log", "-n", 1]))
    assert len(logs) == 1
    logs = simplejson.loads(execute_cli_command(["--json", "log", "dev_test_log"]))
    assert len(logs) == 1
    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    assert len(logs) == 2
    logs = simplejson.loads(execute_cli_command(["--json", "log", "--revision-range", "{}..{}".format(logs[0]["hash"], logs[1]["hash"])]))
    assert len(logs) == 1
    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    assert len(logs) == 2
    logs = simplejson.loads(execute_cli_command(["--json", "log", "--author", "nessie_user1"]))
    assert len(logs) == 1
    assert_that(logs[0]["author"]).is_equal_to("nessie_user1")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "--author", "nessie_user2"]))
    assert len(logs) == 1
    assert_that(logs[0]["author"]).is_equal_to("nessie_user2")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "--author", "nessie_user2", "--author", "nessie_user1"]))
    assert len(logs) == 2
    # the committer is set on the server-side and is empty if we're not logged
    # in when performing a commit
    logs = simplejson.loads(execute_cli_command(["--json", "log", "--committer", ""]))
    assert len(logs) == 2
    logs = simplejson.loads(
        execute_cli_command(["--json", "log", "--query", "commit.author == 'nessie_user2' || commit.author == 'non_existing'"])
    )
    assert len(logs) == 1
    logs = simplejson.loads(
        execute_cli_command(["--json", "log", "--after", "2001-01-01T00:00:00+00:00", "--before", "2999-12-30T23:00:00+00:00"])
    )
    assert len(logs) == 2


@pytest.mark.vcr
def test_branch() -> None:
    """Test create and assign refs."""
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 1
    execute_cli_command(["branch", "dev"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 2
    execute_cli_command(["branch", "etl", "main"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 3
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l", "etl"]), many=False)
    assert_that(references.name).is_equal_to("etl")
    references = simplejson.loads(execute_cli_command(["--json", "branch", "-l", "foo"]))
    assert len(references) == 0
    execute_cli_command(["branch", "-d", "etl"])
    execute_cli_command(["branch", "-d", "dev"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert len(references) == 1


@pytest.mark.vcr
def test_tag() -> None:
    """Test create and assign refs."""
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 0
    execute_cli_command(["tag", "dev-tag", "main"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 1
    execute_cli_command(["tag", "etl-tag", "main"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 2
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag", "-l", "etl-tag"]), many=False)
    assert_that(references.name).is_equal_to("etl-tag")
    references = simplejson.loads(execute_cli_command(["--json", "tag", "-l", "foo"]))
    assert len(references) == 0
    execute_cli_command(["tag", "-d", "etl-tag"])
    execute_cli_command(["tag", "-d", "dev-tag"])
    references = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    assert len(references) == 0
    execute_cli_command(["tag", "v1.0"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)}
    branches = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)}
    assert tags["v1.0"] == branches["main"]


@pytest.mark.vcr
def test_assign() -> None:
    """Test assign operation."""
    execute_cli_command(["branch", "dev"])
    make_commit("assign.foo.bar", _new_table("test_assign"), "dev")
    execute_cli_command(["branch", "main", "dev", "--force"])
    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    refs = {i.name: i.hash_ for i in branches}
    assert refs["main"] == refs["dev"]
    execute_cli_command(["tag", "v1.0", "main"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)}
    assert tags["v1.0"] == refs["main"]
    execute_cli_command(["tag", "v1.0", "dev", "--force"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)}
    assert tags["v1.0"] == refs["dev"]


@pytest.mark.vcr
def test_merge() -> None:
    """Test merge operation."""
    execute_cli_command(["branch", "dev"])
    make_commit("merge.foo.bar", _new_table("test_merge"), "dev")
    ref = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l", "main"]), many=False)
    main_hash = ref.hash_
    execute_cli_command(["merge", "dev", "-c", main_hash])
    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    refs = {i.name: i.hash_ for i in branches}
    assert refs["main"] == refs["dev"]


@pytest.mark.vcr
def test_transplant() -> None:
    """Test transplant operation."""
    execute_cli_command(["branch", "dev"])
    make_commit("transplant.foo.bar", _new_table("test_transplant_1"), "dev")
    make_commit("bar.bar", _new_table("test_transplant_2"), "dev")
    make_commit("foo.baz", _new_table("test_transplant_3"), "dev")
    refs = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l"]), many=True)
    main_hash = next(i.hash_ for i in refs if i.name == "main")
    logs = simplejson.loads(execute_cli_command(["--json", "log", "dev"]))
    first_hash = [i["hash"] for i in logs]
    execute_cli_command(["cherry-pick", "-c", main_hash, "-s", "dev", first_hash[1], first_hash[0]])

    logs = simplejson.loads(execute_cli_command(["--json", "log"]))
    assert len(logs) == 2  # two commits were transplanted into an empty `main`
