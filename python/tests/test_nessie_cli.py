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
import os
from pathlib import Path

import confuse
import pytest
import simplejson
from assertpy import assert_that

from pynessie import __version__
from pynessie.model import Branch
from pynessie.model import Contents
from pynessie.model import ContentsSchema
from pynessie.model import ContentsWithKey
from pynessie.model import ContentsWithKeySchema
from pynessie.model import DeltaLakeTable
from pynessie.model import EntrySchema
from pynessie.model import IcebergTable
from pynessie.model import ReferenceSchema
from pynessie.model import SqlView
from pynessie.utils import contents_key
from .conftest import _cli
from .conftest import _run


@pytest.mark.vcr
def test_command_line_interface() -> None:
    """Test the CLI."""
    assert "Usage: cli" in _cli(list())
    assert "Usage: cli" in _cli(["--help"])
    assert __version__ in _cli(["--version"])
    references = ReferenceSchema().loads(_cli(["--json", "branch", "-l"]), many=True)
    assert len(references) == 1
    assert references[0].name == "main"
    assert isinstance(references[0], Branch)


def test_config_options() -> None:
    """Ensure config cli option is consistent."""
    assert "Usage: cli" in _cli(["config"])
    vars = ["--add x", "--get x", "--list", "--unset x"]
    for i in itertools.permutations(vars, 2):
        assert "Error: Illegal usage: " in _cli(["config"] + [*i[0].split(" "), *i[1].split(" ")], ret_val=2)

    _cli(["config", "x", "--add", "x"])


def test_set_unset() -> None:
    """Test config set/unset/list."""
    _cli(["config", "--add", "test.data", "123", "--type", "int"])
    assert _cli(["config", "test.data", "--type", "int"]) == "123\n"
    _cli(["config", "--unset", "test.data"])
    assert "123" not in _cli(["config", "--list"])


@pytest.mark.vcr
def test_remote() -> None:
    """Test setting and viewing remote."""
    _cli(["remote", "add", "http://test.url"])
    _cli(["remote", "add", "http://localhost:19120/api/v1"])
    assert "main" in _cli(["--json", "remote", "show"])
    _cli(["remote", "set-head", "dev"])
    assert _cli(["config", "default_branch"]) == "dev\n"
    _cli(["remote", "set-head", "dev", "-d"])
    result = _run(["config", "default_branch"], ret_val=1)
    assert result.output == ""
    assert isinstance(result.exception, confuse.exceptions.ConfigTypeError)


def _new_table(table_id: str) -> IcebergTable:
    return IcebergTable(table_id, "/a/b/c", "xyz")


def _make_commit(
    key: str, table: Contents, branch: str, head_hash: str = None, message: str = "test message", author: str = "nessie test"
) -> None:
    if not head_hash:
        refs = {i.name: i.hash_ for i in ReferenceSchema().loads(_cli(["--json", "branch"]), many=True)}
        head_hash = refs[branch]
    _cli(
        ["contents", "--set", "--stdin", key, "--ref", branch, "-m", message, "-c", head_hash, "--author", author],
        input=ContentsSchema().dumps(table),
    )


@pytest.mark.vcr
def test_log() -> None:
    """Test log and log filtering."""
    logs = simplejson.loads(_cli(["--json", "log"]))
    assert len(logs) == 0
    _cli(["branch", "dev_test_log"])
    table = _new_table("test_log_dev")
    _make_commit("log.foo.dev", table, "dev_test_log", author="nessie_user1")
    table = _new_table("test_log")
    _make_commit("log.foo.bar", table, "main", author="nessie_user1")
    tables = ContentsWithKeySchema().loads(_cli(["--json", "contents", "log.foo.bar"]), many=True)
    assert len(tables) == 1
    assert tables[0] == ContentsWithKey(contents_key("log.foo.bar"), table)
    logs = simplejson.loads(_cli(["--json", "log"]))
    assert len(logs) == 1
    logs = simplejson.loads(_cli(["--json", "log", "--revision-range", logs[0]["hash"]]))
    assert len(logs) == 1
    entries = EntrySchema().loads(_cli(["--json", "contents", "--list"]), many=True)
    assert len(entries) == 1
    _cli(
        [
            "--json",
            "contents",
            "--delete",
            "log.foo.bar",
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
    logs = simplejson.loads(_cli(["--json", "log", "-n", 1]))
    assert len(logs) == 1
    logs = simplejson.loads(_cli(["--json", "log", "dev_test_log"]))
    assert len(logs) == 1
    logs = simplejson.loads(_cli(["--json", "log"]))
    assert len(logs) == 2
    logs = simplejson.loads(_cli(["--json", "log", "--revision-range", "{}..{}".format(logs[0]["hash"], logs[1]["hash"])]))
    assert len(logs) == 1
    logs = simplejson.loads(_cli(["--json", "log"]))
    assert len(logs) == 2
    logs = simplejson.loads(_cli(["--json", "log", "--author", "nessie_user1"]))
    assert len(logs) == 1
    assert_that(logs[0]["author"]).is_equal_to("nessie_user1")
    logs = simplejson.loads(_cli(["--json", "log", "--author", "nessie_user2"]))
    assert len(logs) == 1
    assert_that(logs[0]["author"]).is_equal_to("nessie_user2")
    logs = simplejson.loads(_cli(["--json", "log", "--author", "nessie_user2", "--author", "nessie_user1"]))
    assert len(logs) == 2
    # the committer is set on the server-side and is empty if we're not logged
    # in when performing a commit
    logs = simplejson.loads(_cli(["--json", "log", "--committer", ""]))
    assert len(logs) == 2
    logs = simplejson.loads(_cli(["--json", "log", "--query", "commit.author == 'nessie_user2' || commit.author == 'non_existing'"]))
    assert len(logs) == 1
    logs = simplejson.loads(_cli(["--json", "log", "--after", "2001-01-01T00:00:00+00:00", "--before", "2999-12-30T23:00:00+00:00"]))
    assert len(logs) == 2


@pytest.mark.vcr
def test_branch() -> None:
    """Test create and assign refs."""
    references = ReferenceSchema().loads(_cli(["--json", "branch"]), many=True)
    assert len(references) == 1
    _cli(["branch", "dev"])
    references = ReferenceSchema().loads(_cli(["--json", "branch"]), many=True)
    assert len(references) == 2
    _cli(["branch", "etl", "main"])
    references = ReferenceSchema().loads(_cli(["--json", "branch"]), many=True)
    assert len(references) == 3
    references = ReferenceSchema().loads(_cli(["--json", "branch", "-l", "etl"]), many=False)
    assert_that(references.name).is_equal_to("etl")
    references = simplejson.loads(_cli(["--json", "branch", "-l", "foo"]))
    assert len(references) == 0
    _cli(["branch", "-d", "etl"])
    _cli(["branch", "-d", "dev"])
    references = ReferenceSchema().loads(_cli(["--json", "branch"]), many=True)
    assert len(references) == 1


@pytest.mark.vcr
def test_tag() -> None:
    """Test create and assign refs."""
    references = ReferenceSchema().loads(_cli(["--json", "tag"]), many=True)
    assert len(references) == 0
    _cli(["tag", "dev-tag", "main"])
    references = ReferenceSchema().loads(_cli(["--json", "tag"]), many=True)
    assert len(references) == 1
    _cli(["tag", "etl-tag", "main"])
    references = ReferenceSchema().loads(_cli(["--json", "tag"]), many=True)
    assert len(references) == 2
    references = ReferenceSchema().loads(_cli(["--json", "tag", "-l", "etl-tag"]), many=False)
    assert_that(references.name).is_equal_to("etl-tag")
    references = simplejson.loads(_cli(["--json", "tag", "-l", "foo"]))
    assert len(references) == 0
    _cli(["tag", "-d", "etl-tag"])
    _cli(["tag", "-d", "dev-tag"])
    references = ReferenceSchema().loads(_cli(["--json", "tag"]), many=True)
    assert len(references) == 0
    _cli(["tag", "v1.0"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(_cli(["--json", "tag"]), many=True)}
    branches = {i.name: i.hash_ for i in ReferenceSchema().loads(_cli(["--json", "branch"]), many=True)}
    assert tags["v1.0"] == branches["main"]


@pytest.mark.vcr
def test_commit_with_expected_state() -> None:
    """Test making a commit with some expected Contents, i.e. IcebergTable."""
    _cli(["branch", "dev"])
    _make_commit("commit.expected.contents", _new_table("test_expected_contents"), "dev", message="commit 1")
    # the second commit will use the Contents of the first one as "expected contents"
    _make_commit("commit.expected.contents", _new_table("test_expected_contents"), "dev", message="commit 2")


@pytest.mark.vcr
def test_commit_no_expected_state() -> None:
    """Test making two commit without any expected Contents, i.e. using DeltaLakeTable."""
    _cli(["branch", "dev"])
    table1 = DeltaLakeTable(
        id="test_commit_no_expected_state", metadata_location_history=["asd111"], checkpoint_location_history=["def"], last_checkpoint="x"
    )
    _make_commit("commit.expected.contents", table1, "dev", message="commit 1")
    table2 = DeltaLakeTable(
        id="test_commit_no_expected_state", metadata_location_history=["asd222"], checkpoint_location_history=["def"], last_checkpoint="x"
    )
    # the second commit will set new contents without the "expected contents" parameter due to using a DeltaLakeTable
    _make_commit("commit.expected.contents", table2, "dev", message="commit 2")


@pytest.mark.vcr
def test_assign() -> None:
    """Test assign operation."""
    _cli(["branch", "dev"])
    _make_commit("assign.foo.bar", _new_table("test_assign"), "dev")
    _run(["branch", "main", "dev", "--force"])
    branches = ReferenceSchema().loads(_cli(["--json", "branch"]), many=True)
    refs = {i.name: i.hash_ for i in branches}
    assert refs["main"] == refs["dev"]
    _cli(["tag", "v1.0", "main"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(_cli(["--json", "tag"]), many=True)}
    assert tags["v1.0"] == refs["main"]
    _cli(["tag", "v1.0", "dev", "--force"])
    tags = {i.name: i.hash_ for i in ReferenceSchema().loads(_cli(["--json", "tag"]), many=True)}
    assert tags["v1.0"] == refs["dev"]


@pytest.mark.vcr
def test_merge() -> None:
    """Test merge operation."""
    _cli(["branch", "dev"])
    _make_commit("merge.foo.bar", _new_table("test_merge"), "dev")
    ref = ReferenceSchema().loads(_cli(["--json", "branch", "-l", "main"]), many=False)
    main_hash = ref.hash_
    _cli(["merge", "dev", "-c", main_hash])
    branches = ReferenceSchema().loads(_cli(["--json", "branch"]), many=True)
    refs = {i.name: i.hash_ for i in branches}
    assert refs["main"] == refs["dev"]


@pytest.mark.vcr
def test_transplant() -> None:
    """Test transplant operation."""
    _cli(["branch", "dev"])
    _make_commit("transplant.foo.bar", _new_table("test_transplant_1"), "dev")
    _make_commit("bar.bar", _new_table("test_transplant_2"), "dev")
    _make_commit("foo.baz", _new_table("test_transplant_3"), "dev")
    refs = ReferenceSchema().loads(_cli(["--json", "branch", "-l"]), many=True)
    main_hash = next(i.hash_ for i in refs if i.name == "main")
    logs = simplejson.loads(_cli(["--json", "log", "dev"]))
    first_hash = [i["hash"] for i in logs]
    _cli(["cherry-pick", "-c", main_hash, "-s", "dev", first_hash[1], first_hash[0]])

    logs = simplejson.loads(_cli(["--json", "log"]))
    assert len(logs) == 2  # two commits were transplanted into an empty `main`


@pytest.mark.doc
def test_all_help_options() -> None:
    """Write out all help options to std out."""
    args = ["", "config", "branch", "tag", "remote", "log", "merge", "cherry-pick", "contents"]

    for i in args:
        result = _cli([x for x in [i] if x] + ["--help"])
        cwd = os.getcwd()
        with open(Path(Path(cwd), "docs", "{}.rst".format(i if i else "main")), "w") as f:
            f.write(".. code-block:: bash\n\n\t")
            for line in result.split("\n"):
                f.write(line + "\n\t")
            f.write("\n\n")


@pytest.mark.vcr
def test_contents_listing() -> None:
    """Test contents listing and filtering."""
    branch = "contents_listing_dev"
    _cli(["branch", branch])

    iceberg_table = IcebergTable(id="test_contents_listing", metadata_location="/a/b/c", id_generators="xyz")
    delta_lake_table = DeltaLakeTable(
        id="uuid2", metadata_location_history=["asd"], checkpoint_location_history=["def"], last_checkpoint="x"
    )
    sql_view = SqlView(id="uuid3", sql_text="SELECT * FROM foo", dialect="SPARK")

    _make_commit("this.is.iceberg.foo", iceberg_table, branch)
    _make_commit("this.is.delta.bar", delta_lake_table, branch)
    _make_commit("this.is.sql.baz", sql_view, branch)

    tables = ContentsWithKeySchema().loads(_cli(["--json", "contents", "--ref", branch, "this.is.iceberg.foo"]), many=True)
    assert_that(tables).is_equal_to([ContentsWithKey(contents_key("this.is.iceberg.foo"), iceberg_table)])

    tables = ContentsWithKeySchema().loads(_cli(["--json", "contents", "--ref", branch, "this.is.delta.bar"]), many=True)
    assert_that(tables).is_equal_to([ContentsWithKey(contents_key("this.is.delta.bar"), delta_lake_table)])

    tables = ContentsWithKeySchema().loads(
        _cli(["--json", "contents", "--ref", branch, "this.is.iceberg.foo", "this.is.delta.bar"]), many=True
    )
    assert_that(tables).is_equal_to(
        [
            ContentsWithKey(contents_key("this.is.iceberg.foo"), iceberg_table),
            ContentsWithKey(contents_key("this.is.delta.bar"), delta_lake_table),
        ]
    )

    tables = ContentsWithKeySchema().loads(
        _cli(["--json", "contents", "--ref", branch, "this.is.iceberg.foo", "this.is.not.there", "this.is.delta.bar"]), many=True
    )
    assert_that(tables).is_equal_to(
        [
            ContentsWithKey(contents_key("this.is.iceberg.foo"), iceberg_table),
            ContentsWithKey(contents_key("this.is.not.there"), None),
            ContentsWithKey(contents_key("this.is.delta.bar"), delta_lake_table),
        ]
    )

    tables = EntrySchema().loads(_cli(["--json", "contents", "--ref", branch, "--list", "--type", "ICEBERG_TABLE"]), many=True)
    assert_that(tables).is_length(1)
    assert_that(tables[0].kind).is_equal_to("ICEBERG_TABLE")

    tables = EntrySchema().loads(_cli(["--json", "contents", "--ref", branch, "--list", "--type", "DELTA_LAKE_TABLE"]), many=True)
    assert_that(tables).is_length(1)
    assert_that(tables[0].kind).is_equal_to("DELTA_LAKE_TABLE")

    result = _cli(["--json", "contents", "--ref", branch, "--list", "--query", "entry.contentType == 'ICEBERG_TABLE'"])
    tables = EntrySchema().loads(result, many=True)
    assert_that(tables).is_length(1)
    assert_that(tables[0].kind).is_equal_to("ICEBERG_TABLE")

    result = _cli(
        ["--json", "contents", "--ref", branch, "--list", "--query", "entry.contentType in ['ICEBERG_TABLE', 'DELTA_LAKE_TABLE']"]
    )
    tables = EntrySchema().loads(result, many=True)
    assert_that(tables).is_length(2)
    assert_that(set(t.kind for t in tables)).is_equal_to({"DELTA_LAKE_TABLE", "ICEBERG_TABLE"})

    result = _cli(["--json", "contents", "--ref", branch, "--list", "--query", "entry.namespace.startsWith('this.is.del')"])
    tables = EntrySchema().loads(result, many=True)
    assert_that(tables).is_length(1)
    assert_that(tables[0].kind).is_equal_to("DELTA_LAKE_TABLE")

    result = _cli(["--json", "contents", "--ref", branch, "--list", "--query", "entry.namespace.startsWith('this.is')"])
    tables = EntrySchema().loads(result, many=True)
    assert_that(tables).is_length(3)
    assert_that(set(i.kind for i in tables)).is_equal_to({"ICEBERG_TABLE", "VIEW", "DELTA_LAKE_TABLE"})
