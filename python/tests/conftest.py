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
"""Configure pytest."""

import os
import shutil
import tempfile
from typing import Any, List
from typing import Optional

import attr
import pytest
from _pytest.fixtures import FixtureRequest
from assertpy import assert_that
from click.testing import CliRunner
from click.testing import Result
from pytest import Session
from vcr.request import Request

from pynessie import cli
from pynessie.model import Content, ContentSchema, ReferenceSchema


@attr.dataclass
class NessieTestConfig:
    """Test configs for pynessie tests."""

    config_dir: str
    cleanup: bool


nessie_test_config: NessieTestConfig = NessieTestConfig("", False)


def pytest_configure(config):  # noqa
    """Configure pytest."""
    config.addinivalue_line("markers", "doc: mark as end-to-end test.")


def pytest_sessionstart(session: "Session") -> None:
    """Setup a fresh temporary config directory for tests."""
    nessie_test_config.config_dir = tempfile.mkdtemp() + "/"
    # Instruct Confuse to keep Nessie config file in the temp location:
    os.environ["NESSIEDIR"] = nessie_test_config.config_dir


def pytest_sessionfinish(session: "Session", exitstatus: int) -> None:
    """Remove temporary config directory."""
    shutil.rmtree(nessie_test_config.config_dir)


def execute_cli_command(args: List[str], input_data: Optional[str] = None, ret_val: int = 0, output_string: bool = True) -> Any:
    """Execute a Nessie CLI command."""
    if output_string:
        return _run(args, input_data, ret_val).output

    return _run(args, input_data, ret_val)


def _run(args: List[str], input_data: Optional[str] = None, ret_val: int = 0) -> Result:
    result = CliRunner().invoke(cli.cli, args, input=input_data)
    if result.exit_code != ret_val:
        print(result.output)
        print(result)
    assert_that(result.exit_code).is_equal_to(ret_val)
    return result


def make_commit(
    key: str, table: Content, branch: str, head_hash: str = None, message: str = "test message", author: str = "nessie test"
) -> None:
    """Make commit through Nessie CLI."""
    if not head_hash:
        refs = {i.name: i.hash_ for i in ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)}
        head_hash = refs[branch]
    execute_cli_command(
        ["content", "commit", "--stdin", key, "--ref", branch, "-m", message, "-c", head_hash, "--author", author],
        input_data=ContentSchema().dumps(table),
    )


def reset_nessie_server_state() -> None:
    """Resets the Nessie Server to an initial, clean state for testing."""
    # Delete all branches
    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    for branch in branches:
        execute_cli_command(["branch", "-d", branch.name])

    # Delete all tags
    tags = ReferenceSchema().loads(execute_cli_command(["--json", "tag"]), many=True)
    for tag in tags:
        execute_cli_command(["tag", "-d", tag.name])

    # Note: This hash should match the java constant AbstractDatabaseAdapter.NO_ANCESTOR
    no_ancestor_hash = "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d"

    # Re-create the main branch from the "root" (a.k.a. no ancestor) hash
    execute_cli_command(["branch", "--force", "-o", no_ancestor_hash, "main", "main"])

    # Verify the re-created main branch
    branches = ReferenceSchema().loads(execute_cli_command(["--json", "branch"]), many=True)
    assert_that(branches).is_length(1)
    assert_that(branches[0].name).is_equal_to("main")
    assert_that(branches[0].hash_).is_equal_to(no_ancestor_hash)


def before_record_cb(request: Request) -> Optional[Request]:
    """VCR callback that instructs it to not record our "cleanup" requests."""
    if nessie_test_config.cleanup:
        return None
    return request


@pytest.fixture(scope="module")
def vcr_config() -> dict:
    """VCR config that adds a custom before_record_request callback."""
    nessie_test_config.cleanup = False
    return {
        "before_record_request": before_record_cb,
    }


@pytest.fixture(autouse=True)
def _clean_nessie_session_marker(request: "FixtureRequest", record_mode: str) -> None:
    """This pytest fixture is invoked for all test methods and cleans up the Nessie Server state.

    :param request Request object provided by the pytest framework
    :param record_mode Recording mode provided by the VCR plugin
    """
    # Cleaning the Nessie Server state is meaningful only when we are recording,
    # i.e. when the tests are running against a real server, not against VCR cassettes.
    # Consequently, when the tests will run against VCR cassettes, the pre-recorded
    # responses will simulate a "clean" server.
    if record_mode is None or record_mode == "none":
        return

    # Clean the server state for tests that have the @pytest.mark.clean_nessie_session annotation
    if request.node.get_closest_marker("vcr"):
        try:
            nessie_test_config.cleanup = True
            reset_nessie_server_state()
        finally:
            nessie_test_config.cleanup = False
