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
import simplejson
from assertpy import assert_that
from click.testing import CliRunner
from click.testing import Result
from vcr.config import VCR
from vcr.request import Request
from vcr.serializers import yamlserializer

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


def pytest_sessionstart(session: pytest.Session) -> None:
    """Setup a fresh temporary config directory for tests."""
    nessie_test_config.config_dir = tempfile.mkdtemp() + "/"
    # Instruct Confuse to keep Nessie config file in the temp location:
    os.environ["NESSIEDIR"] = nessie_test_config.config_dir


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Remove temporary config directory."""
    shutil.rmtree(nessie_test_config.config_dir)


def execute_cli_command_raw(args: List[str], input_data: Optional[str] = None, ret_val: int = 0) -> Result:
    """Execute a Nessie CLI command."""
    result = CliRunner().invoke(cli.cli, args, input=input_data)
    if result.exit_code != ret_val:
        print(result.stdout)
        print(result.stderr)
        print(result)  # exception
    assert_that(result.exit_code).is_equal_to(ret_val)
    return result


def execute_cli_command(args: List[str], input_data: Optional[str] = None, ret_val: int = 0) -> str:
    """Execute a Nessie CLI command and return its STDOUT."""
    return execute_cli_command_raw(args, input_data=input_data, ret_val=ret_val).stdout


def ref_hash(ref: str) -> str:
    """Get the hash for a reference."""
    refs = ReferenceSchema().loads(execute_cli_command(["--json", "branch", "-l"]), many=True)
    return next(i.hash_ for i in refs if i.name == ref)


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


def _sort_json(value: str) -> str:
    """Parses JSON and serializes it again with a fixed key order."""
    try:
        json = simplejson.loads(value)
        sorted_json = simplejson.dumps(json, sort_keys=True)
        return sorted_json
    except ValueError:
        return value


def _reformat_body(obj: dict) -> None:
    """Sorts JSON objects embedded in HTTP payload by their keys."""
    body = obj["body"]
    sorted_json = None
    if body:
        if isinstance(body, str):
            sorted_json = _sort_json(body)
            obj["body"] = sorted_json
        elif isinstance(body, dict):
            sorted_json = _sort_json(body["string"])
            body["string"] = sorted_json
        else:
            raise RuntimeError("Unexpected cassette structure")

    # Adjust content length if changed
    if sorted_json:
        headers = obj["headers"]
        for k in headers:
            if k.lower() == "content-length":  # might have different case in cassettes
                headers[k] = [str(len(sorted_json))]  # one-element list
                break


class _NessieSortingSerializer:
    @staticmethod
    def deserialize(cassette_string: str) -> Any:
        """Delegates to the default yaml cassette serializer."""
        return yamlserializer.deserialize(cassette_string)

    @staticmethod
    def serialize(cassette_dict: dict) -> str:
        """Reformats JSON payloads in requests and responses.

        This is to avoid spurious changes in cassette yaml files under source control.
        """
        if "interactions" in cassette_dict:
            for i in cassette_dict["interactions"]:
                _reformat_body(i["request"])
                _reformat_body(i["response"])

        return yamlserializer.serialize(cassette_dict)


def pytest_recording_configure(config: pytest.Config, vcr: VCR) -> None:
    """Callback invoked by pytest_recording. We register a custom VCR serializer here."""
    ser_name = "nessie_sorted_json"
    vcr.register_serializer(ser_name, _NessieSortingSerializer())
    vcr.serializer = ser_name


@pytest.fixture(scope="module")
def vcr_config() -> dict:
    """VCR config that adds a custom before_record_request callback."""
    nessie_test_config.cleanup = False
    return {
        "before_record_request": before_record_cb,
    }


@pytest.fixture(autouse=True)
def _clean_nessie_session_marker(request: pytest.FixtureRequest, record_mode: str) -> None:
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

    vcr_marker = request.node.get_closest_marker("vcr")

    # Clean the server state for tests that have the @pytest.mark.vcr annotation
    if vcr_marker:
        if "record_mode" in vcr_marker.kwargs:
            # if we have record_mode set in @pytest.mark.vcr annotation, we test that first
            test_record_mode = vcr_marker.kwargs["record_mode"]

            if test_record_mode is None:
                raise RuntimeError("record_mode can't be None or empty if set.")

            if test_record_mode == "none":
                return

        try:
            nessie_test_config.cleanup = True
            reset_nessie_server_state()
        finally:
            nessie_test_config.cleanup = False
