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
"""Error handling tests for Nessie CLI."""

import pytest

from .conftest import execute_cli_command

# Note: tests in this file use custom VCR files for error server responses.
# Running these tests in recording mode will likely NOT produce the expected
# server responses. The related VCR files need to be reviewed and corrected
# manually.


@pytest.mark.vcr(record_mode="none")
def test_server_error_html() -> None:
    """Test the handling of 500 responses with HTML payload (unexpected, but possible)."""
    result = execute_cli_command(["--json", "remote", "show"], ret_val=1)
    assert "Internal Server Error" in result
    assert "500" in result


@pytest.mark.vcr(record_mode="none")
def test_server_error_json() -> None:
    """Test the handling of 500 responses with JSON payload."""
    result = execute_cli_command(["--json", "remote", "show"], ret_val=1)
    assert "Default branch isn't a branch" in result
    assert "500" in result
