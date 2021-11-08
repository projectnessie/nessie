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
"""Authentication tests for Nessi CLI."""
import pytest

from .conftest import execute_cli_command


@pytest.fixture(scope="module")
def vcr_config() -> dict:
    """Configures VCR to also match on HTTP headers."""
    return {"match_on": ["method", "scheme", "host", "port", "path", "query", "headers"]}


@pytest.mark.vcr
def test_bearer_auth() -> None:
    """Test the "remote show" command with bearer authentication."""
    assert "main" in execute_cli_command(["--json", "--auth-token", "test_token_123", "remote", "show"])


@pytest.mark.vcr
def test_bearer_auth_invalid() -> None:
    """Test any command with an invalid bearer authentication token."""
    result = execute_cli_command(["--json", "--auth-token", "invalid", "remote", "show"], ret_val=1)
    assert "Client Error" in result
    assert "Unauthorized" in result
