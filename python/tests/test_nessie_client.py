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
import pytest

from pynessie import init
from pynessie.error import NessieConflictException
from pynessie.model import Branch
from pynessie.model import Entries


@pytest.mark.vcr
def test_client_interface_e2e() -> None:
    """Test client object against live server."""
    client = init()
    assert isinstance(client.get_base_url(), str)
    assert client.get_base_url() == "http://localhost:19120/api/v1"
    references = client.list_references().references
    assert len(references) == 1
    assert references[0] == Branch("main", references[0].hash_)
    main_name = references[0].name
    main_commit = references[0].hash_
    with pytest.raises(NessieConflictException):
        client.create_branch("main")
    created_reference = client.create_branch("test", main_name, main_commit)
    references = client.list_references().references
    assert len(references) == 2
    assert next(i for i in references if i.name == "main") == Branch("main", main_commit)
    assert next(i for i in references if i.name == "test") == Branch("test", main_commit)
    reference = client.get_reference("test")
    assert created_reference == reference
    tables = client.list_keys(reference.name, reference.hash_)
    assert isinstance(tables, Entries)
    assert len(tables.entries) == 0
    client.delete_branch("test", main_commit)
    references = client.list_references().references
    assert len(references) == 1
