#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `nessie_client` package."""
import pytest
import requests_mock
import simplejson as json

from nessie_client import init
from nessie_client.error import NessieConflictException
from nessie_client.model import Branch


def test_client_interface(requests_mock: requests_mock) -> None:
    """Test client object."""
    requests_mock.post("http://localhost:19120/api/v1/login", text=json.dumps({"token": "12345"}))
    client = init()
    assert client._token == "12345"
    requests_mock.get("http://localhost:19120/api/v1/objects", text=json.dumps([]))
    branches = client.list_branches()
    assert len(branches) == 0

    def callback(request, context):  # noqa
        assert json.loads(request.text) == {"name": "master", "id": None}
        return ""

    requests_mock.post("http://localhost:19120/api/v1/objects/master", text=callback)
    assert client.create_branch("master") is None

    requests_mock.get("http://localhost:19120/api/v1/objects", text=json.dumps([{"name": "master", "id": None}]))
    branches = client.list_branches()
    assert len(branches) == 1

    def callback(request, context):  # noqa
        assert json.loads(request.text) == {"name": "test", "id": "master"}
        return ""

    requests_mock.post("http://localhost:19120/api/v1/objects/test", text=callback)
    assert client.create_branch("test", "master") is None

    requests_mock.get(
        "http://localhost:19120/api/v1/objects",
        text=json.dumps([{"name": "master", "id": None}, {"name": "test", "id": None}]),
    )
    branches = client.list_branches()
    assert len(branches) == 2
    assert [i.name for i in branches] == ["master", "test"]


@pytest.mark.e2e
def test_client_interface_e2e() -> None:
    """Test client object against live server."""
    client = init()
    assert isinstance(client._token, str)
    branches = client.list_branches()
    assert len(branches) == 1
    assert branches[0] == Branch("master", branches[0].id)
    master_commit = branches[0].id
    with pytest.raises(NessieConflictException):
        client.create_branch("master")
    client.create_branch("test", "master")
    branches = client.list_branches()
    assert len(branches) == 2
    assert branches[0] == Branch("master", master_commit)
    assert branches[1] == Branch("test", master_commit)
    branch = client.get_branch("test")
    tables = client.list_tables(branch.name)
    assert isinstance(tables, list)
    assert len(tables) == 0
    client.delete_branch("test")
    branches = client.list_branches()
    assert len(branches) == 1
