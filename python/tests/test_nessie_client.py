#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `nessie_client` package."""
import pytest
import requests_mock
import simplejson as json

from nessie_client import init
from nessie_client.error import NessieConflictException
from nessie_client.model import Reference


class Fake:
    """Fake out auth object."""

    def __init__(self: "Fake") -> None:
        """Fake out auth object."""
        self.headers = dict()


def test_client_interface(requests_mock: requests_mock) -> None:
    """Test client object."""
    client = init()
    requests_mock.get("http://localhost:19120/api/v1/trees", text=json.dumps([]))
    references = client.list_references()
    assert len(references) == 0

    requests_mock.post("http://localhost:19120/api/v1/trees/branch/main")
    assert client.create_branch("main") is None

    requests_mock.get(
        "http://localhost:19120/api/v1/trees",
        text=json.dumps([{"name": "main", "type": "BRANCH", "hash": "1234567890abcdef"}]),
    )
    references = client.list_references()
    assert len(references) == 1

    requests_mock.post("http://localhost:19120/api/v1/trees/branch/test/1234567890fedcba")
    assert client.create_branch("test", "1234567890fedcba") is None

    requests_mock.get(
        "http://localhost:19120/api/v1/trees",
        text=json.dumps(
            [
                {"name": "main", "type": "BRANCH", "hash": "1234567890abcdef"},
                {"name": "test", "type": "BRANCH", "hash": "1234567890fedcba"},
            ]
        ),
    )
    references = client.list_references()
    assert len(references) == 2
    assert [i.name for i in references] == ["main", "test"]


@pytest.mark.e2e
def test_client_interface_e2e() -> None:
    """Test client object against live server."""
    client = init()
    assert isinstance(client._token, str)
    references = client.list_references()
    assert len(references) == 1
    assert references[0] == Reference("main", "BRANCH", references[0].hash_)
    main_commit = references[0].id
    with pytest.raises(NessieConflictException):
        client.create_branch("main")
    client.create_branch("test", main_commit)
    references = client.list_references()
    assert len(references) == 2
    assert references[0] == Reference("main", "BRANCH", main_commit)
    assert references[1] == Reference("test", "BRANCH", main_commit)
    reference = client.get_branch("test")
    tables = client.list_references(reference.name)
    assert isinstance(tables, list)
    assert len(tables) == 0
    client.delete_branch("test")
    references = client.list_references()
    assert len(references) == 1
