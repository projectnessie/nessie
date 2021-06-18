#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `pynessie` package."""
import pytest
from packaging.version import Version

from pynessie import __min_remote_version__, __version__, init
from pynessie._endpoints import _check_nessie_version_headers
from pynessie.error import NessieConflictException
from pynessie.model import Branch
from pynessie.model import Entries


@pytest.mark.vcr
def test_client_interface_e2e() -> None:
    """Test client object against live server."""
    client = init()
    assert isinstance(client._base_url, str)
    references = client.list_references()
    assert len(references) == 1
    assert references[0] == Branch("main", references[0].hash_)
    main_commit = references[0].hash_
    with pytest.raises(NessieConflictException):
        client.create_branch("main")
    created_reference = client.create_branch("test", main_commit)
    references = client.list_references()
    assert len(references) == 2
    assert next(i for i in references if i.name == "main") == Branch("main", main_commit)
    assert next(i for i in references if i.name == "test") == Branch("test", main_commit)
    reference = client.get_reference("test")
    assert created_reference == reference
    tables = client.list_keys(reference.name)
    assert isinstance(tables, Entries)
    assert len(tables.entries) == 0
    client.delete_branch("test", main_commit)
    references = client.list_references()
    assert len(references) == 1


def test_nessie_version() -> None:
    """Test version requirements."""
    # No Nessie-Version header from server
    with pytest.raises(Exception) as e:
        _check_nessie_version_headers({}, Version("1.2.3"))
    assert str(e.value) == "Server replied without the required Nessie-Version header. pynessie requires Nessie-Server version >= 0.7.0"

    # Empty Nessie-Version header from server
    with pytest.raises(Exception) as e:
        _check_nessie_version_headers({"Nessie-Version": ""}, Version("1.2.3"))
    assert str(e.value) == "Server replied without the required Nessie-Version header. pynessie requires Nessie-Server version >= 0.7.0"

    # Nessie-Server version is one patch version too new
    _check_nessie_version_headers({"Nessie-Version": "88.44.22"}, Version("88.0.0"))

    # Nessie-Server version is one patch version too old
    with pytest.raises(Exception) as e:
        _check_nessie_version_headers({"Nessie-Version": "88.44.22"}, Version("88.44.23"))
    assert str(e.value) == f"Nessie-Server is running version 88.44.22, which is incompatible to pynessie {__version__}"

    # Nessie-Server version is one minor version too new
    _check_nessie_version_headers({"Nessie-Version": "88.45.0"}, Version("88.0.0"))

    # Nessie-Server version is one minor version too old
    with pytest.raises(Exception) as e:
        _check_nessie_version_headers({"Nessie-Version": "88.43.0"}, Version("88.44.0"))
    assert str(e.value) == f"Nessie-Server is running version 88.43.0, which is incompatible to pynessie {__version__}"

    # Nessie-Server version is one major version too new
    _check_nessie_version_headers({"Nessie-Version": "89.0.0"}, Version("88.0.0"))

    # Nessie-Server version is one major version too old
    with pytest.raises(Exception) as e:
        _check_nessie_version_headers({"Nessie-Version": "87.44.22"}, Version("88.0.0"))
    assert str(e.value) == f"Nessie-Server is running version 87.44.22, which is incompatible to pynessie {__version__}"

    # Verify that server can safely sent versions with -SNAPSHOT
    _check_nessie_version_headers({"Nessie-Version": "1.2.3-SNAPSHOT"}, Version("1.2.3"))

    # Sanity - check that the pynessie global vars make sense
    _check_nessie_version_headers({"Nessie-Version": f"{__version__}"}, __min_remote_version__)
