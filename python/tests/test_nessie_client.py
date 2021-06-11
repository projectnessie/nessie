#!/usr/bin/env python
# -*- coding: utf-8 -*-
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


@pytest.mark.vcr
def test_create_branch_ref_being_branch_name() -> None:
    """Test creating a branch where the ref is not a hash but a branch name."""
    branch_name = "create_branch_ref_being_branch_name"
    client = init()
    references = client.list_references()
    assert len(references) == 1
    assert references[0] == Branch("main", references[0].hash_)
    created_reference = client.create_branch(branch_name, ref="main")
    references = client.list_references()
    assert len(references) == 2
    assert created_reference == client.get_reference(branch_name)
    client.delete_branch(branch_name, created_reference.hash_)
    references = client.list_references()
    assert len(references) == 1


@pytest.mark.vcr
def test_create_branch_without_ref() -> None:
    """Test creating a branch where the ref is not set."""
    branch_name = "create_branch_without_ref"
    client = init()
    references = client.list_references()
    assert len(references) == 1
    assert references[0] == Branch("main", references[0].hash_)
    created_reference = client.create_branch(branch_name, ref=None)
    references = client.list_references()
    assert len(references) == 2
    assert created_reference == client.get_reference(branch_name)
    client.delete_branch(branch_name, created_reference.hash_)
    references = client.list_references()
    assert len(references) == 1


@pytest.mark.vcr
def test_create_tag_ref_being_branch_name() -> None:
    """Test creating a tag where the ref is not a hash but a branch name."""
    tag_name = "create_tag_ref_being_branch_name"
    client = init()
    references = client.list_references()
    assert len(references) == 1
    assert references[0] == Branch("main", references[0].hash_)
    created_reference = client.create_tag(tag_name, ref="main")
    references = client.list_references()
    assert len(references) == 2
    assert created_reference == client.get_reference(tag_name)
    client.delete_tag(tag_name, created_reference.hash_)
    references = client.list_references()
    assert len(references) == 1
