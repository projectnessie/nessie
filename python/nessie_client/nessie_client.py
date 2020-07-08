# -*- coding: utf-8 -*-
"""Main module."""
from typing import cast
from typing import List

import confuse

from ._endpoints import all_branches
from ._endpoints import create_branch
from ._endpoints import delete_branch
from ._endpoints import get_branch
from ._endpoints import get_table
from ._endpoints import list_tables
from ._endpoints import merge_branch
from .auth import auth
from .model import Branch
from .model import BranchSchema
from .model import Table
from .model import TableSchema


class NessieClient:
    """Base Nessie Client."""

    def __init__(self: "NessieClient", config: confuse.Configuration) -> None:
        """Create a Nessie Client from known config."""
        self._base_url = config["endpoint"].get()
        self._username = config["auth"]["username"].get()
        self._password = config["auth"]["password"].get()
        self._ssl_verify = config["verify"].get(bool)
        self._token = auth(self._base_url, config)
        self._commit_id: str = cast(str, None)

    def list_branches(self: "NessieClient") -> List[Branch]:
        """Fetch all known branches.

        :return: list of Nessie Branches
        """
        branches = all_branches(self._token, self._base_url, self._ssl_verify)
        return [BranchSchema().load(i) for i in branches]

    def get_branch(self: "NessieClient", branch: str) -> Branch:
        """Fetch a branch.

        :param branch: name of branch to fetch
        :return: json Nessie branch
        """
        branch_obj = BranchSchema().load(get_branch(self._token, self._base_url, branch, self._ssl_verify))
        self._commit_id = branch_obj.id
        return branch_obj

    def create_branch(self: "NessieClient", branch: str, base_branch: str = None, reason: str = None) -> None:
        """Fetch all known branches.

        :param branch: name of new branch
        :param base_branch: name of branch to fork from
        :param reason: why this action is being performed (for log)
        """
        create_branch(self._token, self._base_url, branch, base_branch, reason, self._ssl_verify)

    def delete_branch(self: "NessieClient", branch: str, reason: str = None) -> None:
        """Delete a branch.

        :param branch: name of branch to delete
        :param reason: why this action is being performed (for log)
        """
        delete_branch(self._token, self._base_url, branch, self._commit_id, reason, self._ssl_verify)

    def merge_branch(
        self: "NessieClient", to_branch: str, from_branch: str, force: bool = False, reason: str = None
    ) -> None:
        """Merge a branch from_branch into to_branch.

        :param to_branch: name of branch to merge to
        :param from_branch: name of branch to merge from
        :param force: force merge
        :param reason: why this action is being performed (for log)
        """
        merge_branch(
            self._token, self._base_url, to_branch, from_branch, self._commit_id, force, reason, self._ssl_verify
        )

    def list_tables(self: "NessieClient", branch: str, namespace: str = None) -> List[str]:
        """Fetch a list of all tables from a known branch.

        :param branch: name of branch
        :param namespace: optional namespace of
        :return: list of Nessie table names
        """
        return list_tables(self._token, self._base_url, branch, namespace, self._ssl_verify)

    def get_tables(self: "NessieClient", branch: str, *tables: str) -> List[Table]:
        """Fetch a table from a known branch.

        :param branch: name of branch
        :return: Nessie Table
        """
        fetched_tables = [get_table(self._token, self._base_url, branch, i, self._ssl_verify) for i in tables]
        return [TableSchema().load(i) for i in fetched_tables]

    def create_table(self: "NessieClient", branch: str, table: Table, reason: str = None) -> None:
        """Create a Nessie table."""
        raise NotImplementedError("Create table has not been implemented")

    def delete_table(self: "NessieClient", branch: str, table: Table, reason: str = None) -> None:
        """Delete a Nessie table."""
        raise NotImplementedError("Delete table has not been implemented")

    def commit(self: "NessieClient", branch: str, *args: Table, reason: str = None) -> None:
        """Modify a set of Nessie tables."""
        raise NotImplementedError("Commit tables has not been implemented")
