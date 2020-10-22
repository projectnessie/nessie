# -*- coding: utf-8 -*-
"""Main module."""
import re
from typing import Any
from typing import cast
from typing import Generator
from typing import List
from typing import Optional

import confuse

from ._endpoints import all_references
from ._endpoints import assign_branch
from ._endpoints import assign_tag
from ._endpoints import cherry_pick
from ._endpoints import commit
from ._endpoints import create_branch
from ._endpoints import create_tag
from ._endpoints import delete_branch
from ._endpoints import delete_tag
from ._endpoints import get_default_branch
from ._endpoints import get_reference
from ._endpoints import get_table
from ._endpoints import list_logs
from ._endpoints import list_tables
from ._endpoints import merge
from .model import CommitMeta
from .model import Contents
from .model import ContentsKey
from .model import ContentsSchema
from .model import Entries
from .model import EntriesSchema
from .model import LogResponse
from .model import LogResponseSchema
from .model import MultiContents
from .model import MultiContentsSchema
from .model import Reference
from .model import ReferenceSchema

_dot_regex = re.compile('\\.(?=([^"]*"[^"]*")*[^"]*$)')


class NessieClient(object):
    """Base Nessie Client."""

    def __init__(self: "NessieClient", config: confuse.Configuration) -> None:
        """Create a Nessie Client from known config."""
        self._base_url = config["endpoint"].get()
        self._ssl_verify = config["verify"].get(bool)
        self._commit_id: str = cast(str, None)

        try:
            self._base_branch = config["default_branch"].get()
        except confuse.exceptions.NotFoundError:
            self._base_branch = None

    def list_references(self: "NessieClient") -> List[Reference]:
        """Fetch all known references.

        :return: list of Nessie References
        """
        references = all_references(self._base_url, self._ssl_verify)
        return [ReferenceSchema().load(ref) for ref in references]

    def get_reference(self: "NessieClient", ref: Optional[str]) -> Reference:
        """Fetch a ref.

        :param ref: name of ref to fetch
        :return: json Nessie reference
        """
        ref_obj = get_reference(self._base_url, ref, self._ssl_verify) if ref else get_default_branch(self._base_url, self._ssl_verify)
        branch_obj = ReferenceSchema().load(ref_obj)
        return branch_obj

    def create_branch(self: "NessieClient", branch: str, ref: str = None) -> None:
        """Create a branch.

        :param branch: name of new branch
        :param ref: ref to fork from
        """
        create_branch(self._base_url, branch, ref, self._ssl_verify)

    def delete_branch(self: "NessieClient", branch: str, hash_: str) -> None:
        """Delete a branch.

        :param branch: name of branch to delete
        :param hash_: hash of the branch
        """
        delete_branch(self._base_url, branch, hash_, self._ssl_verify)

    def create_tag(self: "NessieClient", tag: str, ref: str = None) -> None:
        """Create a tag.

        :param tag: name of new tag
        :param ref: ref to fork from
        """
        create_tag(self._base_url, tag, ref, self._ssl_verify)

    def delete_tag(self: "NessieClient", tag: str, hash_: str) -> None:
        """Delete a tag.

        :param tag: name of tag to delete
        :param hash_: hash of the branch
        """
        delete_tag(self._base_url, tag, hash_, self._ssl_verify)

    def list_keys(self: "NessieClient", ref: str) -> Entries:
        """Fetch a list of all tables from a known branch.

        :param ref: name of branch
        :return: list of Nessie table names
        """
        return EntriesSchema().load(list_tables(self._base_url, ref, self._ssl_verify))

    def get_values(self: "NessieClient", ref: str, *tables: str) -> Generator[Contents, Any, None]:
        """Fetch a table from a known ref.

        :param ref: name of ref
        :param tables: tables to fetch
        :return: Nessie Table
        """
        return (ContentsSchema().load(get_table(self._base_url, ref, _format_key(i), self._ssl_verify)) for i in tables)

    def commit(
        self: "NessieClient",
        branch: str,
        args: MultiContents,
        reason: Optional[str] = None,
        old_hash: Optional[str] = None,
    ) -> None:
        """Modify a set of Nessie tables."""
        print(args)
        commit(self._base_url, branch, MultiContentsSchema().dumps(args), reason, old_hash)

    def assign_branch(self: "NessieClient", branch: str, to_ref: str, old_hash: Optional[str] = None) -> None:
        """Assign a hash to a branch."""
        if not old_hash:
            old_hash = self.get_reference(branch).hash_
        to_hash = self.get_reference(to_ref).hash_
        assign_branch(self._base_url, branch, old_hash, to_hash, self._ssl_verify)

    def assign_tag(self: "NessieClient", tag: str, to_ref: str, old_hash: Optional[str] = None) -> None:
        """Assign a hash to a tag."""
        if not old_hash:
            old_hash = self.get_reference(tag).hash_
        to_hash = self.get_reference(to_ref).hash_
        assign_tag(self._base_url, tag, old_hash, to_hash, self._ssl_verify)

    def merge(self: "NessieClient", branch: str, to_branch: str, old_hash: Optional[str] = None) -> None:
        """Merge a branch into another branch."""
        if not old_hash:
            old_hash = self.get_reference(branch).hash_
        to_hash = self.get_reference(to_branch).hash_
        merge(self._base_url, branch, to_hash, old_hash, self._ssl_verify)

    def cherry_pick(self: "NessieClient", branch: str, old_hash: Optional[str] = None, *hashes: str) -> None:
        """Cherry pick a list of hashes to a branch."""
        if not old_hash:
            old_hash = self.get_reference(branch).hash_
        cherry_pick(self._base_url, branch, old_hash, self._ssl_verify, *hashes)

    def get_log(self: "NessieClient", start_ref: str) -> Generator[CommitMeta, Any, None]:
        """Fetch all logs starting at start_ref.

        start_ref can be any ref.

        Note:
            this will load the log into local memory and filter at the client. Currently there are no
            primitives in the REST api to limit logs or perform paging. TODO
        """

        def fetch_logs() -> LogResponse:
            fetched_logs = list_logs(self._base_url, start_ref, self._ssl_verify)
            log_schema = LogResponseSchema().load(fetched_logs)
            return log_schema

        log_schema = fetch_logs()

        def generator(log_schema: LogResponse) -> Generator[CommitMeta, Any, None]:
            while True:
                for i in log_schema.operations:
                    yield i
                if log_schema.has_more:
                    log_schema = fetch_logs()
                else:
                    break

        return generator(log_schema)

    def get_default_branch(self: "NessieClient") -> str:
        """Fetch default branch either from config if specified or from the server."""
        return self._base_branch if self._base_branch else self.get_reference(None).name


def _format_key(raw_key: str) -> str:
    elements = _dot_regex.split(raw_key)
    return ".".join(i.replace(".", "\0") for i in elements if i)


def _contents_key(raw_key: str) -> ContentsKey:
    elements = _dot_regex.split(raw_key)
    return ContentsKey([i for i in elements if i])
