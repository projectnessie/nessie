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
from ._endpoints import create_reference
from ._endpoints import delete_branch
from ._endpoints import delete_tag
from ._endpoints import get_default_branch
from ._endpoints import get_reference
from ._endpoints import get_table
from ._endpoints import list_logs
from ._endpoints import list_tables
from ._endpoints import merge
from .model import Branch
from .model import CommitMeta
from .model import Contents
from .model import ContentsKey
from .model import ContentsSchema
from .model import Entries
from .model import EntriesSchema
from .model import LogResponse
from .model import LogResponseSchema
from .model import Merge
from .model import MergeSchema
from .model import MultiContents
from .model import MultiContentsSchema
from .model import Operation
from .model import Reference
from .model import ReferenceSchema
from .model import Tag
from .model import Transplant
from .model import TransplantSchema

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

    def get_reference(self: "NessieClient", name: Optional[str]) -> Reference:
        """Fetch a ref.

        :param name: name of ref to fetch
        :return: Nessie reference
        """
        ref_obj = get_reference(self._base_url, name, self._ssl_verify) if name else get_default_branch(self._base_url, self._ssl_verify)
        ref = ReferenceSchema().load(ref_obj)
        return ref

    def create_branch(self: "NessieClient", branch: str, ref: str = None) -> Branch:
        """Create a branch.

        :param branch: name of new branch
        :param ref: ref to fork from
        :return: Nessie branch object
        """
        ref_json = ReferenceSchema().dump(Branch(branch, ref))
        ref_obj = create_reference(self._base_url, ref_json, self._ssl_verify)
        return cast(Branch, ReferenceSchema().load(ref_obj))

    def delete_branch(self: "NessieClient", branch: str, hash_: str) -> None:
        """Delete a branch.

        :param branch: name of branch to delete
        :param hash_: hash of the branch
        """
        delete_branch(self._base_url, branch, hash_, self._ssl_verify)

    def create_tag(self: "NessieClient", tag: str, ref: str = None) -> Tag:
        """Create a tag.

        :param tag: name of new tag
        :param ref: ref to fork from
        :return: Nessie tag object
        """
        ref_json = ReferenceSchema().dump(Tag(tag, ref))
        ref_obj = create_reference(self._base_url, ref_json, self._ssl_verify)
        return cast(Tag, ReferenceSchema().load(ref_obj))

    def delete_tag(self: "NessieClient", tag: str, hash_: str) -> None:
        """Delete a tag.

        :param tag: name of tag to delete
        :param hash_: hash of the branch
        """
        delete_tag(self._base_url, tag, hash_, self._ssl_verify)

    def list_keys(
        self: "NessieClient",
        ref: str,
        max_result_hint: Optional[int] = None,
        page_token: Optional[str] = None,
        entity_types: Optional[list] = None,
    ) -> Entries:
        """Fetch a list of all tables from a known branch.

        :param ref: name of branch
        :param entity_types: list of types to filter keys on
        :return: list of Nessie table names
        """
        if not entity_types:
            entity_types = list()
        return EntriesSchema().load(list_tables(self._base_url, ref, max_result_hint, page_token, entity_types, self._ssl_verify))

    def get_values(self: "NessieClient", ref: str, *tables: str) -> Generator[Contents, Any, None]:
        """Fetch a table from a known ref.

        :param ref: name of ref
        :param tables: tables to fetch
        :return: Nessie Table
        """
        return (ContentsSchema().load(get_table(self._base_url, ref, _format_key(i), self._ssl_verify)) for i in tables)

    def commit(self: "NessieClient", branch: str, old_hash: str, reason: Optional[str] = None, *ops: Operation) -> Branch:
        """Modify a set of Nessie tables."""
        meta = CommitMeta(message=reason if reason else "")
        ref_obj = commit(self._base_url, branch, MultiContentsSchema().dumps(MultiContents(meta, list(ops))), old_hash)
        return cast(Branch, ReferenceSchema().load(ref_obj))

    def assign_branch(self: "NessieClient", branch: str, to_ref: str, old_hash: Optional[str] = None) -> None:
        """Assign a hash to a branch."""
        if not old_hash:
            old_hash = self.get_reference(branch).hash_
        assert old_hash is not None
        branch_json = ReferenceSchema().dumps(Branch(branch, self.get_reference(to_ref).hash_))
        assign_branch(self._base_url, branch, branch_json, old_hash, self._ssl_verify)

    def assign_tag(self: "NessieClient", tag: str, to_ref: str, old_hash: Optional[str] = None) -> None:
        """Assign a hash to a tag."""
        if not old_hash:
            old_hash = self.get_reference(tag).hash_
        assert old_hash is not None
        tag_json = ReferenceSchema().dumps(Tag(tag, self.get_reference(to_ref).hash_))
        assign_tag(self._base_url, tag, tag_json, old_hash, self._ssl_verify)

    def merge(self: "NessieClient", branch: str, to_branch: str, old_hash: Optional[str] = None) -> None:
        """Merge a branch into another branch."""
        if not old_hash:
            old_hash = self.get_reference(branch).hash_
        assert old_hash is not None
        to_hash = self.get_reference(to_branch).hash_
        assert to_hash is not None
        merge_json = MergeSchema().dump(Merge(to_hash))
        merge(self._base_url, branch, merge_json, old_hash, self._ssl_verify)

    def cherry_pick(self: "NessieClient", branch: str, old_hash: Optional[str] = None, *hashes: str) -> None:
        """Cherry pick a list of hashes to a branch."""
        if not old_hash:
            old_hash = self.get_reference(branch).hash_
        assert old_hash is not None
        transplant_json = TransplantSchema().dump(Transplant(list(hashes)))
        cherry_pick(self._base_url, branch, transplant_json, old_hash, self._ssl_verify)

    def get_log(
        self: "NessieClient", start_ref: str, max_result_hint: Optional[int] = None, page_token: Optional[str] = None
    ) -> Generator[CommitMeta, Any, None]:
        """Fetch all logs starting at start_ref.

        start_ref can be any ref.

        Note:
            this will load the log into local memory and filter at the client. Currently there are no
            primitives in the REST api to limit logs or perform paging. TODO
        """

        def fetch_logs(token: Optional[str] = page_token) -> LogResponse:
            fetched_logs = list_logs(self._base_url, start_ref, max_result_hint, token, self._ssl_verify)
            log_schema = LogResponseSchema().load(fetched_logs)
            return log_schema

        log_schema = fetch_logs()

        def generator(log_schema: LogResponse) -> Generator[CommitMeta, Any, None]:
            while True:
                for i in log_schema.operations:
                    yield i
                if log_schema.has_more:
                    log_schema = fetch_logs(log_schema.token)
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
