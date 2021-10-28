# -*- coding: utf-8 -*-
"""Main module."""

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
from ..auth import setup_auth
from ..model import Branch
from ..model import CommitMeta
from ..model import Contents
from ..model import ContentsSchema
from ..model import Entries
from ..model import EntriesSchema
from ..model import LogResponse
from ..model import LogResponseSchema
from ..model import Merge
from ..model import MergeSchema
from ..model import MultiContents
from ..model import MultiContentsSchema
from ..model import Operation
from ..model import Reference
from ..model import ReferenceSchema
from ..model import Tag
from ..model import Transplant
from ..model import TransplantSchema
from ..utils import format_key


class NessieClient(object):
    """Base Nessie Client."""

    def __init__(self: "NessieClient", config: confuse.Configuration) -> None:
        """Create a Nessie Client from known config."""
        self._base_url = config["endpoint"].get()
        self._ssl_verify = config["verify"].get(bool)
        self._auth = setup_auth(config)
        self._commit_id: str = cast(str, None)

        try:
            self._base_branch = config["default_branch"].get()
        except confuse.exceptions.NotFoundError:
            self._base_branch = None

    def list_references(self: "NessieClient") -> List[Reference]:
        """Fetch all known references.

        :return: list of Nessie References
        """
        references = all_references(self._base_url, self._auth, self._ssl_verify)
        return [ReferenceSchema().load(ref) for ref in references]

    def get_reference(self: "NessieClient", name: Optional[str]) -> Reference:
        """Fetch a ref.

        :param name: name of ref to fetch
        :return: Nessie reference
        """
        ref_obj = (
            get_reference(self._base_url, self._auth, name, self._ssl_verify)
            if name
            else get_default_branch(self._base_url, self._auth, self._ssl_verify)
        )
        ref = ReferenceSchema().load(ref_obj)
        return ref

    def create_branch(self: "NessieClient", branch: str, ref: str = None, hash_on_ref: str = None) -> Branch:
        """Create a branch.

        :param branch: name of new branch to create
        :param ref: branch or tag to fork from ('hash_on_ref' should be reachable from this reference)
        :param hash_on_ref: hash to assign 'branch' to
        :return: Nessie branch object
        """
        ref_json = ReferenceSchema().dump(Branch(branch, hash_on_ref))
        ref_obj = create_reference(self._base_url, self._auth, ref_json, ref, self._ssl_verify)
        return cast(Branch, ReferenceSchema().load(ref_obj))

    def delete_branch(self: "NessieClient", branch: str, hash_: str) -> None:
        """Delete a branch.

        :param branch: name of branch to delete
        :param hash_: hash of the branch
        """
        delete_branch(self._base_url, self._auth, branch, hash_, self._ssl_verify)

    def create_tag(self: "NessieClient", tag: str, ref: str, hash_on_ref: str = None) -> Tag:
        """Create a tag.

        :param tag: name of new tag to create
        :param ref: name of the reference via which 'hash_on_ref' is reachable
        :param hash_on_ref: hash to assign 'branch' to
        :return: Nessie tag object
        """
        ref_json = ReferenceSchema().dump(Tag(tag, hash_on_ref) if hash_on_ref else Tag(tag))
        ref_obj = create_reference(self._base_url, self._auth, ref_json, ref, self._ssl_verify)
        return cast(Tag, ReferenceSchema().load(ref_obj))

    def delete_tag(self: "NessieClient", tag: str, hash_: str) -> None:
        """Delete a tag.

        :param tag: name of tag to delete
        :param hash_: hash of the branch
        """
        delete_tag(self._base_url, self._auth, tag, hash_, self._ssl_verify)

    def list_keys(
        self: "NessieClient",
        ref: str,
        hash_on_ref: str = None,
        max_result_hint: Optional[int] = None,
        page_token: Optional[str] = None,
        query_expression: Optional[str] = None,
    ) -> Entries:
        """Fetch a list of all tables from a known branch.

        :param ref: name of branch
        :param hash_on_ref: hash on reference
        :param entity_types: list of types to filter keys on
        :param query_expression: A CEL expression that allows advanced filtering capabilities
        :return: list of Nessie table names
        """
        return EntriesSchema().load(
            list_tables(self._base_url, self._auth, ref, hash_on_ref, max_result_hint, page_token, query_expression, self._ssl_verify)
        )

    def get_values(self: "NessieClient", ref: str, *tables: str, hash_on_ref: Optional[str] = None) -> Generator[Contents, Any, None]:
        """Fetch a table from a known ref.

        :param ref: name of ref
        :param hash_on_ref: hash on reference
        :param tables: tables to fetch
        :return: Nessie Table
        """
        return (
            ContentsSchema().load(get_table(self._base_url, self._auth, ref, format_key(i), hash_on_ref, self._ssl_verify)) for i in tables
        )

    def commit(
        self: "NessieClient", branch: str, old_hash: str, reason: Optional[str] = None, author: Optional[str] = None, *ops: Operation
    ) -> Branch:
        """Modify a set of Nessie tables."""
        meta = CommitMeta(message=reason if reason else "")
        if author:
            meta.author = author
        ref_obj = commit(self._base_url, self._auth, branch, MultiContentsSchema().dumps(MultiContents(meta, list(ops))), old_hash)
        return cast(Branch, ReferenceSchema().load(ref_obj))

    def assign_branch(self: "NessieClient", branch: str, to_ref: str, to_ref_hash: str = None, old_hash: Optional[str] = None) -> None:
        """Assign a hash to a branch."""
        if not old_hash:
            old_hash = self.get_reference(branch).hash_
        assert old_hash is not None
        ref_json = ReferenceSchema().dumps(Branch(to_ref, to_ref_hash) if to_ref_hash else Branch(to_ref))
        assign_branch(self._base_url, self._auth, branch, ref_json, old_hash, self._ssl_verify)

    def assign_tag(self: "NessieClient", tag: str, to_ref: str, to_ref_hash: str = None, old_hash: Optional[str] = None) -> None:
        """Assign a hash to a tag."""
        if not old_hash:
            old_hash = self.get_reference(tag).hash_
        assert old_hash is not None
        ref_json = ReferenceSchema().dumps(Tag(to_ref, to_ref_hash) if to_ref_hash else Tag(to_ref))
        assign_tag(self._base_url, self._auth, tag, ref_json, old_hash, self._ssl_verify)

    def merge(
        self: "NessieClient", from_ref: str, onto_branch: str, from_hash: Optional[str] = None, old_hash: Optional[str] = None
    ) -> None:
        """Merge a branch into another branch."""
        if not old_hash:
            old_hash = self.get_reference(onto_branch).hash_
        assert old_hash is not None
        if not from_hash:
            from_hash = self.get_reference(from_ref).hash_
        merge_json = MergeSchema().dump(Merge(from_ref, str(from_hash)))
        merge(self._base_url, self._auth, onto_branch, merge_json, old_hash, self._ssl_verify)

    def cherry_pick(self: "NessieClient", branch: str, from_ref: str, old_hash: Optional[str] = None, *hashes: str) -> None:
        """Cherry pick a list of hashes to a branch."""
        if not old_hash:
            old_hash = self.get_reference(branch).hash_
        assert old_hash is not None
        transplant_json = TransplantSchema().dump(Transplant(from_ref, list(hashes)))
        cherry_pick(self._base_url, self._auth, branch, transplant_json, old_hash, self._ssl_verify)

    def get_log(
        self: "NessieClient", start_ref: str, hash_on_ref: Optional[str] = None, max_records: Optional[int] = None, **filtering_args: Any
    ) -> Generator[CommitMeta, Any, None]:
        """Fetch all logs starting at start_ref.

        start_ref can be any ref.

        Note:
            this will load the log into local memory and filter at the client. Currently there are no
            primitives in the REST api to limit logs or perform paging. TODO
        """
        page_token = filtering_args.get("pageToken", None)

        def fetch_logs(max: Optional[int], token: Optional[str] = page_token) -> LogResponse:
            if token:
                filtering_args["pageToken"] = token

            fetched_logs = list_logs(
                base_url=self._base_url,
                auth=self._auth,
                hash_on_ref=hash_on_ref,
                ref=start_ref,
                ssl_verify=self._ssl_verify,
                max_records=max,
                **filtering_args
            )
            log_schema = LogResponseSchema().load(fetched_logs)
            return log_schema

        log_schema = fetch_logs(max=max_records)

        def generator(log_schema: LogResponse, max_records: Optional[int]) -> Generator[CommitMeta, Any, None]:
            while True:
                for log in log_schema.operations:
                    yield log
                    if max_records is not None:
                        max_records -= 1
                        if max_records <= 0:
                            # yield only the requierd number of results, if server returns more records than expected.
                            break
                if not log_schema.has_more or (max_records is not None and max_records <= 0):
                    break
                log_schema = fetch_logs(max=max_records, token=log_schema.token)

        return generator(log_schema, max_records)

    def get_default_branch(self: "NessieClient") -> str:
        """Fetch default branch either from config if specified or from the server."""
        return self._base_branch if self._base_branch else self.get_reference(None).name
