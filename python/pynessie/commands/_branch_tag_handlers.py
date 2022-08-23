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

"""Branch and tag common functions."""

from typing import Optional

import click

from pynessie.client import NessieClient
from pynessie.error import NessieConflictException
from pynessie.model import Branch, ReferenceSchema, Tag, split_into_reference_and_hash


def handle_branch_tag(
    nessie: NessieClient,
    list_references: bool,
    delete_reference: bool,
    ref_name: str,
    hash_on_ref: Optional[str],
    base_ref: str,
    is_branch: bool,
    json: bool,
    force: bool,
    verbose: bool,
    fetch_all: bool,
    expected_hash: Optional[str] = None,
) -> str:
    """Perform branch/tag actions.

    :param nessie NessieClient to use
    :param list_references the -l option choice
    :param delete_reference the -d option choice
    :param ref_name the name of the reference to create/assign/delete
    :param hash_on_ref a specific hash (reachable from base_ref) to be used for creating the new branch or tag instead
           of the HEAD of base_ref
    :param base_ref existing branch or tag to act at the base for creating the new branch or tag
    :param is_branch whether the operation is about a branch (true) or tag (false)
    :param json the --json option choice
    :param force the -f option choice
    :param verbose the -v option choice
    :param fetch_all the -x option to fetch additional metadata for a branch/tag
    :param expected_hash hash whose existence needs to be checked (on the server side) before performing the operation
    """
    if list_references or (not list_references and not delete_reference and not ref_name and not base_ref):
        return _handle_list(nessie, json, verbose, is_branch, ref_name, fetch_all)
    if delete_reference:
        if not hash_on_ref:
            hash_on_ref = nessie.get_reference(ref_name).hash_ or "fail"
        getattr(nessie, "delete_{}".format("branch" if is_branch else "tag"))(ref_name, hash_on_ref)
    else:  # create or force assign
        # use locally configured default branch as base_ref by default
        if not base_ref:
            base_ref = nessie.get_default_branch()
            hash_on_ref_arg = None
        else:
            base_ref, hash_on_ref_arg = split_into_reference_and_hash(base_ref)

        # use the current HEAD of base_ref as the hash for the new branch/tag by default
        if not hash_on_ref:
            hash_on_ref = hash_on_ref_arg if hash_on_ref_arg else nessie.get_reference(base_ref).hash_
        elif hash_on_ref_arg and hash_on_ref_arg != hash_on_ref:
            raise click.exceptions.BadOptionUsage(
                "hash_on_ref", "commit-hash provided via 'base-ref' and 'hash-on-ref', use only one of those"
            )

        # try creating a _new_ branch/tag first
        try:
            getattr(nessie, "create_{}".format("branch" if is_branch else "tag"))(ref_name, base_ref, hash_on_ref)
        except NessieConflictException as conflict:
            # NessieConflictException means the branch/tag already exists - force reassignment if requested
            if force:
                getattr(nessie, "assign_{}".format("branch" if is_branch else "tag"))(ref_name, base_ref, hash_on_ref, expected_hash)
            else:
                raise conflict
    return ""


def _handle_list(nessie: NessieClient, json: bool, verbose: bool, is_branch: bool, ref_name: str, fetch_all: bool) -> str:
    results = nessie.list_references(fetch_all=fetch_all).references
    kept_results = [ref for ref in results if isinstance(ref, (Branch if is_branch else Tag))]
    if ref_name:
        kept_results = [i for i in kept_results if i.name == ref_name]
    if json:
        return _handle_json_output(kept_results, ref_name)
    return _handle_normal_output(kept_results, verbose, nessie.get_default_branch(), fetch_all)


def _handle_json_output(input_data: list, ref_name: str) -> str:
    if ref_name and len(input_data) == 1:
        return ReferenceSchema().dumps(input_data[0], many=False)
    if ref_name and len(input_data) < 1:
        return "{}"
    return ReferenceSchema().dumps(input_data, many=True)


def _handle_normal_output(input_data: list, verbose: bool, default_branch: str, show_additional_info: bool = False) -> str:
    output = ""
    max_width = max((len(i.name) for i in input_data), default=0)
    for x in input_data:
        additional_info = ""
        if show_additional_info and x.metadata:
            commits_ahead = x.metadata.num_commits_ahead if x.metadata.num_commits_ahead else "?"
            commits_behind = x.metadata.num_commits_behind if x.metadata.num_commits_behind else "?"
            total_commits = x.metadata.num_total_commits if x.metadata.num_total_commits else "?"
            ancestor = x.metadata.common_ancestor_hash if x.metadata.common_ancestor_hash else "?"
            msg = (
                x.metadata.commit_meta_of_head.message[:35] + "..."
                if len(x.metadata.commit_meta_of_head.message) >= 35
                else x.metadata.commit_meta_of_head.message
            )
            head = (
                f"[author: {x.metadata.commit_meta_of_head.author} | commitTime: {x.metadata.commit_meta_of_head.commitTime} | msg: {msg}]"
                if x.metadata.commit_meta_of_head
                else "?"
            )
            additional_info = (
                f" [total commits: {total_commits} | commits behind: {commits_behind} | "
                f"commits ahead: {commits_ahead} | ancestor: {ancestor} | HEAD: {head}]"
            )

        next_row = "{}{}{}{}{}\n".format(
            "*".ljust(2) if x.name == default_branch else "  ",
            x.name.ljust(max_width + 1),
            " " if verbose else "",
            x.hash_ if verbose else "",
            additional_info,
        )
        output += click.style(next_row, fg="yellow") if x.name == default_branch else next_row
    return output
