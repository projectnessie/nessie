# -*- coding: utf-8 -*-
from typing import Optional

import click

from . import NessieClient
from .error import NessieConflictException
from .model import Branch
from .model import ReferenceSchema
from .model import Tag


def handle_branch_tag(
    nessie: NessieClient,
    list_references: bool,
    delete_reference: bool,
    branch: str,
    hash_on_ref: Optional[str],
    base_ref: str,
    is_branch: bool,
    json: bool,
    force: bool,
    verbose: bool,
    expected_hash: Optional[str] = None,
) -> str:
    """Perform branch/tag actions.

    :param nessie NessieClient to use
    :param list_references the -l option choice
    :param delete_reference the -d option choice
    :param branch the name of the branch to create/assign/delete
    :param hash_on_ref a specific hash (reachable from base_ref) to be used for creating the new branch or tag instead
           of the HEAD of base_ref
    :param base_ref existing branch or tag to act at the base for creating the new branch or tag
    :param is_branch whether the operation is about a branch (true) or tag (false)
    :param json the --json option choice
    :param force the -f option choice
    :param verbose the -v option choice
    :param expected_hash hash whose existence needs to be checked (on the server side) before performing the operation
    """
    if list_references or (not list_references and not delete_reference and not branch and not base_ref):
        return _handle_list(nessie, json, verbose, is_branch, branch)
    elif delete_reference:
        if not hash_on_ref:
            hash_on_ref = nessie.get_reference(branch).hash_ or "fail"
        getattr(nessie, "delete_{}".format("branch" if is_branch else "tag"))(branch, hash_on_ref)
    else:  # create or force assign
        # use locally configured default branch as base_ref by default
        if not base_ref:
            base_ref = nessie.get_default_branch()

        # use the current HEAD of base_ref as the hash for the new branch/tag by default
        if not hash_on_ref:
            hash_on_ref = nessie.get_reference(base_ref).hash_

        # try creating a _new_ branch/tag first
        try:
            getattr(nessie, "create_{}".format("branch" if is_branch else "tag"))(branch, base_ref, hash_on_ref)
        except NessieConflictException as conflict:
            # NessieConflictException means the branch/tag already exists - force reassignment if requested
            if force:
                getattr(nessie, "assign_{}".format("branch" if is_branch else "tag"))(branch, base_ref, hash_on_ref, expected_hash)
            else:
                raise conflict
    return ""


def _handle_list(nessie: NessieClient, json: bool, verbose: bool, is_branch: bool, branch: str) -> str:
    results = nessie.list_references()
    kept_results = [ref for ref in results if isinstance(ref, (Branch if is_branch else Tag))]
    if json:
        return ReferenceSchema().dumps(kept_results, many=True)
    output = ""
    default_branch = nessie.get_default_branch()
    if branch:
        kept_results = [i for i in kept_results if i.name == branch]
    max_width = max((len(i.name) for i in kept_results), default=0)
    for x in kept_results:
        next_row = "{}{}{}{}{}\n".format(
            "*".ljust(2) if x.name == default_branch else "  ",
            x.name.ljust(max_width + 1),
            " " if verbose else "",
            x.hash_ if verbose else "",
            " comment" if verbose else "",
        )
        output += click.style(next_row, fg="yellow") if x.name == default_branch else next_row
    return output
