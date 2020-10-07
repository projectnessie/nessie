# -*- coding: utf-8 -*-
from typing import Optional

import click

from . import NessieClient
from .error import NessieNotFoundException
from .model import Branch
from .model import ReferenceSchema
from .model import Tag


def handle_branch_tag(
    nessie: NessieClient,
    list_references: bool,
    delete_reference: bool,
    branch: str,
    new_branch: str,
    is_branch: bool,
    json: bool,
    force: bool,
    verbose: bool,
    old_hash: Optional[str] = None,
) -> str:
    """Perform branch/tag actions."""
    if list_references or (not list_references and not delete_reference and not branch and not new_branch):
        return _handle_list(nessie, json, verbose, is_branch, branch)
    elif delete_reference:
        branch_object = nessie.get_reference(branch)
        getattr(nessie, "delete_{}".format("branch" if is_branch else "tag"))(branch, branch_object.hash_)
    elif branch and not new_branch:
        getattr(nessie, "create_{}".format("branch" if is_branch else "tag"))(branch)
    elif branch and new_branch and not force:

        try:
            getattr(nessie, "assign_{}".format("branch" if is_branch else "tag"))(branch, new_branch, old_hash)
        except NessieNotFoundException:
            create_on = nessie.get_reference(new_branch).hash_
            getattr(nessie, "create_{}".format("branch" if is_branch else "tag"))(branch, create_on)
    else:
        getattr(nessie, "assign_{}".format("branch" if is_branch else "tag"))(branch, new_branch, old_hash)
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
