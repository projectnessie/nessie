# -*- coding: utf-8 -*-
#
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

"""Branch CLI command."""

import click

from ._branch_tag_handlers import handle_branch_tag
from ..cli_common_context import ContextObject, MutuallyExclusiveOption
from ..decorators import error_handler, pass_client


@click.command(name="branch")
@click.option("-l", "--list", "is_list", cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"])
@click.option("-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a branch", mutually_exclusive=["is_list"])
@click.option("-f", "--force", is_flag=True, help="force branch assignment")
@click.option(
    "-o",
    "--hash-on-ref",
    help="Hash on source-reference for 'create' and 'assign' operations, if the branch shall not point to the HEAD of "
    "the given source-reference.",
)
@click.option(
    "-c",
    "--condition",
    "expected_hash",
    help="Expected hash. Only perform the action if the branch currently points to the hash specified by this option.",
)
@click.option(
    "-x",
    "--extended",
    "fetch_all",
    is_flag=True,
    help="Retrieve additional metadata for a branch, such as number of commits ahead/behind, "
    "info about the HEAD commit, number of total commits, or the common ancestor hash.",
)
@click.argument("branch", nargs=1, required=False)
@click.argument("base_ref", nargs=1, required=False)
@pass_client
@error_handler
def branch_(
    ctx: ContextObject,
    is_list: bool,
    force: bool,
    hash_on_ref: str,
    delete: bool,
    branch: str,
    base_ref: str,
    expected_hash: str,
    fetch_all: bool,
) -> None:
    """Branch operations.

    BRANCH name of branch to list or create/assign

    BASE_REF name of branch or tag from which to create/assign the new BRANCH

    Examples:

        nessie branch -> list all branches

        nessie branch -l -> list all branches

        nessie branch -l main -> list only main

        nessie branch -d main -> delete main

        nessie branch new_branch -> create new branch named 'new_branch' at current HEAD of the default branch

        nessie branch new_branch main -> create new branch named 'new_branch' at head of reference named 'main'

        nessie branch -o 12345678abcdef new_branch main -> create a branch named 'new_branch' at hash 12345678abcdef
    on reference named 'main'

        nessie branch new_branch main@12345678abcdef -> create a branch named 'new_branch' at hash 12345678abcdef
    on reference named 'main', alternate syntax for the above

        nessie branch -f existing_branch main -> assign branch named 'existing_branch' to head of reference named 'main'

        nessie branch -o 12345678abcdef -f existing_branch main -> assign branch named 'existing_branch' to hash 12345678abcdef
    on reference named 'main'

    """
    results = handle_branch_tag(
        ctx.nessie, is_list, delete, branch, hash_on_ref, base_ref, True, ctx.json, force, ctx.verbose, fetch_all, expected_hash
    )
    if ctx.json:
        click.echo(results)
    elif results:
        click.echo(results)
    else:
        click.echo()
