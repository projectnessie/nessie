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

"""tag CLI command."""

import click

from pynessie.cli_common_context import ContextObject, MutuallyExclusiveOption
from pynessie.commands._branch_tag_handlers import handle_branch_tag
from pynessie.decorators import error_handler, pass_client


@click.command("tag")
@click.option("-l", "--list", "is_list", cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"])
@click.option("-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a branches", mutually_exclusive=["is_list"])
@click.option("-f", "--force", is_flag=True, help="force branch assignment")
@click.option(
    "-o",
    "--hash-on-ref",
    help="Hash on source-reference for 'create' and 'assign' operations, if the tag shall not point to the HEAD "
    "of the given source-reference.",
)
@click.option(
    "-c",
    "--condition",
    "expected_hash",
    help="Expected hash. Only perform the action if the tag currently points to the hash specified by this option.",
)
@click.option(
    "-x",
    "--extended",
    "fetch_all",
    is_flag=True,
    help="Retrieve additional metadata for a tag, such as number of commits ahead/behind, "
    "info about the HEAD commit, number of total commits, or the common ancestor hash.",
)
@click.argument("tag_name", nargs=1, required=False)
@click.argument("base_ref", nargs=1, required=False)
@pass_client
@error_handler
def tag(
    ctx: ContextObject,
    is_list: bool,
    force: bool,
    hash_on_ref: str,
    delete: bool,
    tag_name: str,
    base_ref: str,
    expected_hash: str,
    fetch_all: bool,
) -> None:
    """Tag operations.

    TAG_NAME name of branch to list or create/assign

    BASE_REF name of branch or tag whose HEAD reference is to be used for the new tag

    Examples:

        nessie tag -> list all tags

        nessie tag -l -> list all tags

        nessie tag -l v1.0 -> list only tag "v1.0"

        nessie tag -d v1.0 -> delete tag "v1.0"

        nessie tag new_tag -> create new tag named 'new_tag' at current HEAD of the default branch

        nessie tag new_tag main -> create new tag named 'new_tag' at head of reference named 'main' (branch or tag)

        nessie tag -o 12345678abcdef new_tag test -> create new tag named 'new_tag' at hash 12345678abcdef on
    reference named 'test'

        nessie tag new_tag test@12345678abcdef -> create new tag named 'new_tag' at hash 12345678abcdef on
    reference named 'test', alternative syntax of the above

        nessie tag -f existing_tag main -> assign tag named 'existing_tag' to head of reference named 'main'

        nessie tag -o 12345678abcdef -f existing_tag main -> assign tag named 'existing_tag' to hash 12345678abcdef
    on reference named 'main'

    """
    results = handle_branch_tag(
        ctx.nessie,
        is_list,
        delete,
        tag_name,
        hash_on_ref,
        base_ref,
        False,
        ctx.json,
        force,
        ctx.verbose,
        fetch_all,
        expected_hash,
    )
    if ctx.json:
        click.echo(results)
    elif results:
        click.echo(results)
    else:
        click.echo()
