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

"""merge CLI command."""

import json

import click
from click import UsageError

from pynessie.cli_common_context import ContextObject, MutuallyExclusiveOption
from pynessie.decorators import error_handler, pass_client, validate_reference
from pynessie.model import MergeResponseSchema


@click.command("merge")
@click.option("-b", "--branch", "ref", help="branch to merge onto. If not supplied the default branch from config is used")
@click.argument("from_ref", nargs=1, required=False)
@click.option(
    "-f",
    "--force",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    mutually_exclusive=["condition"],
    help="force branch assignment",
)
@click.option(
    "-c",
    "--condition",
    "expected_hash",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["force"],
    help="Expected hash. Only perform the action if the branch currently points to the hash specified by this option.",
)
@click.option("-o", "--hash-on-ref", help="Hash on merge-from-reference")
@pass_client
@error_handler
@validate_reference
def merge(ctx: ContextObject, ref: str, force: bool, expected_hash: str, hash_on_ref: str, from_ref: str) -> None:
    """Merge FROM_REF into another branch.

    FROM_REF can be a hash or branch.

    Examples:

        nessie merge -c 12345678abcdef dev -> merge dev to default branch with default branch's
    expected hash '12345678abcdef'

        nessie merge -b main -c 12345678abcdef dev -> merge dev to a branch named main with main branch's
    expected hash '12345678abcdef'

        nessie merge -b main -o 56781234abcdef -c 12345678abcdef dev -> merge dev at hash-on-ref '56781234abcdef' to
    main branch with main branch's expected hash '12345678abcdef'

        nessie merge -f -b main dev -> forcefully merge dev to a branch named main
    """
    if not force and not expected_hash:
        raise UsageError(
            """Either condition or force must be set. Condition should be set to a valid hash for concurrency
            control or force to ignore current state of Nessie Store."""
        )
    merge_response = ctx.nessie.merge(from_ref, ref, hash_on_ref, expected_hash)
    if merge_response is None:
        message = "Merge succeeded but legacy server did not respond with additional details."
        if ctx.json:
            hint = {"_cli_hint": message}
            click.echo(json.dumps(hint))
        else:
            click.echo(message)
    else:
        if ctx.json:
            click.echo(MergeResponseSchema().dumps(merge_response))
        else:
            commits = merge_response.source_commits
            click.echo(
                "The following {N} commits were merged onto {target_branch}:".format(
                    N=len(commits), target_branch=merge_response.target_branch
                )
            )
            for commit in commits:
                commit_meta = commit.commit_meta
                click.echo(commit_meta.hash_ + ' "' + commit_meta.message + '" ' + commit_meta.committer)
            if merge_response.resultant_target_hash:
                click.echo(
                    "Resultant hash on {} after merge: {}".format(merge_response.target_branch, merge_response.resultant_target_hash)
                )
