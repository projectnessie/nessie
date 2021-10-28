# -*- coding: utf-8 -*-

"""merge CLI command."""

import click
from click import UsageError

from ..cli_common_context import ContextObject, MutuallyExclusiveOption, pass_client
from ..error import error_handler


@click.command("merge")
@click.option("-b", "--branch", "onto_branch", help="branch to merge onto. If not supplied the default branch from config is used")
@click.argument("from_branch", nargs=1, required=False)
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
def merge(ctx: ContextObject, onto_branch: str, force: bool, expected_hash: str, hash_on_ref: str, from_branch: str) -> None:
    """Merge FROM_BRANCH into another branch. FROM_BRANCH can be a hash or branch."""
    if not force and not expected_hash:
        raise UsageError(
            """Either condition or force must be set. Condition should be set to a valid hash for concurrency
            control or force to ignore current state of Nessie Store."""
        )
    ctx.nessie.merge(from_branch, onto_branch if onto_branch else ctx.nessie.get_default_branch(), hash_on_ref, expected_hash)
    click.echo()
