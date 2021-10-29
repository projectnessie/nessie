# -*- coding: utf-8 -*-

"""merge CLI command."""

import click
from click import UsageError

from ..cli_common_context import ContextObject, MutuallyExclusiveOption, pass_client
from ..error import error_handler


@click.command("merge")
@click.option("-b", "--branch", "onto_branch", help="branch to merge onto. If not supplied the default branch from config is used")
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
def merge(ctx: ContextObject, onto_branch: str, force: bool, expected_hash: str, hash_on_ref: str, from_ref: str) -> None:
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
    ctx.nessie.merge(from_ref, onto_branch if onto_branch else ctx.nessie.get_default_branch(), hash_on_ref, expected_hash)
    click.echo()
