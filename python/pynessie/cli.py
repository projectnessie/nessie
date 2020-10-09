# -*- coding: utf-8 -*-
"""Console script for nessie_client."""
import os
import sys
from typing import Any
from typing import List

import click
from click import Option, UsageError

from . import __version__
from .conf import build_config
from .model import Contents
from .model import ContentsSchema
from .model import ReferenceSchema
from .nessie_client import NessieClient


def _print_version(ctx: Any, param: Any, value: Any) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo(__version__)
    ctx.exit()


class MutuallyExclusiveOption(Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        help = kwargs.get('help', '')
        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            kwargs['help'] = help + (
                ' NOTE: This argument is mutually exclusive with '
                ' arguments: [' + ex_str + '].'
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with "
                "arguments `{}`.".format(
                    self.name,
                    ', '.join(self.mutually_exclusive)
                )
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(
            ctx,
            opts,
            args
        )


@click.group()
@click.argument("endpoint", nargs=1, required=False)
@click.option("--version", is_flag=True, callback=_print_version, expose_value=False, is_eager=True)
@click.pass_context
def cli(ctx: click.core.Context, endpoint: str) -> None:
    """Nessie cli tool.

    Interact with Nessie branches and tables via the command line

    ENDPOINT is optional arg if using a different endpoint than in config
    """
    config = build_config({"endpoint": endpoint})
    nessie = NessieClient(config)
    ctx.obj = dict()
    ctx.obj["nessie"] = nessie


@cli.command()
@click.pass_obj
def log(args: dict) -> None:
    """List all known references."""
    results = args["nessie"].list_references()
    click.echo(ReferenceSchema().dumps(results, many=True))


@cli.command()
@click.option("-l", '--list-references', cls=MutuallyExclusiveOption, is_flag=True, help="list references", mutually_exclusive=["delete_reference"])
@click.option("-d", '--delete-reference', cls=MutuallyExclusiveOption, is_flag=True, help="delete a reference", mutually_exclusive=["list_references"])
@click.argument("branch_name", nargs=1, required=False)
@click.pass_obj
def branch(args: dict, list_references: bool, delete_reference: bool, branch_name: str) -> None:
    """List all known references."""
    if list_references:
        results = args["nessie"].list_references()
        click.echo(ReferenceSchema().dumps(results, many=True))
    elif delete_reference:
        branch_object = args['nessie'].get_reference(branch_name)
        args["nessie"].delete_branch(branch_name, branch_object.hash_)
        click.echo()
    else:
        pass


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
