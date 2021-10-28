# -*- coding: utf-8 -*-

"""config CLI command."""

import click

from ..cli_common_context import ContextObject, DefaultHelp, MutuallyExclusiveOption, pass_client
from ..conf import process
from ..error import error_handler


@click.command("config", cls=DefaultHelp)
@click.option("--get", cls=MutuallyExclusiveOption, help="get config parameter", mutually_exclusive=["set", "list", "unset"])
@click.option("--add", cls=MutuallyExclusiveOption, help="set config parameter", mutually_exclusive=["get", "list", "unset"])
@click.option(
    "-l",
    "--list",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="list config parameters",
    mutually_exclusive=["set", "get", "unset"],
)
@click.option("--unset", cls=MutuallyExclusiveOption, help="unset config parameter", mutually_exclusive=["get", "list", "set"])
@click.option("--type", help="type to interpret config value to set or get. Allowed options: bool, int")
@click.argument("key", nargs=1, required=False)
@pass_client
@error_handler
def config(ctx: ContextObject, get: str, add: str, list: bool, unset: str, type: str, key: str) -> None:
    """Set and view config."""
    res = process(get, add, list, unset, key, type)
    click.echo(res)
