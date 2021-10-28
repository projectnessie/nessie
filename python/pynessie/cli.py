# -*- coding: utf-8 -*-
"""Console script for nessie_client."""
import sys
from typing import Any

import click
import confuse

from . import __version__
from .cli_common_context import ContextObject
from .client import NessieClient
from .commands import branch_
from .commands import cherry_pick
from .commands import config
from .commands import contents
from .commands import log
from .commands import merge
from .commands import remote
from .commands import tag
from .conf import build_config


def _print_version(ctx: Any, param: Any, value: Any) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo("nessie version " + __version__)
    ctx.exit()


@click.group()
@click.option("--json", is_flag=True, help="write output in json format.")
@click.option("-v", "--verbose", is_flag=True, help="Verbose output.")
@click.option("--endpoint", help="Optional endpoint, if different from config file.")
@click.option("--auth-token", help="Optional bearer auth token, if different from config file.")
@click.option("--version", is_flag=True, callback=_print_version, expose_value=False, is_eager=True)
@click.pass_context
def cli(ctx: click.core.Context, json: bool, verbose: bool, endpoint: str, auth_token: str) -> None:
    """Nessie cli tool.

    Interact with Nessie branches and tables via the command line
    """
    try:
        cfg_map = {}
        if endpoint:
            cfg_map["endpoint"] = endpoint
        if auth_token:
            cfg_map["auth.type"] = "bearer"
            cfg_map["auth.token"] = auth_token
        config = build_config(cfg_map)
        nessie = NessieClient(config)
        ctx.obj = ContextObject(nessie, verbose, json)
    except confuse.exceptions.ConfigTypeError as e:
        raise click.ClickException(str(e)) from e


cli.add_command(remote)
cli.add_command(config)
cli.add_command(log)
cli.add_command(branch_)
cli.add_command(tag)
cli.add_command(merge)
cli.add_command(cherry_pick)
cli.add_command(contents)


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
