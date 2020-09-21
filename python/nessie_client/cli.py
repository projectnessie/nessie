# -*- coding: utf-8 -*-
"""Console script for nessie_client."""
import os
import sys
from typing import Any
from typing import List

import click

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


@click.group()
@click.option("--config", type=click.Path(exists=True, dir_okay=True, file_okay=False), help="Custom config file.")
@click.option("-e", "--endpoint", help="Endpoint if different from config file")
@click.option("-u", "--username", help="username if different from config file")
@click.option("--password", help="password if different from config file")
@click.option("--skip-verify", is_flag=True, help="skip verificatoin of ssl cert")
@click.option("--version", is_flag=True, callback=_print_version, expose_value=False, is_eager=True)
@click.pass_context
def cli(ctx: click.core.Context, config: str, endpoint: str, username: str, password: str, skip_verify: bool) -> None:
    """Nessie cli tool.

    Interact with Nessie branches and tables via the command line
    """
    if config:
        os.environ["NESSIE_CLIENTDIR"] = config
    config = build_config({"endpoint": endpoint, "username": username, "password": password, "verify": not skip_verify})
    nessie = NessieClient(config)
    ctx.obj = dict()
    ctx.obj["nessie"] = nessie


@cli.command()
@click.pass_obj
def list_references(args: dict) -> None:
    """List all known references."""
    results = args["nessie"].list_references()
    click.echo(ReferenceSchema().dumps(results, many=True))


@cli.command()
@click.argument("ref", nargs=1, required=True)
@click.pass_obj
def show_reference(args: dict, ref: str) -> None:
    """Show a specific reference."""
    results = args["nessie"].get_reference(ref)
    click.echo(ReferenceSchema().dumps(results))


@cli.command()
@click.argument("branch", nargs=1, required=True)
@click.option("--hash", "-h", "hash_", type=str, help="hash", required=True)
@click.pass_obj
def delete_branch(args: dict, branch: str, hash_: str) -> None:
    """Delete a specific branch."""
    args["nessie"].delete_branch(branch, hash_)
    click.echo()


@cli.command()
@click.argument("branch", nargs=1, required=True)
@click.option("--ref", "-r", type=str, help="ref to clone from")
@click.pass_obj
def create_branch(args: dict, branch: str, ref: str) -> None:
    """Create a branch and optionally fork from ref."""
    args["nessie"].create_branch(branch, ref)
    click.echo()


@cli.command()
@click.argument("ref", nargs=1, required=True)
@click.pass_obj
def list_tables(args: dict, ref: str) -> None:
    """List tables from BRANCH."""
    tables = args["nessie"].list_tables(ref)
    click.echo(tables)


@cli.command()
@click.argument("ref", nargs=1, required=True)
@click.argument("table", nargs=-1, required=True)
@click.pass_obj
def show_table(args: dict, ref: str, table: List[str]) -> None:
    """List tables from ref."""
    tables = args["nessie"].get_tables(ref, *table)
    if len(tables) == 1:
        click.echo(ContentsSchema().dumps(tables[0]))
    else:
        click.echo(Contents().dumps(tables, many=True))


@cli.command()
@click.argument("branch", nargs=1, required=True)
@click.argument("to-branch", nargs=1, required=True)
@click.pass_obj
def assign_branch(args: dict, branch: str, to_branch: str) -> None:
    """Assign from one ref to another."""
    args["nessie"].assign(branch, to_branch)
    click.echo()


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
