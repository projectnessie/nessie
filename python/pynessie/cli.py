# -*- coding: utf-8 -*-
"""Console script for nessie_client."""
import os
import sys
from typing import Any
from typing import List
from typing import Tuple
from typing import Union

import click
import confuse
from click import Option
from click import UsageError

from . import __version__
from .conf import build_config
from .conf import process
from .conf import write_to_file
from .log import show_log
from .model import Contents
from .model import ContentsSchema
from .model import ReferenceSchema
from .nessie_client import NessieClient

pass_client = click.make_pass_decorator(NessieClient)


def _print_version(ctx: Any, param: Any, value: Any) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo("nessie version " + __version__)
    ctx.exit()


class MutuallyExclusiveOption(Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop("mutually_exclusive", []))
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with "
                "arguments `{}`.".format(self.name, ", ".join(self.mutually_exclusive))
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)


class DefaultHelp(click.Command):
    def __init__(self, *args, **kwargs):
        context_settings = kwargs.setdefault("context_settings", {})
        if "help_option_names" not in context_settings:
            context_settings["help_option_names"] = ["-h", "--help"]
        self.help_flag = context_settings["help_option_names"][0]
        super(DefaultHelp, self).__init__(*args, **kwargs)

    def parse_args(self, ctx, args):
        if not args:
            args = [self.help_flag]
        return super(DefaultHelp, self).parse_args(ctx, args)


@click.group()
@click.option("--version", is_flag=True, callback=_print_version, expose_value=False, is_eager=True)
@click.pass_context
def cli(ctx: click.core.Context) -> None:
    """Nessie cli tool.

    Interact with Nessie branches and tables via the command line
    """
    try:
        config = build_config()
        nessie = NessieClient(config)
        ctx.obj = nessie
    except confuse.exceptions.ConfigTypeError as e:
        raise click.ClickException(str(e))


@cli.group()
@pass_client
def remote(nessie: NessieClient):
    """
    Set and view remote endpoint.
    """
    pass


@cli.command(cls=DefaultHelp)
@click.option(
    "--get", cls=MutuallyExclusiveOption, help="get config parameter", mutually_exclusive=["set", "list", "unset"]
)
@click.option(
    "--add", cls=MutuallyExclusiveOption, help="set config parameter", mutually_exclusive=["get", "list", "unset"]
)
@click.option(
    "-l",
    "--list",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="list config parameters",
    mutually_exclusive=["set", "get", "unset"],
)
@click.option(
    "--unset", cls=MutuallyExclusiveOption, help="unset config parameter", mutually_exclusive=["get", "list", "set"]
)
@click.option("--type", help="type to interpret config value to set or get. Allowed options: bool, int")
@click.argument("key", nargs=1, required=False)
@pass_client
def config(nessie: NessieClient, get: str, add: str, list: bool, unset: str, type: str, key: str):
    """
    Set and view config.
    """
    click.echo(process(get, add, list, unset, key, type))


@remote.command()
@pass_client
def show(nessie: NessieClient) -> None:
    """Show current remote."""
    click.echo("Remote URL: " + nessie._base_url)
    click.echo("Default branch: " + nessie.get_reference(None).name)
    click.echo("Remote branches: ")
    for i in nessie.list_references():
        click.echo("\t" + i.name)


@remote.command(name="add")
@click.argument("endpoint", nargs=1, required=True)
@pass_client
def set_(nessie: NessieClient, endpoint: str) -> None:
    """Set current remote"""
    click.echo(process(None, "endpoint", False, None, endpoint))


@cli.command()
@click.argument("revision_range", nargs=1, required=False)
@click.argument("paths", nargs=-1, type=click.Path(exists=False), required=False)
@pass_client
def log(nessie: NessieClient, revision_range: str, paths: Tuple[click.Path]) -> None:
    """
    Show commit log.

    REVISION_RANGE optional branch, tag or hash to start viewing log from. If of the form <hash>..<hash> only show log
    for given range\n
    PATHS optional list of paths. If given, only show commits which affected the given paths
    """
    if not revision_range:
        start = None
        end = None
    else:
        if ".." in revision_range:
            start, end = revision_range.split("..")
        else:
            start = revision_range
            end = None
    log_result = show_log(nessie, start, end, paths)
    click.echo_via_pager(log_result)


@cli.command(name="branch")
@click.option(
    "-l", "--list", cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"]
)
@click.option(
    "-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a branch", mutually_exclusive=["list"]
)
@click.option("-f", "--force", is_flag=True, help="force branch assignment")
@click.argument("branch", nargs=1, required=False)
@click.argument("new_branch", nargs=1, required=False)
@pass_client
def branch_(nessie: NessieClient, list_references: bool, delete_reference: bool, branch: str, new_branch: str) -> None:
    """
    Branch operations.

    BRANCH name of branch to list or create/assign
    NEW_BRANCH name of branch to assign from or rename to

    examples:
    nessie branch -l -> list all branches
    nessie branch -l main -> list only main
    nessie branch -d main -> delete main
    nessie branch -> list all branches
    nessie branch main -> create branch main at current head
    nessie branch main test -> create branch main at head of test
    nessie branch -f main test -> assign main to head of test
    """
    if list_references:
        results = nessie.list_references()
        click.echo(ReferenceSchema().dumps(results, many=True))
    elif delete_reference:
        branch_object = nessie.get_reference(branch)
        nessie.delete_branch(branch, branch_object.hash_)
        click.echo()
    else:
        pass


@cli.command()
@click.option(
    "-l", "--list", cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"]
)
@click.option(
    "-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a branches", mutually_exclusive=["list"]
)
@click.option("-f", "--force", is_flag=True, help="force branch assignment")
@click.argument("tag", nargs=1, required=False)
@click.argument("new_tag", nargs=1, required=False)
@pass_client
def tag(nessie: NessieClient, list_references: bool, delete_reference: bool, tag_name: str) -> None:
    """
    Tag operations.

    TAG name of branch to list or create/assign
    NEW_TAG name of branch to assign from or rename to

    examples:
    nessie tag -l -> list all tags
    nessie tag -l main -> list only main
    nessie tag -d main -> delete main
    nessie tag -> list all tags
    nessie tag main -> create tag xxx at current head
    nessie tag main test -> create tag xxx at head of test
    nessie tag -f main test -> assign xxx to head of test
    """
    if list_references:
        results = nessie.list_references()
        click.echo(ReferenceSchema().dumps(results, many=True))
    elif delete_reference:
        branch_object = nessie.get_reference(tag_name)
        nessie.delete_branch(tag_name, branch_object.hash_)
        click.echo()
    else:
        pass


@cli.command()
@click.option(
    "-b", "--branch", help="branch to cherry-pick onto. If not supplied the default branch from config is used"
)
@click.argument("merge_branch", nargs=1, required=False)
@pass_client
def merge(nessie: NessieClient, branch: str, merge_branch: str) -> None:
    """
    Merge BRANCH into current branch. BRANCH can be a hash or branch
    """
    pass


@cli.command()
@click.option(
    "-b", "--branch", help="branch to cherry-pick onto. If not supplied the default branch from config is used"
)
@click.argument("hashes", nargs=-1, required=False)
@pass_client
def cherry_pick(nessie: NessieClient, branch: str, hashes: Tuple) -> None:
    """
    Transplant HASHES onto current branch.
    """
    pass


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
