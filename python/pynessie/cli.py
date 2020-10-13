# -*- coding: utf-8 -*-
"""Console script for nessie_client."""
import os
import sys
from typing import Any
from typing import List
from typing import Tuple

import click
from click import Option, UsageError

from . import __version__
from .conf import build_config, write
from .model import Contents
from .model import ContentsSchema
from .model import ReferenceSchema
from .nessie_client import NessieClient
from .log import show_log

pass_client = click.make_pass_decorator(NessieClient)


def _print_version(ctx: Any, param: Any, value: Any) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo("nessie version " + __version__)
    ctx.exit()


class MutuallyExclusiveOption(Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        help = kwargs.get('help', '')
        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            # kwargs['help'] = help + (
            #     ' NOTE: This argument is mutually exclusive with '
            #     ' arguments: [' + ex_str + '].'
            # )
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


class DefaultHelp(click.Command):
    def __init__(self, *args, **kwargs):
        context_settings = kwargs.setdefault('context_settings', {})
        if 'help_option_names' not in context_settings:
            context_settings['help_option_names'] = ['-h', '--help']
        self.help_flag = context_settings['help_option_names'][0]
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
    config = build_config()
    nessie = NessieClient(config)
    ctx.obj = nessie


@cli.group()
@pass_client
def remote(nessie: NessieClient):
    """
    Set and view remote endpoint.
    """
    pass


@cli.command(cls=DefaultHelp)
@click.option('--get', cls=MutuallyExclusiveOption, help="get config parameter", mutually_exclusive=["set", "list", 'unset'])
@click.option('--set', cls=MutuallyExclusiveOption, help="set config parameter", mutually_exclusive=["get", "list", 'unset'])
@click.option('-l', '--list', cls=MutuallyExclusiveOption, is_flag=True, help="list config parameters", mutually_exclusive=["set", "get", 'unset'])
@click.option('--unset', cls=MutuallyExclusiveOption, help="unset config parameter", mutually_exclusive=["get", "list", 'set'])
@click.argument("key", nargs=1, required=False)
@pass_client
def config(nessie: NessieClient, get, set, list, unset, key):
    """
    Set and view config.
    """
    if (get or set or list or unset) and key:
        raise UsageError("can't set argument and specify option flags")
    if get or key:
        click.echo("fetch config for " + get if get else key)
    if set:
        click.echo("set config for " + set)
    if list:
        click.echo("list config")
    if unset:
        click.echo("unset config for " + unset)
    pass


@remote.command()
@pass_client
def show(nessie: NessieClient) -> None:
    """Show current remote."""
    # todo fill in with more `git remote show` details.
    click.echo(nessie._base_url)


@remote.command(name="set")
@click.argument("endpoint", nargs=1, required=True)
@pass_client
def set_(nessie: NessieClient, endpoint: str) -> None:
    """Set current remote"""
    config = build_config()
    config["endpoint"].set(endpoint)
    write(config)
    click.echo()


@cli.command()
@click.argument("revision_range", nargs=1, required=False)
@click.argument('paths', nargs=-1, type=click.Path(exists=False), required=False)
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
        if '..' in revision_range:
            start, end = revision_range.split("..")
        else:
            start = revision_range
            end = None
    log_result = show_log(nessie, start, end, paths)
    click.echo_via_pager(log_result)


@cli.command(name="branch")
@click.option("-l", '--list', cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"])
@click.option("-d", '--delete', cls=MutuallyExclusiveOption, is_flag=True, help="delete a branch", mutually_exclusive=["list"])
@click.option("-f", '--force', is_flag=True, help="force branch assignment")
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
    nessie branch -> show all branches
    nessie branch main -> create branch main at current head
    nessie branch main test -> create branch main at head of test
    nessie branch -f main test -> assign main to head of test
    """
    """
    no args: show all refs paged
    -l same as no args
    -d delete
    --show-current list currently set branch
    branch only - create branch at current branch head
    branch and object - create branch at object.
    branch [and object] and -f - assign to object or current head
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
@click.option("-l", '--list', cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"])
@click.option("-d", '--delete', cls=MutuallyExclusiveOption, is_flag=True, help="delete a branches", mutually_exclusive=["list"])
@click.argument("tag_name", nargs=1, required=False)
@pass_client
def tag(nessie: NessieClient, list_references: bool, delete_reference: bool, tag_name: str) -> None:
    """List all known references."""
    """
    no args: show all refs paged
    -l same as no args
    -d delete
    --show-current list currently set branch
    branch only - create branch at current branch head
    branch and object - create branch at object.
    branch [and object] and -f - assign to object or current head
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
@pass_client
def merge(nessie: NessieClient) -> None:
    """List all known references."""
    """
    todo
    """
    pass


@cli.command()
@pass_client
def transplant(nessie: NessieClient) -> None:
    """List all known references."""
    """
    todo
    """
    pass


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
