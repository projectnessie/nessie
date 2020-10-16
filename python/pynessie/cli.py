# -*- coding: utf-8 -*-
"""Console script for nessie_client."""
import datetime
import sys
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import click
import confuse
import simplejson
from click import Option
from click import UsageError
from dateutil.tz import tzlocal

from . import __version__
from ._log import show_log
from ._ref import handle_branch_tag
from .conf import build_config
from .conf import process
from .model import CommitMetaSchema
from .model import Entries
from .model import Entry
from .model import EntrySchema
from .nessie_client import NessieClient

pass_client = click.make_pass_decorator(NessieClient)


def _print_version(ctx: Any, param: Any, value: Any) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo("nessie version " + __version__)
    ctx.exit()


class MutuallyExclusiveOption(Option):
    """Only allow one option in a list to be set at once."""

    def __init__(self: "MutuallyExclusiveOption", *args: List, **kwargs: Dict) -> None:
        """Instantiated a mutually exclusive option."""
        self.mutually_exclusive = set(kwargs.pop("mutually_exclusive", []))
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self: "MutuallyExclusiveOption", ctx: click.Context, opts: Dict, args: List) -> Tuple:
        """Ensure mutually exclusive options are not used together."""
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with "
                "arguments `{}`.".format(self.name, ", ".join(self.mutually_exclusive))
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)


class DefaultHelp(click.Command):
    """If no options are presented show help."""

    def __init__(self: "DefaultHelp", *args: List, **kwargs: Dict) -> None:
        """Ensure that help is shown if nothing else is selected."""
        context_settings = kwargs.setdefault("context_settings", {})
        if "help_option_names" not in context_settings:
            context_settings["help_option_names"] = ["-h", "--help"]
        self.help_flag = context_settings["help_option_names"][0]
        super(DefaultHelp, self).__init__(*args, **kwargs)

    def parse_args(self: "DefaultHelp", ctx: click.Context, args: List) -> List:
        """Ensure that help is shown if nothing else is selected."""
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
def remote(nessie: NessieClient) -> None:
    """Set and view remote endpoint."""
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
def config(nessie: NessieClient, get: str, add: str, list: bool, unset: str, type: str, key: str) -> None:
    """Set and view config."""
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
    """Set current remote."""
    click.echo(process(None, "endpoint", False, None, endpoint))


@cli.command()
@click.option("-n", "--number", help="number of log entries to return", type=int, default=-1)
@click.option("--since", "--after", help="Commits more recent than specific date")
@click.option("--until", "--before", help="Commits older than specific date")
@click.option("--author", "--committer", is_flag=True, help="limit commits to specific committer")
@click.option("--json", is_flag=True, help="write output in json format.")
@click.argument("revision_range", nargs=1, required=False)
@click.argument("paths", nargs=-1, type=click.Path(exists=False), required=False)
@pass_client
def log(
    nessie: NessieClient,
    number: int,
    since: str,
    until: str,
    author: str,
    json: bool,
    revision_range: str,
    paths: Tuple[click.Path],
) -> None:
    """Show commit log.

    REVISION_RANGE optional branch, tag or hash to start viewing log from. If of the form <hash>..<hash> only show log
    for given range

    PATHS optional list of paths. If given, only show commits which affected the given paths
    """
    if not revision_range:
        start = nessie.get_default_branch()
        end = None
    else:
        if ".." in revision_range:
            start, end = revision_range.split("..")
        else:
            start = revision_range
            end = None

    log_result = show_log(nessie, start, number, since, until, author, end, paths)
    if json:
        click.echo(CommitMetaSchema().dumps(log_result, many=True))
    else:
        click.echo_via_pager(_format_log_result(x) for x in log_result)


def _format_log_result(x: str) -> str:
    result = click.style("commit {}\n".format(x.hash_), fg="yellow")
    result += click.style("Author: {} <{}>\n".format(x.commiter, x.email))
    result += click.style("Date: {}\n".format(_format_time(x.commitTime)))
    result += click.style("\n\t{}\n\n".format(x.message))
    return result


def _format_time(epoch: int) -> str:
    dt = datetime.datetime.fromtimestamp(epoch / 1000, datetime.timezone.utc)
    return dt.astimezone(tzlocal()).strftime("%c %z")


@cli.command(name="branch")
@click.option(
    "-l", "--list", cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"]
)
@click.option(
    "-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a branch", mutually_exclusive=["list"]
)
@click.option("-f", "--force", is_flag=True, help="force branch assignment")
@click.option("--json", is_flag=True, help="write output in json format.")
@click.option("-v", "--verbose", is_flag=True, help="Verbose output.")
@click.option(
    "-c", "--condition", help="Conditional Hash. Only perform the action if branch currently points to condition."
)
@click.argument("branch", nargs=1, required=False)
@click.argument("new_branch", nargs=1, required=False)
@pass_client
def branch_(
    nessie: NessieClient,
    list: bool,
    force: bool,
    delete: bool,
    json: bool,
    branch: str,
    new_branch: str,
    verbose: bool,
    condition: str,
) -> None:
    """Branch operations.

    BRANCH name of branch to list or create/assign

    NEW_BRANCH name of branch to assign from or rename to

    Examples:

        nessie branch -l -> list all branches

        nessie branch -l main -> list only main

        nessie branch -d main -> delete main

        nessie branch -> list all branches

        nessie branch main -> create branch main at current head

        nessie branch main test -> create branch main at head of test

        nessie branch -f main test -> assign main to head of test

    """
    results = handle_branch_tag(nessie, list, delete, branch, new_branch, True, json, force, verbose, condition)
    if json:
        click.echo(results)
    else:
        click.echo_via_pager(results)


@cli.command()
@click.option(
    "-l", "--list", cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"]
)
@click.option(
    "-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a branches", mutually_exclusive=["list"]
)
@click.option("-f", "--force", is_flag=True, help="force branch assignment")
@click.option("--json", is_flag=True, help="write output in json format.")
@click.option("-v", "--verbose", is_flag=True, help="Verbose output.")
@click.option(
    "-c", "--condition", help="Conditional Hash. Only perform the action if branch currently points to condition."
)
@click.argument("tag_name", nargs=1, required=False)
@click.argument("new_tag", nargs=1, required=False)
@pass_client
def tag(
    nessie: NessieClient,
    list: bool,
    json: bool,
    force: bool,
    delete: bool,
    tag_name: str,
    new_tag: str,
    verbose: bool,
    condition: str,
) -> None:
    """Tag operations.

    TAG_NAME name of branch to list or create/assign

    NEW_TAG name of branch to assign from or rename to

    Examples:

        nessie tag -l -> list all tags

        nessie tag -l main -> list only main

        nessie tag -d main -> delete main

        nessie tag -> list all tags

        nessie tag main -> create tag xxx at current head

        nessie tag main test -> create tag xxx at head of test

        nessie tag -f main test -> assign xxx to head of test

    """
    results = handle_branch_tag(nessie, list, delete, tag_name, new_tag, False, json, force, verbose, condition)
    if json:
        click.echo(results)
    else:
        click.echo_via_pager(results)


@cli.command()
@click.option(
    "-b", "--branch", help="branch to cherry-pick onto. If not supplied the default branch from config is used"
)
@click.argument("merge_branch", nargs=1, required=False)
@pass_client
def merge(nessie: NessieClient, branch: str, merge_branch: str) -> None:
    """Merge BRANCH into current branch. BRANCH can be a hash or branch."""
    nessie.merge(branch if branch else nessie.get_default_branch(), merge_branch)
    click.echo()


@cli.command()
@click.option(
    "-b", "--branch", help="branch to cherry-pick onto. If not supplied the default branch from config is used"
)
@click.argument("hashes", nargs=-1, required=False)
@pass_client
def cherry_pick(nessie: NessieClient, branch: str, hashes: Tuple[str]) -> None:
    """Transplant HASHES onto current branch."""
    nessie.cherry_pick(branch if branch else nessie.get_default_branch(), hashes)
    click.echo()


@cli.command()
@click.option(
    "-l", "--list", cls=MutuallyExclusiveOption, is_flag=True, help="list tables", mutually_exclusive=["delete"]
)
@click.option(
    "-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a table", mutually_exclusive=["list"]
)
@click.option("--json", is_flag=True, help="write output in json format.")
@click.option("-b", "--branch", help="branch to list from. If not supplied the default branch from config is used")
@click.argument("key", nargs=1, required=False)
@pass_client
def contents(nessie: NessieClient, list: bool, json: bool, delete: bool, key: str, branch: str) -> None:
    """Contents operations.

    KEY name of object to view, delete. If listing the key will limit by namespace what is included.
    """
    if list:
        keys = nessie.list_keys(branch if branch else nessie.get_default_branch())
        results = EntrySchema().dumps(_format_keys_json(keys, key), many=True) if json else _format_keys(keys, key)
    elif delete:
        raise NotImplementedError("performing delete from the command line is not yet implemented.")
    else:
        raise NotImplementedError("performing gets from the command line is not yet implemented.")
        # content = nessie.get_values(branch if branch else nessie.get_default_branch(), key)
        # results = ContentSchema().dumps(_format_keys_json(keys, key), many=True) if json else _format_keys(keys, key)
    if json:
        click.echo(results)
    else:
        click.echo_via_pager(results)


def _format_keys_json(keys: Entries, key: Optional[str]) -> List[Entry]:
    results = list()
    for k in keys.entries:
        value = ['"{}"'.format(i) if "." in i else i for i in k.name.elements]
        if key and key not in value:
            continue
        results.append(k)
    return results


def _format_keys(keys: Entries, key: Optional[str]) -> str:
    results = defaultdict(list)
    result_str = ""
    for k in keys.entries:
        results[k.kind].append(k.name)
    for k in results.keys():
        result_str += k + ":\n"
        for v in results[k]:
            value = ['"{}"'.format(i) if "." in i else i for i in v.elements]
            if key and key not in value:
                continue
            result_str += "\t{}\n".format(".".join(value))
    return result_str


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
