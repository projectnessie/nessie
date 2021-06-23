# -*- coding: utf-8 -*-
"""Console script for nessie_client."""
import datetime
import sys
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import attr
import click
import confuse
from click import Option
from click import UsageError
from dateutil.tz import tzlocal

from . import __version__
from ._log import show_log
from ._ref import handle_branch_tag
from .conf import build_config
from .conf import process
from .error import error_handler
from .error import NessieNotFoundException
from .model import CommitMeta
from .model import CommitMetaSchema
from .model import Contents
from .model import ContentsSchema
from .model import Delete
from .model import Entries
from .model import Entry
from .model import EntrySchema
from .model import Operation
from .model import Put
from .nessie_client import _contents_key
from .nessie_client import _format_key
from .nessie_client import NessieClient


@attr.s(auto_attribs=True)
class ContextObject(object):
    """Click context object."""

    nessie: NessieClient
    verbose: bool
    json: bool


pass_client = click.make_pass_decorator(ContextObject)


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
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)  # type: ignore

    def handle_parse_result(self: "MutuallyExclusiveOption", ctx: click.Context, opts: Mapping, args: List) -> Tuple[Any, List[str]]:
        """Ensure mutually exclusive options are not used together."""
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with " "arguments `{}`.".format(self.name, ", ".join(self.mutually_exclusive))
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
        super(DefaultHelp, self).__init__(*args, **kwargs)  # type: ignore

    def parse_args(self: "DefaultHelp", ctx: click.Context, args: List) -> List:
        """Ensure that help is shown if nothing else is selected."""
        if not args:
            args = [self.help_flag]
        return super(DefaultHelp, self).parse_args(ctx, args)


@click.group()
@click.option("--json", is_flag=True, help="write output in json format.")
@click.option("-v", "--verbose", is_flag=True, help="Verbose output.")
@click.option("--endpoint", help="Optional endpoint, if different from config file.")
@click.option("--version", is_flag=True, callback=_print_version, expose_value=False, is_eager=True)
@click.pass_context
def cli(ctx: click.core.Context, json: bool, verbose: bool, endpoint: str) -> None:
    """Nessie cli tool.

    Interact with Nessie branches and tables via the command line
    """
    try:
        config = build_config({"endpoint": endpoint} if endpoint else None)
        nessie = NessieClient(config)
        ctx.obj = ContextObject(nessie, verbose, json)
    except confuse.exceptions.ConfigTypeError as e:
        raise click.ClickException(str(e))


@cli.group()
@pass_client
def remote(ctx: ContextObject) -> None:
    """Set and view remote endpoint."""
    pass


@cli.command("config", cls=DefaultHelp)
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


@remote.command("show")
@pass_client
@error_handler
def show(ctx: ContextObject) -> None:
    """Show current remote."""
    click.echo("Remote URL: " + ctx.nessie._base_url)
    click.echo("Default branch: " + ctx.nessie.get_reference(None).name)
    click.echo("Remote branches: ")
    for i in ctx.nessie.list_references():
        click.echo("\t" + i.name)


@remote.command(name="add")
@click.argument("endpoint", nargs=1, required=True)
@pass_client
@error_handler
def set_(ctx: ContextObject, endpoint: str) -> None:
    """Set current remote."""
    click.echo(process(None, "endpoint", False, None, endpoint))


@remote.command("set-head")
@click.argument("head", nargs=1, required=True)
@click.option("-d", "--delete", is_flag=True, help="delete the default branch")
@pass_client
@error_handler
def set_head(ctx: ContextObject, head: str, delete: bool) -> None:
    """Set current default branch. If -d is passed it will remove the default branch."""
    if delete:
        click.echo(process(None, None, False, "default_branch", None))
    else:
        click.echo(process(None, "default_branch", False, None, head))


@cli.command("log")
@click.option("-n", "--number", help="number of log entries to return", type=int)
@click.option("--since", "--after", help="Only include commits newer than specific date")
@click.option("--until", "--before", help="Only include commits older than specific date")
@click.option(
    "--author",
    multiple=True,
    help="Limit commits to a specific author (this is the original committer). Supports specifying multiple authors to filter by.",
)
@click.option(
    "--committer",
    multiple=True,
    help="Limit commits to a specific committer (this is the logged in user/account who performed the commit). "
    "Supports specifying multiple committers to filter by.",
)
@click.argument("revision_range", nargs=1, required=False)
@click.argument("paths", nargs=-1, type=click.Path(exists=False), required=False)
@pass_client
@error_handler
def log(
    ctx: ContextObject,
    number: int,
    since: str,
    until: str,
    author: List[str],
    committer: List[str],
    revision_range: str,
    paths: Tuple[click.Path],
) -> None:
    """Show commit log.

    REVISION_RANGE optional branch, tag or hash to start viewing log from. If of the form <hash>..<hash> only show log
    for given range

    PATHS optional list of paths. If given, only show commits which affected the given paths
    """
    if not revision_range:
        start = ctx.nessie.get_default_branch()
        end = None
    else:
        if ".." in revision_range:
            start, end = revision_range.split("..")
        else:
            start = revision_range
            end = None

    filtering_args: Any = {}
    if number:
        filtering_args["max"] = str(number)
    if author:
        filtering_args["authors"] = author
    if committer:
        filtering_args["committers"] = committer
    if since:
        filtering_args["after"] = since
    if until:
        filtering_args["before"] = until
    if end:
        filtering_args["end"] = end

    log_result = show_log(nessie=ctx.nessie, start_ref=start, limits=paths, **filtering_args)
    if ctx.json:
        click.echo(CommitMetaSchema().dumps(log_result, many=True))
    else:
        click.echo_via_pager(_format_log_result(x) for x in log_result)


def _format_log_result(x: CommitMeta) -> str:
    result = click.style("commit {}\n".format(x.hash_), fg="yellow")
    result += click.style("Author: {} <{}>\n".format(x.committer, x.email))
    result += click.style("Date: {}\n".format(_format_time(x.commitTime)))
    result += click.style("\n\t{}\n\n".format(x.message))
    return result


def _format_time(dt: datetime.datetime) -> str:
    return dt.astimezone(tzlocal()).strftime("%c %z")


@cli.command(name="branch")
@click.option("-l", "--list", cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"])
@click.option("-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a branch", mutually_exclusive=["list"])
@click.option("-f", "--force", is_flag=True, help="force branch assignment")
@click.option("-c", "--condition", help="Conditional Hash. Only perform the action if branch currently points to condition.")
@click.argument("branch", nargs=1, required=False)
@click.argument("new_branch", nargs=1, required=False)
@pass_client
@error_handler
def branch_(
    ctx: ContextObject,
    list: bool,
    force: bool,
    delete: bool,
    branch: str,
    new_branch: str,
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
    results = handle_branch_tag(ctx.nessie, list, delete, branch, new_branch, True, ctx.json, force, ctx.verbose, condition)
    if ctx.json:
        click.echo(results)
    elif results:
        click.echo(results)
    else:
        click.echo()


@cli.command("tag")
@click.option("-l", "--list", cls=MutuallyExclusiveOption, is_flag=True, help="list branches", mutually_exclusive=["delete"])
@click.option("-d", "--delete", cls=MutuallyExclusiveOption, is_flag=True, help="delete a branches", mutually_exclusive=["list"])
@click.option("-f", "--force", is_flag=True, help="force branch assignment")
@click.option("-c", "--condition", help="Conditional Hash. Only perform the action if branch currently points to condition.")
@click.argument("tag_name", nargs=1, required=False)
@click.argument("new_tag", nargs=1, required=False)
@pass_client
@error_handler
def tag(ctx: ContextObject, list: bool, force: bool, delete: bool, tag_name: str, new_tag: str, condition: str) -> None:
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
    results = handle_branch_tag(ctx.nessie, list, delete, tag_name, new_tag, False, ctx.json, force, ctx.verbose, condition)
    if ctx.json:
        click.echo(results)
    elif results:
        click.echo(results)
    else:
        click.echo()


@cli.command("merge")
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
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["force"],
    help="Conditional Hash. Only perform the action if branch currently points to condition.",
)
@pass_client
@error_handler
def merge(ctx: ContextObject, onto_branch: str, force: bool, condition: str, from_branch: str) -> None:
    """Merge FROM_BRANCH into current branch. FROM_BRANCH can be a hash or branch."""
    if not force and not condition:
        raise UsageError(
            """Either condition or force must be set. Condition should be set to a valid hash for concurrency
            control or force to ignore current state of Nessie Store."""
        )
    ctx.nessie.merge(from_branch, onto_branch if onto_branch else ctx.nessie.get_default_branch(), condition)
    click.echo()


@cli.command("cherry-pick")
@click.option("-b", "--branch", help="branch to cherry-pick onto. If not supplied the default branch from config is used")
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
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["force"],
    help="Conditional Hash. Only perform the action if branch currently points to condition.",
)
@click.argument("hashes", nargs=-1, required=False)
@pass_client
@error_handler
def cherry_pick(ctx: ContextObject, branch: str, force: bool, condition: str, hashes: Tuple[str]) -> None:
    """Transplant HASHES onto current branch."""
    if not force and not condition:
        raise UsageError(
            """Either condition or force must be set. Condition should be set to a valid hash for concurrency
            control or force to ignore current state of Nessie Store."""
        )
    ctx.nessie.cherry_pick(branch if branch else ctx.nessie.get_default_branch(), condition, *hashes)
    click.echo()


@cli.command("contents")
@click.option(
    "-l",
    "--list",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="list tables",
    mutually_exclusive=["delete", "set"],
)
@click.option(
    "-d",
    "--delete",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="delete a table",
    mutually_exclusive=["list", "set"],
)
@click.option(
    "-s",
    "--set",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="modify a table",
    mutually_exclusive=["list", "delete"],
)
@click.option(
    "-c",
    "--condition",
    help="Conditional Hash. Only perform the action if branch currently points to condition.",
)
@click.option("-r", "--ref", help="branch to list from. If not supplied the default branch from config is used")
@click.option("-m", "--message", help="commit message")
@click.option(
    "-t",
    "--type",
    "entity_type",
    help="entity types to filter on, if no entity types are passed then all types are returned",
    multiple=True,
)
@click.option("--author", help="The author to use for the commit")
@click.argument("key", nargs=-1, required=False)
@pass_client
@error_handler
def contents(
    ctx: ContextObject,
    list: bool,
    delete: bool,
    set: bool,
    key: List[str],
    ref: str,
    message: str,
    condition: str,
    entity_type: list,
    author: str,
) -> None:
    """Contents operations.

    KEY name of object to view, delete. If listing the key will limit by namespace what is included.
    """
    if list:
        keys = ctx.nessie.list_keys(ref if ref else ctx.nessie.get_default_branch(), entity_types=entity_type)
        results = EntrySchema().dumps(_format_keys_json(keys, *key), many=True) if ctx.json else _format_keys(keys, *key)
    elif delete:
        ctx.nessie.commit(ref, condition, _get_message(message), author, *_get_contents(ctx.nessie, ref, delete, *key))
        results = ""
    elif set:
        ctx.nessie.commit(ref, condition, _get_message(message), author, *_get_contents(ctx.nessie, ref, delete, *key))
        results = ""
    else:

        def content(*x: str) -> Generator[Contents, Any, None]:
            return ctx.nessie.get_values(ref if ref else ctx.nessie.get_default_branch(), *x)

        results = ContentsSchema().dumps(content(*key), many=True) if ctx.json else "\n".join((i.pretty_print() for i in content(*key)))
    click.echo(results)


def _get_contents(nessie: NessieClient, ref: str, delete: bool = False, *keys: str) -> List[Operation]:
    contents_altered: List[Operation] = list()
    for raw_key in keys:
        key = _format_key(raw_key)
        try:
            content = nessie.get_values(ref, key)
            content_json = ContentsSchema().dumps(content.__next__())
        except NessieNotFoundException:
            content_json = click.get_text_stream("stdin").read()

        MARKER = "# Everything below is ignored\n"
        message: Optional[str] = None
        if delete:
            message = "\n\n" + MARKER
        else:
            edit_message = (
                content_json
                + "\n\n"
                + MARKER
                + "Edit the content above to commit changes."
                + " Closing without change will result in a no-op."
                + " Removing the content results in a delete"
            )
            try:
                message = click.edit(edit_message)
            except click.ClickException:
                message = edit_message

        if message is not None:
            message_altered = message.split(MARKER, 1)[0].strip("\n")
            if message_altered:
                contents_altered.append(Put(_contents_key(raw_key), ContentsSchema().loads(message_altered)))
            else:
                contents_altered.append(Delete(_contents_key(raw_key)))
    return contents_altered


def _get_commit_message(*keys: str) -> Optional[str]:
    MARKER = "# Everything below is ignored\n"
    message = click.edit("\n\n" + MARKER + "\n".join(keys))
    return message.split(MARKER, 1)[0].rstrip("\n") if message is not None else None


def _get_message(message: str, *keys: str) -> Optional[str]:
    if message:
        return message
    return _get_commit_message(*keys)


def _format_keys_json(keys: Entries, *expected_keys: str) -> List[Entry]:
    results = list()
    for k in keys.entries:
        value = ['"{}"'.format(i) if "." in i else i for i in k.name.elements]
        if expected_keys and all(key for key in expected_keys if key not in value):
            continue
        results.append(k)
    return results


def _format_keys(keys: Entries, *key: str) -> str:
    results = defaultdict(list)
    result_str = ""
    for e in keys.entries:
        results[e.kind].append(e.name)
    for k in results.keys():
        result_str += k + ":\n"
        for v in results[k]:
            value = ['"{}"'.format(i) if "." in i else i for i in v.elements]
            if key and all(k for k in key if k not in value):
                continue
            result_str += "\t{}\n".format(".".join(value))
    return result_str


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
