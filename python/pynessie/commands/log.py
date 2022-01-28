# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""log CLI command."""

import datetime
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

import click
from dateutil.tz import tzlocal

from ..cli_common_context import ContextObject, MutuallyExclusiveOption
from ..decorators import error_handler, pass_client, validate_reference
from ..model import CommitMetaSchema, LogEntry, LogEntrySchema, split_into_reference_and_hash
from ..utils import build_filter_for_commit_log_flags


@click.command("log")
@click.argument("ref", nargs=1, required=False)
@click.option("-n", "--number", help="number of log entries to return", type=int)
@click.option("--since", "--after", help="Only include commits newer than specific date, such as '2001-01-01T00:00:00+00:00'")
@click.option("--until", "--before", help="Only include commits older than specific date, such as '2999-12-30T23:00:00+00:00'")
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
@click.option(
    "-r",
    "--revision-range",
    help="Hash to start viewing log from. If of the form '<start_hash>'..'<end_hash>' "
    "only show log for given range on the particular ref that was provided, the '<end_hash>' "
    "is inclusive and '<start_hash>' is exclusive.",
)
@click.option(
    "--filter",
    "query_filter",
    multiple=False,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["author", "committer", "since", "until"],
    help="Allows advanced filtering using the Common Expression Language (CEL). "
    "An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n"
    "Some examples with usable variables 'commit.author' (string) / 'commit.committer' (string) / 'commit.commitTime' (timestamp) / "
    "'commit.hash' (string) / 'commit.message' (string) / 'commit.properties' (map) are:\n"
    "commit.author=='nessie_author'\n"
    "commit.committer=='nessie_committer'\n"
    "timestamp(commit.commitTime) > timestamp('2021-06-21T10:39:17.977922Z')\n",
)
@click.option(
    "-x",
    "--extended",
    "fetch_all",
    is_flag=True,
    help="Retrieve all available information for the commit entries. "
    "This option will also return the operations for each commit and the parent hash. "
    "The schema of the JSON output will then produce a list of LogEntrySchema, otherwise a list of CommitMetaSchema.",
)
@pass_client
@error_handler
@validate_reference
def log(  # noqa: C901
    ctx: ContextObject,
    ref: str,
    number: int,
    since: str,
    until: str,
    author: List[str],
    committer: List[str],
    revision_range: str,
    query_filter: str,
    fetch_all: bool,
) -> None:
    """Show commit log.

    REF name of branch or tag to use to show the commit logs

    Examples:

        nessie log -> show commit logs using the configured default branch

        nessie log 1234567890abcdef -> show commit logs starting at commit 1234567890abcdef

        nessie log dev -> show commit logs for 'dev' branch, starting at the most recent (HEAD) commit of 'dev'

        nessie log dev@1234567890abcdef -> show commit logs for 'dev' branch, starting at commit 1234567890abcdef in 'dev'

        nessie log -n 5 dev -> show commit logs for 'dev' branch limited by 5 commits

        nessie log --revision-range 12345678abcdef..12345678efghj dev -> show commit logs in range of hash
    '12345678abcdef' and '12345678efghj' in 'dev' branch

        nessie log --author nessie.user dev -> show commit logs for user 'nessie.user' in 'dev' branch

        nessie log --filter "commit.author == 'nessie_user2' || commit.author == 'non_existing'" dev ->
    show commit logs using query in 'dev' branch

        nessie log --after "2019-01-01T00:00:00+00:00" --before "2021-01-01T00:00:00+00:00" dev ->
    show commit logs between "2019-01-01T00:00:00+00:00" and "2021-01-01T00:00:00+00:00" in 'dev' branch

    """
    ref, start_hash, end_hash = _log_ref_and_hashes(ref, revision_range)

    filtering_args: Any = {}
    if start_hash:
        filtering_args["startHash"] = start_hash
    if end_hash:
        filtering_args["endHash"] = end_hash
    expr = build_filter_for_commit_log_flags(query_filter, author, committer, since, until)
    if expr:
        filtering_args["filter"] = expr

    # TODO: limiting by path is not yet supported.
    log_result = ctx.nessie.get_log(start_ref=ref, max_records=number, fetch_all=fetch_all, **filtering_args)
    if ctx.json:
        if fetch_all:
            click.echo(LogEntrySchema().dumps(log_result, many=True))
        else:
            commit_metas = [entry.commit_meta for entry in log_result]
            click.echo(CommitMetaSchema().dumps(commit_metas, many=True))
    else:
        click.echo_via_pager(_format_log_result(x, ref, index, fetch_all) for index, x in enumerate(log_result))


def _log_ref_and_hashes(ref: str, revision_range: str) -> Tuple[str, Optional[str], Optional[str]]:
    start_hash = None
    ref, end_hash = split_into_reference_and_hash(ref)
    if revision_range:
        if ".." in revision_range:
            start_hash, end_hash_range = revision_range.split("..")
            if len(start_hash) == 0:
                start_hash = None
            if len(end_hash_range) > 0:
                if end_hash and end_hash != end_hash_range:
                    raise click.exceptions.BadOptionUsage(
                        "revision_range", "end-hash provided via 'ref' argument and 'revision-range', use only one of those"
                    )
                end_hash = end_hash_range
        elif end_hash and end_hash != revision_range:
            raise click.exceptions.BadOptionUsage(
                "revision_range", "end-hash provided via 'ref' argument and 'revision-range', use only one of those"
            )
        else:
            end_hash = revision_range
    return ref, start_hash, end_hash


def _format_log_result(x: LogEntry, ref: str, index: int, fetch_all: bool) -> str:
    result = _format_commit_log_string(x.commit_meta.hash_, ref, index)
    result += click.style(f"Author: {x.commit_meta.author}\n")
    result += click.style(f"Date: {_format_time(x.commit_meta.commitTime)}\n")
    if fetch_all:
        result += click.style(f"Parent: {x.parent_commit_hash}\n")
        result += click.style("Operations:\n")
        if not x.operations or len(x.operations) == 0:
            result += click.style("  (None)\n")
        else:
            for operation in x.operations:
                result += click.style(f"  {operation.pretty_print()}\n")
    result += click.style(f"\n\t{x.commit_meta.message}\n\n")
    return result


def _format_commit_log_string(commit_hash: str, ref: str, index: int) -> str:
    result = click.style(f"commit {commit_hash}", fg="yellow")
    if index == 0:
        result += click.style(f" ({ref})\n", fg="green")
    else:
        result += "\n"
    return result


def _format_time(date_time: datetime.datetime) -> str:
    return date_time.astimezone(tzlocal()).strftime("%c %z")
