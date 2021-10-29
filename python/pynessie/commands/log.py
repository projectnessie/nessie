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

import click
from dateutil.tz import tzlocal

from ..cli_common_context import ContextObject, MutuallyExclusiveOption, pass_client
from ..error import error_handler
from ..model import CommitMeta, CommitMetaSchema
from ..utils import build_query_expression_for_commit_log_flags


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
    "--query",
    "--query-expression",
    "query_expression",
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
@pass_client
@error_handler
def log(  # noqa: C901
    ctx: ContextObject,
    ref: str,
    number: int,
    since: str,
    until: str,
    author: List[str],
    committer: List[str],
    revision_range: str,
    query_expression: str,
) -> None:
    """Show commit log.

    REF name of branch or tag to use to show the commit logs

    Examples:

        nessie log -> show commit logs using the configured default branch

        nessie log dev -> show commit logs for 'dev' branch

        nessie log -n 5 dev -> show commit logs for 'dev' branch limited by 5 commits

        nessie log --revision-range 12345678abcdef..12345678efghj dev -> show commit logs in range of hash
    '12345678abcdef' and '12345678efghj' in 'dev' branch

        nessie log --author nessie.user dev -> show commit logs for user 'nessie.user' in 'dev' branch

        nessie log --query "commit.author == 'nessie_user2' || commit.author == 'non_existing'" dev ->
    show commit logs using query in 'dev' branch

        nessie log --after "2019-01-01T00:00:00+00:00" --before "2021-01-01T00:00:00+00:00" dev ->
    show commit logs between "2019-01-01T00:00:00+00:00" and "2021-01-01T00:00:00+00:00" in 'dev' branch

    """
    if not ref:
        ref = ctx.nessie.get_default_branch()
    start_hash = None
    end_hash = None
    if revision_range:
        if ".." in revision_range:
            start_hash, end_hash = revision_range.split("..")
        else:
            end_hash = revision_range

    filtering_args: Any = {}
    if start_hash:
        filtering_args["startHash"] = start_hash
    if end_hash:
        filtering_args["endHash"] = end_hash
    expr = build_query_expression_for_commit_log_flags(query_expression, author, committer, since, until)
    if expr:
        filtering_args["query_expression"] = expr

    # TODO: limiting by path is not yet supported.
    log_result = ctx.nessie.get_log(start_ref=ref, max_records=number, **filtering_args)
    if ctx.json:
        click.echo(CommitMetaSchema().dumps(log_result, many=True))
    else:
        click.echo_via_pager(_format_log_result(x, ref, index) for index, x in enumerate(log_result))


def _format_log_result(x: CommitMeta, ref: str, index: int) -> str:
    result = _format_commit_log_string(x.hash_, ref, index)
    result += click.style("Author: {}\n".format(x.author))
    result += click.style("Date: {}\n".format(_format_time(x.commitTime)))
    result += click.style("\n\t{}\n\n".format(x.message))
    return result


def _format_commit_log_string(commit_hash: str, ref: str, index: int) -> str:
    result = click.style("commit {}".format(commit_hash), fg="yellow")
    if index == 0:
        result += click.style(" ({})\n".format(ref), fg="green")
    else:
        result += "\n"
    return result


def _format_time(dt: datetime.datetime) -> str:
    return dt.astimezone(tzlocal()).strftime("%c %z")
