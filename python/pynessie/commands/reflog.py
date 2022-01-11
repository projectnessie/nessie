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

"""reflog CLI command."""

import datetime
from typing import Any

import click
from dateutil.tz import tzlocal

from ..cli_common_context import ContextObject
from ..decorators import error_handler, pass_client
from ..model import ReflogEntry, ReflogEntrySchema


@click.command("reflog")
@click.option("-n", "--number", help="number of reflog entries to return", type=int)
@click.option(
    "-r",
    "--revision-range",
    help="Hash to start viewing reflog from. If of the form '<start_hash>'..'<end_hash>' "
    "only show reflog for given range on the particular ref that was provided. Both the '<end_hash>' "
    "and '<start_hash>' is inclusive.",
)
@pass_client
@error_handler
def reflog(  # noqa: C901
    ctx: ContextObject,
    number: int,
    revision_range: str,
) -> None:
    """Show reflog.

    Reflog entries from the current HEAD or from the specified revision_range.

    Examples:

        nessie reflog -> show reflog entries from the current HEAD

        nessie reflog -n 5 -> show reflog entries from the current HEAD limited by 5 entries

        nessie reflog --revision-range 12345678abcdef..12345678efghj -> show reflog entries in range of hash
    '12345678abcdef' and '12345678efghj' (both inclusive)

    """
    start_hash = None
    end_hash = None
    if revision_range:
        if ".." in revision_range:
            start_hash, end_hash = revision_range.split("..")
        else:
            end_hash = revision_range

    query_params: Any = {}
    if start_hash:
        query_params["startHash"] = start_hash
    if end_hash:
        query_params["endHash"] = end_hash

    log_result = ctx.nessie.get_reflog(max_records=number, **query_params)
    if ctx.json:
        click.echo(ReflogEntrySchema().dumps(log_result, many=True))
    else:
        click.echo_via_pager(_format_reflog_result(x) for x in log_result)


def _format_reflog_result(x: ReflogEntry) -> str:
    result = click.style(f"reflog {x.reflog_id}", fg="yellow")
    result += click.style(f" ({x.ref_type} : {x.ref_name})\n", fg="green")
    result += click.style(f"Operation: {x.operation}\n")
    result += click.style(f"OperationTime: {_format_time(x.operation_time)}\n")
    result += click.style(f"CommitHash: {x.commit_hash}\n")
    result += click.style(f"ParentReflogId: {x.parent_reflog_id}\n")
    result += click.style("SourceHashes:\n")
    if not x.source_hashes or len(x.source_hashes) == 0:
        result += click.style("  (None)\n")
    else:
        for source_hash in x.source_hashes:
            result += click.style(f"  {source_hash}\n")
    result += click.style("\n")
    return result


def _format_time(epoch_micro: int) -> str:
    epoch_seconds = epoch_micro / 1000000
    return datetime.datetime.fromtimestamp(epoch_seconds, tz=tzlocal()).strftime("%c %z")
