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

"""Contents List Command CLI."""

from collections import defaultdict
from typing import List

import click

from ...cli_common_context import ContextObject, MutuallyExclusiveOption
from ...decorators import error_handler, pass_client, validate_reference
from ...model import Entries, Entry, EntrySchema
from ...utils import build_filter_for_contents_listing_flags


@click.command("list")
@click.option("-r", "--ref", help="Branch to list from. If not supplied the default branch from config is used")
@click.option(
    "-t",
    "--type",
    "entity_types",
    help="entity types to filter on, if no entity types are passed then all types are returned",
    multiple=True,
)
@click.option(
    "--filter",
    "query_filter",
    multiple=False,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["entity_type"],
    help="Allows advanced filtering using the Common Expression Language (CEL). "
    "An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n"
    "Some examples with usable variables 'entry.namespace' (string) & 'entry.contentType' (string) are:\n"
    "entry.namespace.startsWith('a.b.c')\n"
    "entry.contentType in ['ICEBERG_TABLE','DELTA_LAKE_TABLE']\n"
    "entry.namespace.startsWith('some.name.space') && entry.contentType in ['ICEBERG_TABLE','DELTA_LAKE_TABLE']\n",
)
@pass_client
@error_handler
@validate_reference
def list_(ctx: ContextObject, ref: str, query_filter: str, entity_types: List[str]) -> None:
    """List content.

    Examples:

        nessie content list -r dev -> List all contents in 'dev' branch.

        nessie content list -r dev -t DELTA_LAKE_TABLE ->  List all contents in
    'dev' branch with type `DELTA_LAKE_TABLE`.

        nessie content list -r dev --filter "entry.namespace.startsWith('some.name.space')" -> List all contents in
    'dev' branch that start with 'some.name.space'
    """
    keys = ctx.nessie.list_keys(
        ref,
        query_filter=build_filter_for_contents_listing_flags(query_filter, entity_types),
    )
    results = EntrySchema().dumps(_format_keys_json(keys), many=True) if ctx.json else _format_keys(keys)
    click.echo(results)


def _format_keys_json(keys: Entries) -> List[Entry]:
    results = []
    for k in keys.entries:
        results.append(k)
    return results


def _format_keys(keys: Entries) -> str:
    results = defaultdict(list)
    result_str = ""
    for entry in keys.entries:
        results[entry.kind].append(entry.name)
    for k, result_list in results.items():
        result_str += k + ":\n"
        for v in result_list:
            value = ['"{}"'.format(i) if "." in i else i for i in v.elements]
            result_str += "\t{}\n".format(".".join(value))
    return result_str
