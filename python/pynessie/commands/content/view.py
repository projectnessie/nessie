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

"""Contents View Command CLI."""
from typing import List

import click

from pynessie.types import CONTENT_KEY
from ... import NessieClient
from ...cli_common_context import ContextObject
from ...decorators import error_handler, pass_client, validate_reference
from ...model import Content, ContentKey, ContentSchema


@click.command("view")
@click.option("-r", "--ref", help="Branch to list from. If not supplied the default branch from config is used.")
@click.argument("key", nargs=-1, required=True, type=CONTENT_KEY)
@pass_client
@error_handler
@validate_reference
def view(ctx: ContextObject, ref: str, key: List[ContentKey]) -> None:
    """View content.

        KEY is the content key that is associated with a specific content to view.
    This accepts as well multiple keys with space in between. The key can be in this format:
    'table.key' or 'namespace."table.key"'.

    Examples:

        nessie content view -r dev my_table -> View content details for content "my_table"
    in 'dev' branch.
    """
    if ctx.json:
        results = ContentSchema().dumps(_get_content_for_all_keys(ctx.nessie, ref, key), many=True)
    else:
        results = "\n".join((i.pretty_print() for i in _get_content_for_all_keys(ctx.nessie, ref, key)))

    click.echo(results)


def _get_content_for_all_keys(client: NessieClient, ref: str, keys: List[ContentKey]) -> List[Content]:
    contents: List[Content] = []

    for key in keys:
        contents.append(client.get_content(ref, key))

    return contents
