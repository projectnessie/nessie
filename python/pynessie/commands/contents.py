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

"""contents CLI command."""

from collections import defaultdict
from typing import Any, Generator, Optional
from typing import List

import click

from ..cli_common_context import ContextObject, MutuallyExclusiveOption, pass_client
from ..client import NessieClient
from ..error import error_handler, NessieNotFoundException
from ..model import Contents, ContentsSchema, Delete, Entries, Entry, EntrySchema, Operation, Put
from ..utils import build_query_expression_for_contents_listing_flags, contents_key, format_key


@click.command("contents")
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
    "-i",
    "--stdin",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="read contents for --set from STDIN (separated by Ctrl-D)",
    mutually_exclusive=["list", "delete"],
)
@click.option(
    "-S",
    "--expect-same-contents",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="send the same contents both as the new and expected (old contents) parameters for --set operations",
    mutually_exclusive=["list", "delete"],
)
@click.option(
    "-c",
    "--condition",
    "expected_hash",
    help="Expected hash. Only perform the action if the branch currently points to the hash specified by this option.",
)
@click.option("-r", "--ref", help="branch to list from. If not supplied the default branch from config is used")
@click.option("-m", "--message", help="commit message")
@click.option(
    "-t",
    "--type",
    "entity_types",
    help="entity types to filter on, if no entity types are passed then all types are returned",
    multiple=True,
)
@click.option(
    "--query",
    "--query-expression",
    "query_expression",
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
@click.option("--author", help="The author to use for the commit")
@click.argument("key", nargs=-1, required=False)
@pass_client
@error_handler
def contents(
    ctx: ContextObject,
    list: bool,
    delete: bool,
    set: bool,
    stdin: bool,
    expect_same_contents: bool,
    key: List[str],
    ref: str,
    message: str,
    expected_hash: str,
    entity_types: List[str],
    author: str,
    query_expression: str,
) -> None:
    """Contents operations.

    KEY name of object to view, delete. If listing the key will limit by namespace what is included.
    """
    if list:
        keys = ctx.nessie.list_keys(
            ref if ref else ctx.nessie.get_default_branch(),
            query_expression=build_query_expression_for_contents_listing_flags(query_expression, entity_types),
        )
        results = EntrySchema().dumps(_format_keys_json(keys, *key), many=True) if ctx.json else _format_keys(keys, *key)
    elif delete or set:
        ops = _get_contents(ctx.nessie, ref, delete, stdin, expect_same_contents, *key)
        if ops:
            ctx.nessie.commit(ref, expected_hash, _get_message(message), author, *ops)
        else:
            click.echo("No changes requested")
        results = ""
    else:

        def content(*x: str) -> Generator[Contents, Any, None]:
            return ctx.nessie.get_values(ref if ref else ctx.nessie.get_default_branch(), *x)

        results = ContentsSchema().dumps(content(*key), many=True) if ctx.json else "\n".join((i.pretty_print() for i in content(*key)))
    click.echo(results)


def _edit_contents(key: str, body: str) -> Optional[str]:
    marker = "# Everything below is ignored\n"
    edit_message = (
        body
        + "\n\n"
        + marker
        + "Edit the content above to commit changes."
        + " Closing without change will result in a no-op."
        + " Removing the content results in a delete"
        + "\nContents Key: "
        + key
    )
    try:
        message = click.edit(edit_message)
        # Note: None can occur when the user quits the editor without saving
        if message is None:
            return None
        message_altered = message.split(marker, 1)[0].strip("\n")
        return message_altered
    except click.ClickException:
        pass
    return None


def _get_contents(
    nessie: NessieClient, ref: str, delete: bool = False, use_stdin: bool = False, expect_same_contents: bool = False, *keys: str
) -> List[Operation]:
    contents_altered: List[Operation] = list()
    for raw_key in keys:
        key = format_key(raw_key)
        content_orig = None

        try:  # get current contents, if present
            content = nessie.get_values(ref, key)
            content_orig = content.__next__()
        except NessieNotFoundException:
            pass

        if delete:
            content_json = None
        elif use_stdin:
            click.echo("Enter JSON contents for key " + key)
            content_json = click.get_text_stream("stdin").read()
            content_json = content_json.strip("\n")
        else:  # allow the user to provide interactive input via the shell $EDITOR
            content_json = _edit_contents(key, (ContentsSchema().dumps(content_orig) if content_orig else ""))
            if content_json is None:
                click.echo("Skipping key " + key + " (contents not edited)")
                continue

        if content_json:
            click.echo("Setting contents for key " + key)
            content_new = ContentsSchema().loads(content_json)
            if content_new.requires_expected_state():
                content_expected = content_new if expect_same_contents else content_orig
                contents_altered.append(Put(contents_key(raw_key), content_new, content_expected))
            else:
                contents_altered.append(Put(contents_key(raw_key), content_new))
        else:
            click.echo("Deleting contents for key " + key)
            contents_altered.append(Delete(contents_key(raw_key)))

    return contents_altered


def _get_commit_message(*keys: str) -> Optional[str]:
    MARKER = "# Everything below is ignored\n"
    message = click.edit("\n\n" + MARKER + "Edit the commit message above." + "\n".join(keys))
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
