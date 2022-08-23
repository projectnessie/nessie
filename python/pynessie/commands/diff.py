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

"""diff CLI command."""

import click

from pynessie.cli_common_context import ContextObject
from pynessie.decorators import error_handler, pass_client
from pynessie.model import DiffResponseSchema, split_into_reference_and_hash


@click.command("diff")
@click.argument("from_ref", nargs=1, required=True)
@click.argument("to_ref", nargs=1, required=True)
@pass_client
@error_handler
def diff(ctx: ContextObject, from_ref: str, to_ref: str) -> None:  # noqa: C901
    """Show diff between two given references.

    'from_ref'/'to_ref': name of branch or tag to use to show the diff

    'from_ref' and 'to_ref' can be either:

        The name of a reference (branch or tag), resolving to the HEAD of the
        named reference.

        The name of a reference (branch or tag) with a commit-id on that named
        reference.

        A detached commit-id/hash. Example:

    Examples:

        nessie diff from_ref to_ref -> show diff between 'from_ref' and 'to_ref', using the HEADs of both references

        nessie diff main@1234567890abcdef my-branch -> compare main branch at commit-id 1234567890abcdef with the HEAD of my-branch

        nessie diff 1234567890abcdef main -> compare the main branch w/ commit-id 1234567890abcdef

    """
    from_ref, from_hash_on_ref = split_into_reference_and_hash(from_ref)

    to_ref, to_hash_on_ref = split_into_reference_and_hash(to_ref)

    diff_response = ctx.nessie.get_diff(from_ref=from_ref, to_ref=to_ref, from_hash_on_ref=from_hash_on_ref, to_hash_on_ref=to_hash_on_ref)
    if ctx.json:
        click.echo(DiffResponseSchema().dumps(diff_response))
    else:
        click.echo_via_pager(x.pretty_print() + "\n" for index, x in enumerate(diff_response.diffs))
