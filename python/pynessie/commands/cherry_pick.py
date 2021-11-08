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

"""cherry-pick CLI command."""

from typing import Tuple

import click
from click import UsageError

from ..cli_common_context import ContextObject, MutuallyExclusiveOption
from ..decorators import error_handler, pass_client, validate_reference


@click.command("cherry-pick")
@click.option("-b", "--branch", "ref", help="branch to cherry-pick onto. If not supplied the default branch from config is used")
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
    "expected_hash",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["force"],
    help="Expected hash. Only perform the action if the branch currently points to the hash specified by this option.",
)
@click.option("-s", "--source-ref", required=True, help="Name of the reference used to read the hashes from.")
@click.argument("hashes", nargs=-1, required=False)
@pass_client
@error_handler
@validate_reference
def cherry_pick(ctx: ContextObject, ref: str, force: bool, expected_hash: str, source_ref: str, hashes: Tuple[str]) -> None:
    """Cherry-pick HASHES onto another branch.

    HASHES commit hashes to be cherry-picked from the source reference.

    Examples:

        nessie cherry-pick -c 12345678abcdef -s dev 21345678abcdef 31245678abcdef -> cherry pick 2 commits with
    commit hash '21345678abcdef' '31245678abcdef' from dev branch to default branch
    with default branch's expected hash '12345678abcdef'

        nessie cherry-pick -b main -c 12345678abcdef -s dev 21345678abcdef 31245678abcdef -> cherry pick 2 commits with
    commit hash '21345678abcdef' '31245678abcdef' from dev branch to a branch named main
    with main branch's expected hash '12345678abcdef'
    """
    if not force and not expected_hash:
        raise UsageError(
            """Either condition or force must be set. Condition should be set to a valid hash for concurrency
            control or force to ignore current state of Nessie Store."""
        )
    ctx.nessie.cherry_pick(ref, source_ref, expected_hash, *hashes)
    click.echo()
