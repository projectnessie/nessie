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

"""config CLI command."""

import click

from pynessie.cli_common_context import (
    ContextObject,
    DefaultHelp,
    MutuallyExclusiveOption,
)
from pynessie.conf import process
from pynessie.decorators import error_handler, pass_client


@click.command("config", cls=DefaultHelp)
@click.option("--get", cls=MutuallyExclusiveOption, help="get config parameter", mutually_exclusive=["set", "is_list", "unset"])
@click.option("--add", cls=MutuallyExclusiveOption, help="set config parameter", mutually_exclusive=["get", "is_list", "unset"])
@click.option(
    "-l",
    "--list",
    "is_list",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="list config parameters",
    mutually_exclusive=["set", "get", "unset"],
)
@click.option("--unset", cls=MutuallyExclusiveOption, help="unset config parameter", mutually_exclusive=["get", "is_list", "set"])
@click.option("--type", "input_type", help="type to interpret config value to set or get. Allowed options: bool, int")
@click.argument("key", nargs=1, required=False)
@pass_client
@error_handler
def config(ctx: ContextObject, get: str, add: str, is_list: bool, unset: str, input_type: str, key: str) -> None:
    """Set and view config."""
    res = process(get, add, is_list, unset, key, input_type)
    click.echo(res)
