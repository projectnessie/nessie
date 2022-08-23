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
"""Console script for nessie_client."""
import sys
from typing import Any

import click
import confuse

from pynessie import __version__
from pynessie.cli_common_context import ContextObject
from pynessie.client import NessieClient
from pynessie.commands import (
    branch_,
    cherry_pick,
    config,
    content,
    diff,
    log,
    merge,
    reflog,
    remote,
    tag,
)
from pynessie.conf import build_config


def _print_version(ctx: Any, param: Any, value: Any) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo("nessie version " + __version__)
    ctx.exit()


@click.group("nessie")
@click.option("--json", is_flag=True, help="write output in json format.")
@click.option("-v", "--verbose", is_flag=True, help="Verbose output.")
@click.option("--endpoint", help="Optional endpoint, if different from config file.")
@click.option("--auth-token", help="Optional bearer auth token, if different from config file.")
@click.option("--version", is_flag=True, callback=_print_version, expose_value=False, is_eager=True)
@click.pass_context
def cli(ctx: click.core.Context, json: bool, verbose: bool, endpoint: str, auth_token: str) -> None:
    """Nessie cli tool.

    Interact with Nessie branches and tables via the command line
    """
    try:
        cfg_map = {}
        if endpoint:
            cfg_map["endpoint"] = endpoint
        if auth_token:
            cfg_map["auth.type"] = "bearer"
            cfg_map["auth.token"] = auth_token
        nessie = NessieClient(build_config(cfg_map))
        ctx.obj = ContextObject(nessie, verbose, json)
    except confuse.exceptions.ConfigTypeError as e:
        raise click.ClickException(str(e)) from e


cli.add_command(remote)
cli.add_command(config)
cli.add_command(log)
cli.add_command(branch_)
cli.add_command(tag)
cli.add_command(merge)
cli.add_command(cherry_pick)
cli.add_command(content)
cli.add_command(diff)
cli.add_command(reflog)


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    sys.exit(cli())  # pragma: no cover
