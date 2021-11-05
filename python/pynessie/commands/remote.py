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

"""Remote CLI group command."""

import click

from ..cli_common_context import ContextObject
from ..conf import process
from ..decorators import error_handler, pass_client


@click.group()
@pass_client
def remote(ctx: ContextObject) -> None:
    """Set and view remote endpoint."""
    pass


@remote.command("show")
@pass_client
@error_handler
def show(ctx: ContextObject) -> None:
    """Show current remote."""
    click.echo("Remote URL: " + ctx.nessie.get_base_url())
    click.echo("Default branch: " + ctx.nessie.get_reference(None).name)
    click.echo("Remote branches: ")
    for i in ctx.nessie.list_references().references:
        click.echo("\t" + i.name)


@remote.command(name="add")
@click.argument("endpoint", nargs=1, required=True)
@pass_client
@error_handler
def set_(ctx: ContextObject, endpoint: str) -> None:
    """Set current remote."""
    click.echo(process(None, "endpoint", False, None, endpoint))


@remote.command("set-head")
@click.argument("head", nargs=1, required=True)
@click.option("-d", "--delete", is_flag=True, help="delete the default branch")
@pass_client
@error_handler
def set_head(ctx: ContextObject, head: str, delete: bool) -> None:
    """Set current default branch. If -d is passed it will remove the default branch."""
    if delete:
        click.echo(process(None, None, False, "default_branch", None))
    else:
        click.echo(process(None, "default_branch", False, None, head))
