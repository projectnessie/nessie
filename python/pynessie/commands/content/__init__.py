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

"""Top-level package for Nessie Contents CLI commands."""

import click

from pynessie.cli_common_context import ContextObject
from pynessie.commands.content.commit import commit
from pynessie.commands.content.list_ import list_
from pynessie.commands.content.view import view
from pynessie.decorators import pass_client


@click.group(name="content")
@pass_client
def content(ctx: ContextObject) -> None:
    """View, list content, and commit changes."""
    pass


content.add_command(list_)
content.add_command(view)
content.add_command(commit)
