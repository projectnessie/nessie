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
"""Generate pynessie docs script."""
import os
from pathlib import Path
from typing import IO, List, Optional

from click import Group
from click.testing import CliRunner

from pynessie import cli

PATH_DOCS = "docs"


def generate_docs() -> None:
    """Generate all the commands docs."""
    print("Generating docs....\n\n")
    # Write the initial CLI help doc
    _write_command_doc([])
    # Go through all the commands items and generate the docs
    _generate_commands_docs([], cli.cli)


def _write_command_doc(command: List[str]) -> None:
    with _open_doc_file(command) as f:
        _write_command_output_to_file(f, command + ["--help"])


def _open_doc_file(command: List[str]) -> IO:
    file_name = _get_file_name_from_command(command)
    cwd = os.getcwd()
    file_full_path = Path(Path(cwd), PATH_DOCS, file_name)
    print(f"Writing file: {file_full_path.parent}/{file_full_path.name}")
    return open(file_full_path, "w", encoding="UTF-8")


def _get_file_name_from_command(command: List[str]) -> str:
    return f"{'_'.join(command) if len(command) > 0 else 'main'}.rst"


def _write_command_output_to_file(file_io: IO, command: List[str]) -> None:
    result = _run_cli(command)
    file_io.write(".. code-block:: bash\n\n")
    for line in result.splitlines():
        indent = "   " if line else ""
        file_io.write(f"{indent}{line}\n")
    file_io.write("\n\n")


def _run_cli(args: List[str], input_data: Optional[str] = None) -> str:
    return CliRunner().invoke(cli.cli, args, input=input_data).output


def _generate_commands_docs(parent_commands: List[str], command_group: Group) -> None:
    for name, value in command_group.commands.items():
        command = parent_commands + [name]
        if isinstance(value, Group):
            _write_command_group_doc(command, list(value.commands.keys()))
            _generate_commands_docs(command, value)
        else:
            _write_command_doc(command)


def _write_command_group_doc(command: List[str], command_items: List[str]) -> None:
    with _open_doc_file(command) as f:
        _write_command_output_to_file(f, command + ["--help"])
        f.write("It contains the following sub-commands:\n\n")
        for item in command_items:
            _write_sub_command_reference_to_file(f, command + [item])


def _write_sub_command_reference_to_file(file_io: IO, command: List[str]) -> None:
    command_title = " ".join([c.capitalize() for c in command])

    file_io.write(f"{command_title} Command\n")
    file_io.write("~~~~~~~~~\n\n")
    file_io.write(f".. include:: {_get_file_name_from_command(command)}\n\n")


if __name__ == "__main__":
    generate_docs()  # pragma: no cover
