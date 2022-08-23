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

"""Contents Commit Command CLI."""
from json import JSONDecodeError
from typing import List, Optional

import click

from pynessie.cli_common_context import ContextObject
from pynessie.client import NessieClient
from pynessie.decorators import error_handler, pass_client, validate_reference
from pynessie.error import NessieCliError, NessieNotFoundException
from pynessie.model import Content, ContentKey, ContentSchema, Delete, Operation, Put
from pynessie.types import CONTENT_KEY


@click.command("commit")
@click.option("-r", "--ref", help="Branch to commit content to. If not supplied the default branch from config is used.")
@click.option("-i", "--stdin", is_flag=True, help="Read content for --set from STDIN (separated by Ctrl-D)")
@click.option(
    "-s",
    "--expect-same-content",
    is_flag=True,
    help="Send the same content both as the new and expected (old contents) parameters for the edit operations",
)
@click.option(
    "-c",
    "--condition",
    "expected_hash",
    required=True,
    help="Expected hash. Only perform the action if the branch currently points to the hash specified by this option.",
)
@click.option("-R", "--remove-key", "delete", is_flag=True, help="Delete a content.")
@click.option("-m", "--message", help="Commit message.")
@click.option("--author", help="The author to use for the commit")
@click.argument("key", nargs=-1, required=True, type=CONTENT_KEY)
@pass_client
@error_handler
@validate_reference
def commit(
    ctx: ContextObject,
    ref: str,
    stdin: bool,
    expect_same_content: bool,
    expected_hash: str,
    delete: bool,
    message: str,
    author: str,
    key: List[ContentKey],
) -> None:
    """Commit content.

        KEY is the content key that is associated with a specific content to commit.
    This accepts as well multiple keys with space in between. The key can be in this format:
    'table.key' or 'namespace."table.key"'.

    Examples:

        nessie content commit -m "my awesome commit message" -r dev -c 122345abcd my_table -> Commit to branch 'dev'
    on expected hash '122345abcd' (head of the branch) the content from the interactive editor with the
    commit message "my awesome commit message" for content "my_table".

        nessie content commit -i -r dev -c 122345abcd my_table -> Commit to branch 'dev'
    on expected hash '122345abcd' (head of the branch) the content from STDIN with the commit message from
    the interactive editor for content "my_table".

        nessie content commit -m "my awesome commit message" -R -r dev -c 122345abcd my_table_1 my_table_2 ->
    Delete contents "my_table_1" and "my_table_2" from branch 'dev' on expected hash '122345abcd' and commit
    to branch 'dev" with the commit message "my awesome commit message".
    """
    operations = _get_commit_operations(ctx.nessie, ref, key, delete, stdin, expect_same_content)

    if operations:
        ctx.nessie.commit(ref, expected_hash, _get_message(message, key), author, *operations)
        click.echo("Changes have been committed.")
    else:
        click.echo("No changes requested")


def _get_commit_operations(
    client: NessieClient, ref: str, keys: List[ContentKey], is_delete: bool, use_stdin: bool = False, expect_same_contents: bool = False
) -> List[Operation]:
    if is_delete:
        return _get_delete_content_operations(keys)

    return _get_edit_content_operations(client, ref, keys, use_stdin, expect_same_contents)


def _get_delete_content_operations(keys: List[ContentKey]) -> List[Operation]:
    contents_operations: List[Operation] = []
    for content_key in keys:
        contents_operations.append(Delete(content_key))

    return contents_operations


def _get_edit_content_operations(
    client: NessieClient, ref: str, keys: List[ContentKey], use_stdin: bool = False, expect_same_contents: bool = False
) -> List[Operation]:
    contents_operations: List[Operation] = []
    for content_key in keys:
        content_original = _get_existing_content(client, ref, content_key)
        formatted_content_key = content_key.to_string()

        content_json = _get_edited_json_content(content_key, content_original, use_stdin)

        if _shall_skip_key(content_json, content_original):
            click.echo("Skipping key " + formatted_content_key + " (content not edited)")
            continue

        if _is_delete_operation(content_json, content_original):
            contents_operations.append(Delete(content_key))
            click.echo("Deleting content for key " + formatted_content_key)
            continue

        # If the user has an invalid JSON, we raise an Error
        try:
            content_new = ContentSchema().loads(content_json)
        except JSONDecodeError as e:
            raise NessieCliError("Invalid JSON Error", e.msg) from e

        # if we have the same content as the original one, we just skip
        if content_new == content_original:
            click.echo("Skipping key " + formatted_content_key + " (content not edited)")
            continue

        click.echo("Setting content for key " + formatted_content_key)
        if content_new.requires_expected_state():
            content_expected = content_new if expect_same_contents else content_original
            contents_operations.append(Put(content_key, content_new, content_expected))
        else:
            contents_operations.append(Put(content_key, content_new))

    return contents_operations


def _get_existing_content(client: NessieClient, ref: str, key: ContentKey) -> Optional[Content]:
    # get current contents, if present
    try:
        return client.get_content(ref, key)
    except NessieNotFoundException:
        return None


def _get_edited_json_content(key: ContentKey, content_orig: Optional[Content], use_stdin: bool) -> Optional[str]:
    if use_stdin:
        click.echo("Enter JSON content for key " + key.to_string())
        return click.get_text_stream("stdin").read().strip("\n")

    # allow the user to provide interactive input via the shell $EDITOR
    edited_content = _edit_contents(key.to_string(), (ContentSchema().dumps(content_orig) if content_orig else ""))

    if edited_content is not None:
        return edited_content.strip()

    return edited_content


def _edit_contents(key: str, body: str) -> Optional[str]:
    marker = "# Everything below is ignored\n"
    edit_message = (
        body
        + "\n\n"
        + marker
        + "Edit the content above to commit changes."
        + " Closing without change will result in a no-op."
        + "\nContent Key: "
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


def _shall_skip_key(content_json: Optional[str], content_original: Optional[Content]) -> bool:
    if content_json is None:
        # Skip the key if user for example exit with no editing e.g: :q!
        return True

    if not content_json and content_original is None:
        # Skip if no content (empty text) were edited and no existing content
        return True

    return False


def _is_delete_operation(content_json: Optional[str], content_original: Optional[Content]) -> bool:
    if not content_json and content_original is not None:
        # We let the user to remove content from existing content key which will be treated as DELETE op
        return True

    return False


def _get_message(message: str, keys: List[ContentKey]) -> Optional[str]:
    if message:
        return message
    return _get_commit_message(keys)


def _get_commit_message(keys: List[ContentKey]) -> Optional[str]:
    marker = "# Everything below is ignored\n"
    message = click.edit("\n\n" + marker + "Edit the commit message above: " + "\n".join([i.to_string() for i in keys]))
    return message.split(marker, 1)[0].rstrip("\n") if message is not None else None
