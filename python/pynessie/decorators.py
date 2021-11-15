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

"""Top-level decorators."""
import functools
import sys
from typing import Any, Callable

import click
from marshmallow import ValidationError

from .cli_common_context import ContextObject
from .error import NessieCliError, NessieException


# Decorator to pass Nessie client down into Click sub-commands.
pass_client = click.make_pass_decorator(ContextObject)


def validate_reference(f: Callable) -> Callable:
    """Decorator to validate Nessie reference and override it with default if needed."""

    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        """Wrapper Decorator."""
        # Nessie context is always as the first argument
        if len(args) < 1 or not isinstance(args[0], ContextObject):
            raise RuntimeError("Use this decorator only with Nessie context.")

        ctx: ContextObject = args[0]

        # Check for ref and override with default if is not set
        if "ref" in kwargs:
            kwargs["ref"] = kwargs["ref"] if kwargs["ref"] else ctx.nessie.get_default_branch()

        return f(*args, **kwargs)

    return wrapper


def error_handler(f: Callable) -> Callable:
    """Decorator to catch and pretty print errors."""

    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        """Wrapper object."""
        try:
            f(*args, **kwargs)
        except NessieCliError as e:
            if args[0].json:
                click.echo(e.json())
            else:
                click.echo(_format_cli_error(e.title, e.msg))
        except ValidationError as e:
            if args[0].json:
                click.echo(e.messages)
            else:
                click.echo(_format_cli_error("Validation Error", e.messages))
            sys.exit(1)
        except NessieException as e:
            if args[0].json:
                click.echo(e.json())
            else:
                click.echo(_format_error(e))
            sys.exit(1)

    return wrapper


def _format_error(e: NessieException) -> str:
    fmt = "{} (Status code: {})\n".format(click.style(e, fg="red"), e.status_code)
    fmt += "{} {}\n".format(click.style("Requested URL:", fg="yellow"), e.url)
    fmt += "{} {}\n".format(click.style("Server status:", fg="yellow"), e.server_status)
    fmt += "{} {}\n".format(click.style("Error code:", fg="yellow"), e.error_code)
    fmt += "{} {}\n".format(click.style("Server message:", fg="yellow"), e.server_message)
    fmt += "{} {}\n".format(click.style("Server traceback:", fg="yellow"), e.server_stack_trace)
    return fmt


def _format_cli_error(title: str, message: Any) -> str:
    fmt = "{}: \n".format(click.style(title, fg="red"))
    fmt += click.style(message, fg="yellow")
    return fmt
