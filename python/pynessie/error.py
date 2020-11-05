# -*- coding: utf-8 -*-
"""Nessie Exceptions."""
import functools
import sys
import traceback
from typing import Any
from typing import Callable

import click
import requests
import simplejson as json

_REMEDIES = {
    "Cannot create an unassigned tag reference": "set a valid reference on which to create this tag. eg `nessie tag tag_name main`"
}


class NessieException(Exception):
    """Base Nessie exception."""

    def __init__(self: "NessieException", msg: str, original_exception: Exception, response: requests.models.Response) -> None:
        """Construct base Nessie Exception."""
        super(NessieException, self).__init__(msg + (": %s" % original_exception))
        self.original_exception = original_exception
        try:
            parsed_response = response.json()

        except:  # NOQA
            parsed_response = dict()

        self.status_code = response.status_code
        self.server_message = parsed_response.get("message", response.text)
        self.server_status = parsed_response.get("status")
        self.server_stack_trace = parsed_response.get("serverStackTrace")
        self.url = response.url

    def json(self: "NessieException") -> str:
        """Dump this exception as a json object."""
        return json.dumps(
            dict(
                server_message=self.server_message,
                server_status=self.server_status,
                server_stack_trace=self.server_stack_trace,
                status_code=self.status_code,
                url=self.url,
                original_exception=traceback.format_tb(self.original_exception.__traceback__),
                msg=" ".join(self.args),
            )
        )


class NessieUnauthorizedException(NessieException):
    """Nessie exception for login error 401."""

    pass


class NessiePermissionException(NessieException):
    """Nessie exception for permission error 403."""

    pass


class NessieNotFoundException(NessieException):
    """Nessie exception for not found error 404."""

    pass


class NessiePreconidtionFailedException(NessieException):
    """Nessie exception for pre-condition failed error 412."""

    pass


class NessieConflictException(NessieException):
    """Nessie exception for conflict error 408."""

    pass


class NessieServerException(NessieException):
    """Nessie exception for server errors 5xx."""

    pass


def error_handler(f: Callable) -> Callable:
    """Wrap a click method to catch and pretty print errors."""

    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> None:
        """Wrapper object."""
        try:
            f(*args, **kwargs)
        except NessieException as e:
            if args[0].json:
                click.echo(e.json())
            click.echo(_format_error(e))
            sys.exit(1)

    return wrapper


def _format_error(e: NessieException) -> str:
    fmt = "{} is {} with status code: {}.\n".format(click.style("Nessie Exception", fg="red"), e, e.status_code)
    if e.server_message in _REMEDIES:
        fmt += "{} {}\n".format(click.style("FIX:", fg="green"), _REMEDIES[e.server_message])
    fmt += "{} {}\n".format(click.style("Original URL:", fg="yellow"), e.url)
    fmt += "{} {}\n".format(click.style("Server status:", fg="yellow"), e.server_status)
    fmt += "{} {}\n".format(click.style("Server message:", fg="yellow"), e.server_message)
    fmt += "{} {}\n".format(click.style("Server traceback:", fg="yellow"), e.server_stack_trace)
    return fmt
