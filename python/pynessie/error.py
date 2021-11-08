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
"""Nessie Exceptions."""
import functools
import sys
from typing import Any
from typing import Callable
from typing import Optional

import click
import simplejson as json

_REMEDIES = {
    "Cannot create an unassigned tag reference": "set a valid reference on which to create this tag. eg `nessie tag tag_name main`"
}


class NessieException(Exception):
    """Base Nessie exception."""

    def __init__(
        self: "NessieException",
        parsed_response: dict,
        status: int,
        url: str,
        reason: str,
        msg: str = None,
    ) -> None:
        """Construct base Nessie Exception."""
        if "message" in parsed_response:
            exception_msg = reason + ": " + parsed_response["message"]
        elif msg:
            exception_msg = reason + ": " + msg
        else:
            # If we did not get a JSON response for a 500 error, use a generic message.
            # In this case the HTML response contents are likely not user-friendly anyway.
            if 500 <= status <= 599:
                exception_msg = "Internal Server Error"
            else:
                exception_msg = "Unknown Error"

        super().__init__(exception_msg)

        self.status_code = status
        self.server_message = parsed_response.get("message", str(parsed_response))
        self.server_status = parsed_response.get("status", "UNKNOWN")
        self.error_code = parsed_response.get("errorCode", "UNKNOWN")
        self.server_stack_trace = parsed_response.get("serverStackTrace")
        self.url = url

    def json(self: "NessieException") -> str:
        """Dump this exception as a json object."""
        return json.dumps(
            dict(
                server_message=self.server_message,
                error_code=self.error_code,
                server_status=self.server_status,
                server_stack_trace=self.server_stack_trace,
                status_code=self.status_code,
                url=self.url,
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
    """Generic Nessie exception for not found error 404."""

    pass


class NessieReferenceNotFoundException(NessieNotFoundException):
    """This exception is thrown when a requested reference is not present in the store."""

    pass


class NessieContentNotFoundException(NessieNotFoundException):
    """This exception is thrown when the requested content object is not present in the store."""

    pass


class NessiePreconidtionFailedException(NessieException):
    """Nessie exception for pre-condition failed error 412."""

    pass


class NessieConflictException(NessieException):
    """Generic Nessie exception for conflict error 409."""

    pass


class NessieReferenceAlreadyExistsException(NessieConflictException):
    """Reference not found.

    This exception is thrown when a reference could not be created because another reference with the same name is
    already present in the store.
    """

    pass


class NessieReferenceConflictException(NessieConflictException):
    """Expected hash did not match actual hash on a reference.

    This exception is thrown when the hash associated with a named reference does not match with the hash provided
    by the caller.
    """

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
            else:
                click.echo(_format_error(e))
            sys.exit(1)

    return wrapper


def _create_nessie_exception(error: dict, status: int, reason: str, url: str) -> Optional[Exception]:
    if "errorCode" not in error:
        return None

    error_code = error["errorCode"]
    if error_code == "REFERENCE_NOT_FOUND":
        return NessieReferenceNotFoundException(error, status, url, reason)
    if error_code == "REFERENCE_ALREADY_EXISTS":
        return NessieReferenceAlreadyExistsException(error, status, url, reason)
    if error_code == "CONTENT_NOT_FOUND":
        return NessieContentNotFoundException(error, status, url, reason)
    if error_code == "REFERENCE_CONFLICT":
        return NessieReferenceConflictException(error, status, url, reason)
    return None


def _create_exception(error: dict, status: int, reason: str, url: str) -> Exception:
    if 400 <= status < 500:
        reason = f"Client Error {reason}"
    elif 500 <= status < 600:
        reason = f"Server Error {reason}"

    ex = _create_nessie_exception(error, status, reason, url)
    if ex:
        return ex

    if status == 412:
        return NessiePreconidtionFailedException(error, status, url, reason, msg="Unable to complete transaction, please retry.")
    if status == 401:
        return NessieUnauthorizedException(error, status, url, reason, msg="Unauthorized to access API endpoint")
    if status == 403:
        return NessiePermissionException(error, status, url, reason, msg="Insufficient permissions to access " + url)
    if status == 404:
        return NessieNotFoundException(error, status, url, reason, msg="Entity not found at " + url)
    if status == 409:
        return NessieConflictException(error, status, url, reason, msg="Unable to complete transaction.")
    if 500 <= status <= 599:
        return NessieServerException(error, status, url, reason)
    return NessieException(error, status, url, reason)


def _format_error(e: NessieException) -> str:
    fmt = "{} (Status code: {})\n".format(click.style(e, fg="red"), e.status_code)
    if e.server_message in _REMEDIES:
        fmt += "{} {}\n".format(click.style("FIX:", fg="green"), _REMEDIES[e.server_message])
    fmt += "{} {}\n".format(click.style("Requested URL:", fg="yellow"), e.url)
    fmt += "{} {}\n".format(click.style("Server status:", fg="yellow"), e.server_status)
    fmt += "{} {}\n".format(click.style("Error code:", fg="yellow"), e.error_code)
    fmt += "{} {}\n".format(click.style("Server message:", fg="yellow"), e.server_message)
    fmt += "{} {}\n".format(click.style("Server traceback:", fg="yellow"), e.server_stack_trace)
    return fmt
