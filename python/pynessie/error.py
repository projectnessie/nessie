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
from typing import Optional

import simplejson as json


class NessieException(Exception):
    """Base Nessie exception."""

    def __init__(
        self,
        parsed_response: dict,
        status: int,
        url: str,
        reason: str,
        msg: Optional[str] = None,
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

    def json(self) -> str:
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


class NessieCliError(Exception):
    """Base Nessie CLI related errors."""

    def __init__(self, title: str, msg: Optional[str] = None) -> None:
        """Construct base Nessie CLI Error."""
        super().__init__()

        self.title = title
        self.msg = msg

    def json(self) -> str:
        """Dump this error as a json object."""
        return json.dumps(dict(title=self.title, message=self.msg))


def _create_nessie_exception(error: dict, status: int, reason: str, url: str) -> Optional[NessieException]:
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


def _create_exception(error: dict, status: int, reason: str, url: str) -> NessieException:
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
