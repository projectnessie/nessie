# -*- coding: utf-8 -*-
"""Nessie Exceptions."""
import requests


class NessieException(Exception):
    """Base Nessie exception."""

    def __init__(
        self: "NessieException", msg: str, original_exception: Exception, response: requests.models.Response = None
    ) -> None:
        """Construct base Nessie Exception."""
        super(NessieException, self).__init__(msg + (": %s" % original_exception))
        self.original_exception = original_exception
        self.response = response


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
