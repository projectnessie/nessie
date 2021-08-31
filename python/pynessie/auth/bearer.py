# -*- coding: utf-8 -*-
"""Bearer auth for Nessie client."""
from requests import models
from requests.auth import AuthBase


class TokenAuth(AuthBase):
    """AuthBase override for bearer token based auth."""

    def __init__(self: "TokenAuth", token: str) -> None:
        """Create an AuthBase for the specified bearer auth token."""
        self._token = token

    def __call__(self: "TokenAuth", r: models.PreparedRequest) -> models.PreparedRequest:
        """Append Auth Header to request."""
        r.headers["Authorization"] = "Bearer {}".format(self._token)
        return r
