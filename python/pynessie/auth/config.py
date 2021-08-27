# -*- coding: utf-8 -*-
"""Config based auth mechanism."""

from confuse import Configuration
from requests.auth import AuthBase

from .bearer import TokenAuth as BearerTokenAuth


def setup_auth(config: Configuration) -> AuthBase:
    """Setup an AuthBase object for future authentication against a Nessie Server.

    :param config: confuse.Configuration object
    :return: a requests.auth.AuthBase object that is able to inject authentication data into HTTP requests
    """
    auth_type = config["auth"]["type"].get()

    if auth_type == "none":
        return None
    elif auth_type == "bearer":
        token = config["auth"]["token"].get()
        return BearerTokenAuth(token)

    raise NotImplementedError("Unsupported authentication type: " + auth_type)
