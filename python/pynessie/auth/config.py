# -*- coding: utf-8 -*-
"""Config based auth mechanism."""

from confuse import Configuration
from requests.auth import AuthBase
from requests.auth import HTTPBasicAuth

from .aws import setup_aws_auth
from .bearer import TokenAuth as BearerTokenAuth


def setup_auth(config: Configuration) -> AuthBase:
    """Setup an AuthBase object for future authentication against a Nessie Server.

    :param config: confuse.Configuration object
    :return: a requests.auth.AuthBase object that is able to inject authentication data into HTTP requests
    """
    auth_type = config["auth"]["type"].get()

    if auth_type == "none":
        return None
    elif auth_type == "basic":
        username = config["auth"]["username"].get()
        password = config["auth"]["password"].get()
        return HTTPBasicAuth(username, password)
    elif auth_type == "bearer":
        token = config["auth"]["token"].get()
        return BearerTokenAuth(token)
    elif auth_type == "aws":
        region = config["auth"]["region"].get()
        return setup_aws_auth(region)

    raise NotImplementedError("Unsupported authentication type: " + auth_type)
