# -*- coding: utf-8 -*-
"""Auth schema for Nessie Client."""
from confuse import Configuration
from requests.auth import AuthBase

from .aws import login as aws_auth
from .basic import login as basic_auth
from .config import login as config_auth

__all__ = ["basic_auth", "config_auth", "auth", "aws_auth"]


def auth(base_url: str, config_dict: Configuration) -> AuthBase:
    """Authorise user against Nessie Client.

    :param base_url: base URL for Nessie Client
    :param config_dict: confuse.Configuration object
    :return: auth token
    """
    auth_type = config_dict["auth"]["type"].get()
    if auth_type == "basic":
        return config_auth(base_url, config_dict)
    elif auth_type == "aws":
        return aws_auth(config_dict["auth"]["region"].get())
    raise NotImplementedError("Auth type is unsupported " + auth_type)
