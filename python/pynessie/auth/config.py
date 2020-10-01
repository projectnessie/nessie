# -*- coding: utf-8 -*-
"""Config based auth mechanism."""
import json
import os
import time

import confuse
from confuse import Configuration
from confuse import ConfigValueError
from confuse import NotFoundError
from requests import models
from requests.auth import AuthBase

from .basic import login as base_login


def login(base_url: str, config_dict: confuse.Configuration) -> AuthBase:
    """Return a TokenAuth auth object for standard jwt login."""
    return TokenAuth(base_url, config_dict)


def _login(base_url: str, config_dict: confuse.Configuration) -> str:
    """Log into Nessie using basic auth and looking for config.

    :param base_url: Full Nessie url
    :param config_dict: config dict
    :return: auth token
    """
    username = config_dict["auth"]["username"].get()
    if not username:
        raise RuntimeError("No username available, can't login")
    password = config_dict["auth"]["password"].get()
    if not password:
        raise RuntimeError("No password available, can't login")
    try:
        timeout = config_dict["auth"]["timeout"].get(int)
    except NotFoundError:
        timeout = 10
    try:
        verify = config_dict["verify"].get(bool)
    except NotFoundError:
        verify = 10
    return base_login(base_url, username, password, timeout, verify)


def _write_token(token: str, config_dict: Configuration) -> None:
    for source in config_dict.sources:
        if source.filename and not source.default:
            directory = os.path.dirname(source.filename)
            with open(os.path.join(directory, "auth.json"), "w") as f:
                json.dump(
                    {
                        "token": token,
                        "timestamp": time.time(),
                        "hostname": config_dict["hostname"].get(),
                        "user": config_dict["auth"]["username"].get(),
                    },
                    f,
                )
            return


def _existing_token(config_dict: Configuration) -> str:
    for source in config_dict.sources:
        if source.filename and not source.default:
            directory = os.path.dirname(source.filename)
            if os.path.exists(os.path.join(directory, "auth.json")):
                with open(os.path.join(directory, "auth.json")) as f:
                    authfile = json.load(f)
                    if _is_valid(authfile, config_dict):
                        return authfile["token"]
    raise KeyError


def _is_valid(authfile: dict, config_dict: Configuration) -> bool:
    hostname = authfile["hostname"]
    username = authfile["username"]
    try:
        expected_hostname = config_dict["hostname"].get()
        expected_username = config_dict["auth"]["username"].get()
    except (ConfigValueError, NotFoundError):
        return False
    if hostname != expected_hostname:
        return False
    if username != expected_username:
        return False
    now = time.time()
    then = authfile["timestamp"]
    return (then + 60 * 60 * 10) > now


class TokenAuth(AuthBase):
    """AuthBase override for simple token based auth."""

    def __init__(self: "TokenAuth", base_url: str, config_dict: Configuration) -> None:
        """Fetch token from service."""
        self.config_dict = config_dict
        self.base_url = base_url
        self._token = self._get_token()

    def __call__(self: "TokenAuth", r: models.PreparedRequest) -> models.PreparedRequest:
        """Append Auth Header to request."""
        r.headers["Authorization"] = "Bearer {}".format(self._token)
        return r

    def _get_token(self: "TokenAuth") -> str:
        try:
            return _existing_token(self.config_dict)
        except KeyError:
            token = _login(self.base_url, self.config_dict)
            _write_token(token, self.config_dict)
            return token
