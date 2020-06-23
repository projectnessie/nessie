# -*- coding: utf-8 -*-
"""Config based auth mechanism."""
import confuse
from confuse import NotFoundError

from .basic import login as _login


def login(base_url: str, config_dict: confuse.Configuration) -> str:
    """Log into dremio using basic auth and looking for config.

    :param base_url: Dremio url
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
    return _login(base_url, username, password, timeout, verify)
