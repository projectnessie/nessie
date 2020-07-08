# -*- coding: utf-8 -*-
"""Basic auth for Nessie client."""
import requests


def login(base_url: str, username: str, password: str, timeout: int = 10, verify: bool = True) -> str:
    """Log into Nessie using basic auth.

    :param base_url: Nessie url
    :param username: username
    :param password: password
    :param timeout: optional timeout
    :param verify: If false ignore ssl errors
    :return: auth token
    """
    url = base_url + "/login"

    r = requests.post(url, data={"username": username,
            "password": password,
            "grant_type": "password"},
        timeout=timeout, verify=verify)
    r.raise_for_status()
    return r.json()["token"]
