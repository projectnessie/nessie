# -*- coding: utf-8 -*-
"""Tests for `pynessie.auth` package."""

import requests
from assertpy import assert_that

from pynessie.auth.config import setup_auth
from pynessie.conf.config_parser import build_config


def test_auth_type_none() -> None:
    """Makes sure the auth type "none" resolves to a None authenticator."""
    config = build_config({"auth.type": "none"})
    assert_that(setup_auth(config)).is_none()


def test_auth_type_bearer() -> None:
    """Makes sure the auth type "none" resolves to a concrete authenticator object."""
    config = build_config({"auth.type": "bearer"})
    assert_that(setup_auth(config)).is_not_none()


def test_bearer_token() -> None:
    """Makes sure the bearer auth token is added to HTTP request headers."""
    config = build_config({"auth.type": "bearer", "auth.token": "test_token_123"})
    auth = setup_auth(config)
    r = requests.Request(url="http://127.255.0.0:0/", auth=auth)
    prepared = r.prepare()
    assert_that(prepared.headers.get("Authorization")).is_equal_to("Bearer test_token_123")
