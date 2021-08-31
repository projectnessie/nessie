# -*- coding: utf-8 -*-
"""Tests for `pynessie.auth` package."""

import os

import requests
from assertpy import assert_that

from pynessie.auth.config import setup_auth
from pynessie.conf.config_parser import build_config


def test_auth_type_none() -> None:
    """Makes sure the auth type "none" resolves to a None authenticator."""
    config = build_config({"auth.type": "none"})
    assert_that(setup_auth(config)).is_none()


def test_bearer_token() -> None:
    """Makes sure the bearer auth token is added to HTTP request headers."""
    config = build_config({"auth.type": "bearer", "auth.token": "test_token_123"})
    auth = setup_auth(config)
    r = requests.Request(url="http://127.255.0.0:0/", auth=auth)
    prepared = r.prepare()
    assert_that(prepared.headers.get("Authorization")).is_equal_to("Bearer test_token_123")


def test_basic_auth() -> None:
    """Makes sure the basic auth token is added to HTTP request headers."""
    config = build_config({"auth.type": "basic", "auth.username": "user_123", "auth.password": "pwd_123"})
    auth = setup_auth(config)
    r = requests.Request(url="http://127.255.0.0:0/", auth=auth)
    prepared = r.prepare()
    assert_that(prepared.headers.get("Authorization")).is_equal_to("Basic dXNlcl8xMjM6cHdkXzEyMw==")


def test_aws_auth() -> None:
    """Makes sure the request is signed according to AWS auth requirements."""
    config = build_config({"auth.type": "aws", "auth.region": "us-west-2"})
    os.environ["AWS_ACCESS_KEY_ID"] = "test_key_id"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test_secret_key"  # noqa: S105
    auth = setup_auth(config)
    r = requests.Request(url="http://127.255.0.0:0/", method="GET", auth=auth)
    prepared = r.prepare()
    assert_that(prepared.headers.get("Authorization")).matches(
        r"AWS4-HMAC-SHA256 Credential=test_key_id/[0-9]+/us-west-2/execute-api/aws4_request, "
        r"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=.+"
    )
