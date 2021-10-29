# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests for `pynessie.auth` package."""

import os
from pathlib import Path

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


def test_aws_auth_profile(tmp_path: Path) -> None:
    """Makes sure the request is signed according to AWS auth requirements."""
    aws_credentials = tmp_path / "aws.config"
    aws_credentials.write_text(
        """
[test1]
aws_access_key_id=test1_key
aws_secret_access_key=test1_secret
aws_session_token=test1_session
    """
    )
    config = build_config({"auth.type": "aws", "auth.region": "us-west-2", "auth.profile": "test1"})
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(aws_credentials)
    auth = setup_auth(config)
    r = requests.Request(url="http://127.255.0.0:0/", method="GET", auth=auth)
    prepared = r.prepare()
    assert_that(prepared.headers.get("Authorization")).matches(
        r"AWS4-HMAC-SHA256 Credential=test1_key/[0-9]+/us-west-2/execute-api/aws4_request, "
        r"SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=.+"
    )
