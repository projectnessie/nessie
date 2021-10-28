# -*- coding: utf-8 -*-
"""Authentication classes for nessie client."""

from .aws import setup_aws_auth
from .config import setup_auth

__all__ = ["setup_aws_auth", "setup_auth"]
