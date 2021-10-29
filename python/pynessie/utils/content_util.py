# -*- coding: utf-8 -*-

"""Utility functions that can be used to manipulate contents like keys and values."""

import re

from ..model import ContentsKey

_dot_regex = re.compile('\\.(?=([^"]*"[^"]*")*[^"]*$)')


def format_key(raw_key: str) -> str:
    """Format given key based on regex."""
    elements = _dot_regex.split(raw_key)
    return ".".join(i.replace(".", "\0") for i in elements if i)


def contents_key(raw_key: str) -> ContentsKey:
    """Format given content key based on regex."""
    elements = _dot_regex.split(raw_key)
    return ContentsKey([i for i in elements if i])
