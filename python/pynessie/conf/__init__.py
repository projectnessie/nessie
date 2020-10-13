# -*- coding: utf-8 -*-
"""Config classes for nessie client."""
from io import StringIO
import os

import confuse
import yaml

from .config_parser import build_config

__all__ = ["build_config", "to_dict", "write"]


def to_dict(config: confuse.Configuration) -> dict:
    """Convert confuse Configuration object to dict."""
    ys = config.dump(redact=True)
    fd = StringIO(ys)
    dct = yaml.safe_load(fd)
    return dct


def write(config: confuse.Configuration) -> None:
    """Write updated config to file"""
    config_filename = os.path.join(config.config_dir(), confuse.CONFIG_FILENAME)
    with open(config_filename, 'w') as f:
        f.write(config.dump())

