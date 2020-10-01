# -*- coding: utf-8 -*-
"""Config classes for nessie client."""
from io import StringIO

import confuse
import yaml

from .config_parser import build_config

__all__ = ["build_config", "to_dict"]


def to_dict(config: confuse.Configuration) -> dict:
    """Convert confuse Configuration object to dict."""
    ys = config.dump(redact=True)
    fd = StringIO(ys)
    dct = yaml.safe_load(fd)
    return dct
