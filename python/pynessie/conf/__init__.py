# -*- coding: utf-8 -*-
"""Config classes for nessie client."""
from io import StringIO

import confuse
import yaml

from .config_command import process
from .config_parser import build_config
from .io import write_to_file

__all__ = ["build_config", "to_dict", "write_to_file", "process"]


def to_dict(config: confuse.Configuration) -> dict:
    """Convert confuse Configuration object to dict."""
    ys = config.dump(redact=True)
    fd = StringIO(ys)
    dct = yaml.safe_load(fd)
    return dct
