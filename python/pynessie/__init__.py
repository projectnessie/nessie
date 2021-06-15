# -*- coding: utf-8 -*-
"""Top-level package for Python API and CLI for Nessie."""
import os
import re
from typing import Tuple

import confuse
from packaging.version import _BaseVersion, Version

from .conf import build_config
from .nessie_client import NessieClient

__author__ = """Project Nessie"""
__email__ = "nessie-release-builder@dremio.com"
__version__ = "0.7.1"


def __make_version_range() -> Tuple[_BaseVersion, _BaseVersion]:
    match = re.match("([0-9]+)[.]([0-9]+)[.].*", __version__)
    if not match:
        raise Exception(f"Invalid pynessie package version string {__version__}")
    major = int(match.group(1))
    minor = int(match.group(2))
    return Version(f"{major}.{minor}.0"), Version(f"{major}.{minor + 1}.0")


__required_version_range__ = __make_version_range()


def get_config(config_dir: str = None, args: dict = None) -> confuse.Configuration:
    """Retrieve a confuse Configuration object."""
    if config_dir:
        os.environ["NESSIE_CLIENTDIR"] = config_dir
    return build_config(args)


def init(config_dir: str = None, config_dict: dict = None) -> NessieClient:
    """Create a new Nessie client object.

    :param config_dir: optional directory to look for config in
    :param config_dict: dictionary of extra config arguments
    :return: either a simple or rich client
    :example:
    >>> client = init('/my/config/dir')
    """
    if config_dict is None:
        config_dict = dict()
    config = get_config(config_dir, args=config_dict)
    return _connect(config)


def _connect(config: confuse.Configuration) -> NessieClient:
    return NessieClient(config)
