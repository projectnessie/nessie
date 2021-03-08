# -*- coding: utf-8 -*-
"""Top-level package for Python API and CLI for Nessie."""
import os

import confuse

from .conf import build_config
from .nessie_client import NessieClient

__author__ = """Project Nessie"""
__email__ = "nessie-release-builder@dremio.com"
__version__ = "0.4.0"


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
