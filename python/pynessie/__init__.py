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
"""Top-level package for Python API and CLI for Nessie."""
import os
from typing import Optional

import confuse

from pynessie.client import NessieClient
from pynessie.conf import build_config

__author__ = """Project Nessie"""
__email__ = "nessie-release-builder@dremio.com"
__version__ = "0.47.1"


def get_config(config_dir: Optional[str] = None, args: Optional[dict] = None) -> confuse.Configuration:
    """Retrieve a confuse Configuration object."""
    if config_dir:
        os.environ["NESSIE_CLIENTDIR"] = config_dir
    return build_config(args)


def init(config_dir: Optional[str] = None, config_dict: Optional[dict] = None) -> NessieClient:
    """Create a new Nessie client object.

    :param config_dir: optional directory to look for config in
    :param config_dict: dictionary of extra config arguments
    :return: either a simple or rich client
    :example:
    >>> client = init('/my/config/dir')
    """
    if config_dict is None:
        config_dict = {}
    config = get_config(config_dir, args=config_dict)
    return _connect(config)


def _connect(config: confuse.Configuration) -> NessieClient:
    return NessieClient(config)
