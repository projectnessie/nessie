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
    yaml_config = config.dump(redact=True)
    dct = yaml.safe_load(StringIO(yaml_config))
    return dct
