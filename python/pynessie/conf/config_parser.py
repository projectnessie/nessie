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
"""Parser for confuse Configuration object."""
import os
from typing import Optional

import confuse


def _get_env_args() -> dict:
    args = {}
    for k, v in os.environ.items():
        if "NESSIE_" in k and k != "NESSIE_CLIENTDIR":
            name = k.replace("NESSIE_", "").lower().replace("_", ".")
            if name == "auth.timeout":
                v = int(v)  # type: ignore
            args[name] = v
    return args


def build_config(args: Optional[dict] = None) -> confuse.Configuration:
    """Build configuration object from input params, env variables and yaml file."""
    config = confuse.Configuration("nessie", __name__)
    env_args = _get_env_args()
    config.set_args(env_args, dots=True)
    if args:
        config.set_args(args, dots=True)
    config["auth"]["password"].redact = True
    return config
