# -*- coding: utf-8 -*-
"""Parser for confuse Configuration object."""
import os

import confuse


def _get_env_args() -> dict:
    args = dict()
    for k, v in os.environ.items():
        if "NESSIE_" in k and k != "NESSIE_CLIENTDIR":
            name = k.replace("NESSIE_", "").lower().replace("_", ".")
            if name == "auth.timeout":
                v = int(v)  # type: ignore
            args[name] = v
    return args


def build_config(args: dict = None) -> confuse.Configuration:
    """Build configuration object from input params, env variables and yaml file."""
    config = confuse.Configuration("nessie", __name__)
    if args:
        config.set_args(args, dots=True)
    env_args = _get_env_args()
    config.set_args(env_args, dots=True)
    config["auth"]["password"].redact = True
    return config
