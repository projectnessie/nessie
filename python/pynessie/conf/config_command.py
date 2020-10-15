# -*- coding: utf-8 -*-
from typing import Optional
from typing import Union

import click
import confuse

from .config_parser import build_config
from .io import write_to_file


def process(
    get_op: Optional[str],
    add_op: Optional[str],
    list_op: bool,
    unset_op: Optional[str],
    key: Optional[str],
    type_str: Optional[str] = "str",
) -> str:
    if (get_op or list or unset_op) and key:
        raise click.UsageError("can't set argument and specify option flags")
    if get_op or (key and not add_op):
        config = get_key(get_op or key)
        return config.get(get_type(type_str))
    if add_op or unset_op:
        config = build_config()
        config.set_args({add_op: set_type(key, type_str) if add_op else None}, dots=True)
        write_to_file(config)
        return ""
    if list_op:
        config = build_config()
        return config.dump(redact=True)


def get_type(type_str: str) -> type:
    if type_str == "bool":
        return bool
    elif type_str == "int":
        return int
    else:
        return str


def set_type(key: str, type_str: str) -> Union[bool, int, str]:
    if type_str == "bool":
        return key.lower() in {"true", "1", "t", "y", "yes", "yeah", "yup", "certainly", "uh-huh"}
    elif type_str == "int":
        return int(key)
    else:
        return key


def get_key(key: str) -> confuse.Configuration:
    config = build_config()
    for i in key.split("."):
        if not i:
            continue
        config = config[i]
    return config
