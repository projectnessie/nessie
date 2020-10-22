# -*- coding: utf-8 -*-
"""IO methods to read and write config."""
import os

import confuse


def write_to_file(config: confuse.Configuration) -> None:
    """Write updated config to file."""
    config_filename = os.path.join(config.config_dir(), confuse.CONFIG_FILENAME)
    with open(config_filename, "w") as f:
        f.write(config.dump())
