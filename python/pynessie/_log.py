# -*- coding: utf-8 -*-
from typing import Any
from typing import Generator
from typing import Tuple

import click

from pynessie import NessieClient
from pynessie.model import CommitMeta


def show_log(nessie: NessieClient, limits: Tuple[click.Path] = None, **filtering_args: str) -> Generator[CommitMeta, Any, None]:
    """Fetch and filter commit log.

    Note:
        limiting by path is not yet supported.
    """
    end = filtering_args.pop("end", None)
    raw_log = nessie.get_log(**filtering_args)

    def generator() -> Generator[CommitMeta, Any, None]:
        for i in raw_log:
            if end and i.hash_ == end:
                break
            yield i

    return generator()
