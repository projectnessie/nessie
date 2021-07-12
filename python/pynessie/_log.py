# -*- coding: utf-8 -*-
from typing import Any
from typing import Generator
from typing import Tuple

import click

from pynessie import NessieClient
from pynessie.model import CommitMeta


def show_log(
    nessie: NessieClient, start_ref: str, limits: Tuple[click.Path] = None, **filtering_args: Any
) -> Generator[CommitMeta, Any, None]:
    """Fetch and filter commit log.

    Note:
        limiting by path is not yet supported.
    """
    start = filtering_args.pop("start", None)
    end = filtering_args.pop("end", None)
    raw_log = nessie.get_log(start_ref=start_ref, **filtering_args)

    def generator() -> Generator[CommitMeta, Any, None]:
        found_start = False
        for i in raw_log:
            if start and i.hash_ == start:
                found_start = True
            if end and i.hash_ == end:
                break
            if found_start:
                yield i

    return generator()
