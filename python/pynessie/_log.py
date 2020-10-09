# -*- coding: utf-8 -*-
from typing import Any
from typing import Generator
from typing import Optional
from typing import Tuple

import click
from dateutil.parser import parse

from pynessie import NessieClient
from pynessie.model import CommitMeta


def show_log(
    nessie: NessieClient,
    start: str,
    number: Optional[int] = -1,
    after: Optional[str] = None,
    before: Optional[str] = None,
    committer: Optional[str] = None,
    end: Optional[str] = None,
    limits: Tuple[click.Path] = None,
) -> Generator[CommitMeta, Any, None]:
    """Fetch and filter commit log.

    Note:
        limiting by path is not yet supported.
    """
    raw_log = nessie.get_log(start)

    def generator() -> Generator[CommitMeta, Any, None]:
        for i in range(number if number > 0 else len(raw_log)):
            if committer and raw_log[i].commiter != committer:
                continue
            if after and _is_after(after, raw_log[i].commitTime):
                continue
            if before and _is_before(before, raw_log[i].commitTime):
                continue
            if end and raw_log[i].hash_ == end:
                raise StopIteration
            yield raw_log[i]

    return generator()


def _is_after(after: str, commit_time: int) -> bool:
    return parse(after).timestamp() * 1000 < commit_time


def _is_before(before: str, commit_time: int) -> bool:
    return parse(before).timestamp() * 1000 < commit_time
