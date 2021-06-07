# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Any
from typing import Generator
from typing import Tuple

import click
from dateutil.parser import parse

from pynessie import NessieClient
from pynessie.model import CommitMeta


def show_log(
    nessie: NessieClient, start_ref: str, limits: Tuple[click.Path] = None, **filtering_args: Any
) -> Generator[CommitMeta, Any, None]:
    """Fetch and filter commit log.

    Note:
        limiting by path is not yet supported.
    """
    end = filtering_args.pop("end", None)
    raw_log = nessie.get_log(start_ref=start_ref, **filtering_args)
    committer = filtering_args.get("author", None)
    after = filtering_args.get("after", None)
    before = filtering_args.get("before", None)

    # since #1325 we do filter by author/committer/before/after on the
    # server-side. However, we want to still keep client-side filtering here
    # in case the client connects to an older server that doesn't support
    # server-side filtering.
    # Once we have full backward compatibility in place, this filtering
    # here should be removed.
    def generator() -> Generator[CommitMeta, Any, None]:
        for i in raw_log:
            if committer and i.committer != committer:
                continue
            if after and _is_after(after, i.commitTime):
                continue
            if before and _is_before(before, i.commitTime):
                continue
            if end and i.hash_ == end:
                break
            yield i

    return generator()


def _is_after(after: str, commit_time: datetime) -> bool:
    return parse(after) < commit_time


def _is_before(before: str, commit_time: datetime) -> bool:
    return parse(before) < commit_time
