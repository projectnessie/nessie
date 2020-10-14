# -*- coding: utf-8 -*-
from typing import Any
from typing import List
from typing import Tuple

import click

from pynessie import NessieClient


def show_log(nessie: NessieClient, start: str = None, end: str = None, limits: Tuple[click.Path] = None) -> List[Any]:
    pass
