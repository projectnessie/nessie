from typing import Tuple, List, Any

import click

from pynessie import NessieClient


def show_log(nessie: NessieClient, start: str = None, end: str = None, limits: Tuple[click.Path] = None) -> List[Any]:
    pass
