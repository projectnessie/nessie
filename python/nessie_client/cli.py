# -*- coding: utf-8 -*-
"""Console script for nessie_client."""
import sys

import click


@click.command()
def main(args=None):
    """Console script for nessie_client."""
    click.echo("Replace this message by putting your code into " "nessie_client.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
