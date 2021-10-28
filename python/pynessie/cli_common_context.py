# -*- coding: utf-8 -*-

"""Cli Common context functions that can be used by CLI commands/groups."""

from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Tuple

import attr
import click
from click import Option
from click import UsageError

from .client import NessieClient


@attr.s(auto_attribs=True)
class ContextObject(object):
    """Click context object."""

    nessie: NessieClient
    verbose: bool
    json: bool


class MutuallyExclusiveOption(Option):
    """Only allow one option in a list to be set at once."""

    def __init__(self: "MutuallyExclusiveOption", *args: List, **kwargs: Dict) -> None:
        """Instantiated a mutually exclusive option."""
        self.mutually_exclusive = set(kwargs.pop("mutually_exclusive", []))
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)  # type: ignore

    def handle_parse_result(self: "MutuallyExclusiveOption", ctx: click.Context, opts: Mapping, args: List) -> Tuple[Any, List[str]]:
        """Ensure mutually exclusive options are not used together."""
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive with " "arguments `{}`.".format(self.name, ", ".join(self.mutually_exclusive))
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)


class DefaultHelp(click.Command):
    """If no options are presented show help."""

    def __init__(self: "DefaultHelp", *args: List, **kwargs: Dict) -> None:
        """Ensure that help is shown if nothing else is selected."""
        context_settings = kwargs.setdefault("context_settings", {})
        if "help_option_names" not in context_settings:
            context_settings["help_option_names"] = ["-h", "--help"]
        self.help_flag = context_settings["help_option_names"][0]
        super(DefaultHelp, self).__init__(*args, **kwargs)  # type: ignore

    def parse_args(self: "DefaultHelp", ctx: click.Context, args: List) -> List:
        """Ensure that help is shown if nothing else is selected."""
        if not args:
            args = [self.help_flag]
        return super(DefaultHelp, self).parse_args(ctx, args)


pass_client = click.make_pass_decorator(ContextObject)
