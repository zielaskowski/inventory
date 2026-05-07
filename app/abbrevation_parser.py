"""
Abbreviation parser: try to match shorted arguments
also store arguments in log class
"""

import argparse
import sys
from typing import Dict

from app.common import match_from_list
from app.error import AmbigousMatchError, NoMatchError
from app.sql import log


class AbbreviationParser(argparse.ArgumentParser):
    """override argparser to provide arguments abbreviation"""

    def _get_abbreviation(  # pylint: disable=inconsistent-return-statements
        self,
        cmd: str,
        choices: Dict,
    ) -> str:
        if cmd in ["-h", "--help"]:
            return cmd
        try:
            return match_from_list(cmd=cmd, choices=choices)
        except AmbigousMatchError as err:
            self.error(str(err))
        except NoMatchError as err:
            self.error(str(err))

    def parse_args(self, args: list | None = None, namespace=None):  # type: ignore
        if args is None:
            args = sys.argv[1:]
        # only subcommand given so show help
        if len(args) == 1:
            args += ["-h"]

        # in case choices are None
        try:
            choices = (
                    self # pylint: disable=protected-access
                    ._subparsers
                    ._actions[1]  # pyright: ignore[reportAssignmentType,reportOptionalMemberAccess]
                    .choices
                    ) # fmt: skip
        except AttributeError:
            self.error("No arguments added to argparser")

        # Check if the first positional argument is an abbreviation of a subcommand
        if args and args[0] in choices:
            arg_namespace = super().parse_args(args, namespace)
            log.log(arg_namespace)
            return arg_namespace

        if args and args[0]:
            full_cmd = self._get_abbreviation(
                args[0],
                choices,  # pyright: ignore[reportArgumentType]
            )
            args[0] = full_cmd  # pyright: ignore
        arg_namespace = super().parse_args(args, namespace)
        log.log(arg_namespace)
        return arg_namespace
