import typing
import asyncio
import logging
import argparse
import time
from math import floor, ceil
from netCDF4 import Dataset
from forge.timeparse import parse_time_bounds_arguments
from ..execute import Execute, ExecuteStage
from . import ParseCommand, ParseArguments
from .get import DataSelection, Command as GetCommand, FilterStage, ArchiveRead


_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["select"]
    HELP: str = "select variables"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return not cmd.is_first and not cmd.is_last

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        parser.add_argument('data',
                            help="data selections")
        parser.add_argument('time',
                            help="time selection",
                            nargs='?')

        parser.add_argument('--discard-statistics',
                            dest='retain_statistics', action='store_false',
                            help="remove statistics variables if present and only the parent matched")
        parser.set_defaults(retain_statistics=True)

        parser.epilog = GetCommand.DATA_DESCRIPTION + " " + GetCommand.TIME_DESCRIPTION

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        if args.time:
            time_args = [args.time] + extra_args
            try:
                start, end = parse_time_bounds_arguments(time_args)
            except ValueError:
                _LOGGER.debug("Error parsing time arguments", exc_info=True)
                parser.error(f"The time specification '{' '.join(time_args)}' is not valid")
                raise
            start_ms = int(floor(start.timestamp() * 1000))
            end_ms = int(ceil(end.timestamp() * 1000))
        else:
            start_ms = None
            end_ms = None
            for stage in reversed(execute.stages):
                if isinstance(stage, ArchiveRead):
                    if stage.keep_all:
                        break
                    start_ms = stage.start_ms
                    end_ms = stage.end_ms
                    break

        data_selection = DataSelection(args.data, parser)

        execute.install(SelectStage(
            execute, data_selection,
            start_ms, end_ms,
            retain_statistics=args.retain_statistics,
        ))


class SelectStage(FilterStage):
    pass
