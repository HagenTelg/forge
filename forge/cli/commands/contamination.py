import typing
import asyncio
import logging
import argparse
import time
from netCDF4 import Dataset
from forge.processing.average.contamination import invalidate_contamination
from ..execute import Execute, ExecuteStage
from . import ParseCommand, ParseArguments


_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["contamination", "contam"]
    HELP: str = "invalidate contaminated data"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return not cmd.is_first and not cmd.is_last

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        parser.add_argument('--station',
                            dest='station',
                            help="override the station used for contamination")
        parser.add_argument('--tag',
                            dest='tags',
                            help="override the tags used for data identification")

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        cls.no_extra_args(parser, extra_args)

        override_tags: typing.Optional[typing.Set[str]] = None
        if args.tags is not None:
            override_tags = set([t.strip() for t in args.tags.split(',')])
            override_tags.discard("")

        execute.install(RemoveContaminationStage(execute, args.station, override_tags))


class RemoveContaminationStage(ExecuteStage):
    def __init__(
            self, execute: Execute,
            override_station: typing.Optional[str] = None,
            override_tags: typing.Optional[typing.Set[str]] = None
    ):
        super().__init__(execute)
        self.override_station = override_station.lower() if override_station else None
        self.override_tags = override_tags

    async def __call__(self) -> None:
        begin_time = time.monotonic()
        for file in self.data_file_progress("Invalidating contaminated data", write=True):
            file = Dataset(str(file), 'r+')
            try:
                invalidate_contamination(file, self.override_station, self.override_tags)
            finally:
                file.close()
        _LOGGER.debug("Contamination filter completed in %.3f seconds", time.monotonic() - begin_time)
