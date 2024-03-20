import typing
import asyncio
import logging
import argparse
import time
import numpy as np
from netCDF4 import Dataset
from forge.timeparse import parse_interval_argument
from forge.formattime import format_iso8601_duration
from forge.processing.average.file import average_file
from forge.processing.average.calculate import FixedIntervalFileAverager, MonthFileAverager, FileAverager
from ..execute import Execute, ExecuteStage
from . import ParseCommand, ParseArguments
from .netcdf import MergeInstrument


_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["avg", "average"]
    HELP: str = "invalidate contaminated data"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return True

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        if cmd.is_first:
            from .get import Command as GetCommand
            GetCommand.install_pure(cmd, execute, parser)
        if cmd.is_last:
            from .export import Command as ExportCommand
            ExportCommand.install_pure(cmd, execute, parser)

        if not cmd.is_first:
            parser.add_argument('interval',
                                default='1H',
                                help="averaging interval",
                                nargs='?')
        else:
            parser.add_argument('--interval',
                                dest='interval',
                                default='1H',
                                help="averaging interval")

        parser.add_argument('--include-contamination',
                            dest='keep_contam', action='store_true',
                            help="do not remove contaminated data before averaging")

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        from .get import Command as GetCommand, FilterStage
        if cmd.is_first:
            GetCommand.instantiate_pure(cmd, execute, parser, args, extra_args)
        else:
            cls.no_extra_args(parser, extra_args)

        interval = args.interval
        if interval.lower() == 'month' or interval == 'P1M' or interval == '1mo':
            averager = MonthFileAverager
            file_interval = "P1M"
        else:
            try:
                interval = parse_interval_argument(interval) * 1000
            except ValueError:
                parser.error(f"invalid averaging interval '{args.interval}'")

            def make_averager(times_epoch_ms, averaged_time_ms, nominal_spacing_ms):
                return FixedIntervalFileAverager(interval, times_epoch_ms, averaged_time_ms,
                                                 nominal_spacing_ms)

            averager = make_averager
            file_interval = format_iso8601_duration(interval)

        if not args.keep_contam:
            from .contamination import RemoveContaminationStage
            execute.install(RemoveContaminationStage(execute))

        execute.install(MergeInstrument(execute))
        execute.install(_AverageStage(execute, averager, file_interval))

        if cmd.is_last:
            FilterStage.instantiate_if_available(
                execute,
                retain_statistics=(args.stddev or args.count or args.quantiles)
            )
            from .export import Command as ExportCommand
            ExportCommand.instantiate_pure(cmd, execute, parser, args, extra_args)


class _AverageStage(ExecuteStage):
    def __init__(
            self,
            execute: Execute,
            make_averager: typing.Callable[[np.ndarray, typing.Optional[np.ndarray], typing.Optional[typing.Union[int, float]]], FileAverager],
            file_interval: typing.Optional[str] = None,
    ):
        super().__init__(execute)
        self.make_averager = make_averager
        self.file_interval = file_interval

    async def __call__(self) -> None:
        begin_time = time.monotonic()
        with self.data_replacement() as output_path:
            for input_file in self.data_file_progress("Averaging data", write=True):
                output_file = Dataset(str(output_path / input_file.name), 'w', format='NETCDF4')
                input_file = Dataset(str(input_file), 'r')
                try:
                    average_file(input_file, output_file, self.make_averager)
                    if self.file_interval:
                        output_file.setncattr("time_coverage_resolution", self.file_interval)
                finally:
                    input_file.close()
                    output_file.close()
        _LOGGER.debug("Averaging completed in %.3f seconds", time.monotonic() - begin_time)
