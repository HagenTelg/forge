import typing
import asyncio
import logging
import argparse
import shutil
from math import floor
from pathlib import Path
from tempfile import TemporaryDirectory, NamedTemporaryFile
from netCDF4 import Dataset
from forge.data.merge.instrument import MergeInstrument as MergeFiles
from forge.data.merge.flatten import MergeFlatten as FlattenFiles
from forge.timeparse import parse_iso8601_time
from forge.const import MAX_I64
from . import ParseCommand, ParseArguments
from ..execute import Execute, ExecuteStage

_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["netcdf", "nc"]
    HELP: str = "write NetCDF4 native Forge data files"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return cmd.is_last

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        if cmd.is_first:
            from .get import Command as GetCommand
            GetCommand.install_pure(cmd, execute, parser)

        parser.add_argument('--directory',
                            dest='directory',
                            help="output directory instead of the current one")
        parser.add_argument('--merge',
                            choices=['none', 'instrument', 'all'],
                            default='instrument',
                            help="data file merging")
        parser.add_argument('--output',
                            help="output data file for full merging")
        parser.add_argument('--align-state',
                            dest='align_state', action='store_true',
                            help="align state to data times when full merging")
        parser.add_argument('--no-round-times',
                            dest='no_round_times', action='store_true',
                            help="disable time rounding when full merging")

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        if args.merge != 'all':
            if args.output:
                parser.error("--output is only valid with --merge=all")
            if args.align_state:
                parser.error("--align-state is only valid with --merge=all")
            if args.no_round_times:
                parser.error("--no-round-times is only valid with --merge=all")

        from .get import Command as GetCommand, FilterStage
        if cmd.is_first:
            GetCommand.instantiate_pure(cmd, execute, parser, args, extra_args)
        else:
            cls.no_extra_args(parser, extra_args)
        FilterStage.instantiate_if_available(execute)

        if args.merge == 'instrument':
            execute.install(MergeInstrument(execute))
        elif args.merge == 'all':
            execute.install(MergeInstrument(execute))
            execute.install(MergeFlatten(execute, Path(args.output) if args.output else Path("data.nc"),
                                         align_state=args.align_state,
                                         round_times=not args.no_round_times))
            return

        execute.install(_MoveOutput(execute, Path(args.directory) if args.directory else Path(".")))


class _MoveOutput(ExecuteStage):
    def __init__(self, execute: Execute, destination: Path):
        super().__init__(execute)
        self.destination = destination

    async def __call__(self) -> None:
        _LOGGER.debug(f"Moving files to {self.destination}")
        for input_file in self.data_file_progress("Copying files"):
            output_file = self.destination / input_file.name
            if input_file.is_symlink():
                await asyncio.get_event_loop().run_in_executor(None, shutil.copy, str(input_file), str(output_file))
            else:
                await asyncio.get_event_loop().run_in_executor(None, shutil.move, str(input_file), str(output_file))
            _LOGGER.debug(f"Moved data file to {output_file}")


class MergeInstrument(ExecuteStage):
    class _FileSet:
        def __init__(self, output_file: Path):
            self.output_file = output_file
            self.input_files: typing.List[typing.Tuple[int, Path]] = list()

        def execute(self) -> None:
            if len(self.input_files) == 1:
                shutil.copy(str(self.input_files[0][1]), str(self.output_file))
                return

            def do_merge(input_files: typing.Iterable[Path], output_file: Path) -> None:
                open_files: typing.List[Dataset] = list()
                try:
                    merge = MergeFiles()
                    for add_file in input_files:
                        add_file = Dataset(str(add_file), 'r')
                        open_files.append(add_file)
                        merge.overlay(add_file)
                    merge.execute(output_file).close()
                finally:
                    for file in open_files:
                        file.close()

            if len(self.input_files) <= 32:
                do_merge([f[1] for f in self.input_files], self.output_file)
                return

            with TemporaryDirectory() as merge_dir:
                def split_merge(merge_files: typing.List[Path], output_file: Path) -> None:
                    if len(merge_files) <= 32:
                        do_merge(merge_files, output_file)
                        return

                    block_size = int(len(merge_files) // 16)
                    combined_files: typing.List[NamedTemporaryFile] = list()
                    try:
                        for idx in range(0, len(merge_files), block_size):
                            destination = NamedTemporaryFile(dir=merge_dir)
                            combined_files.append(destination)
                            split_merge(merge_files[idx:idx+block_size], Path(destination.name))

                        do_merge([f.name for f in combined_files], output_file)
                    finally:
                        for f in combined_files:
                            f.close()

                self.input_files.sort(key=lambda x: x[0])
                split_merge([f[1] for f in self.input_files], self.output_file)

    async def __call__(self) -> None:
        with self.progress("Merging instruments") as progress, self.data_replacement() as output_path:
            merge_sets: typing.Dict[str, MergeInstrument._FileSet] = dict()
            for input_file in self.data_files():
                input_data = Dataset(str(input_file), 'r')
                try:
                    file_id: typing.List[str] = list()
                    station_var = input_data.variables.get("station_name")
                    if station_var is not None:
                        file_id.append(str(station_var[0]).upper())

                    archive = getattr(input_data, 'forge_archive', None)
                    if archive:
                        file_id.append(archive.upper())

                    instrument_id = getattr(input_data, 'instrument_id', None)
                    if not instrument_id:
                        _LOGGER.warning(f"No instrument available for {input_file}")
                        continue
                    file_id.append(instrument_id.upper())

                    start_time = getattr(input_data, 'time_coverage_start', None)
                    if start_time is not None:
                        start_time = int(floor(parse_iso8601_time(str(start_time)).timestamp() * 1000.0))
                    if not start_time:
                        start_time = -MAX_I64

                    file_id: str = '-'.join(file_id)
                    merge_target = merge_sets.get(file_id)
                    if merge_target is None:
                        merge_target = self._FileSet(output_path / f"{file_id}.nc")
                        merge_sets[file_id] = merge_target
                    merge_target.input_files.append((start_time, input_file))
                finally:
                    input_data.close()

            _LOGGER.debug(f"Located {len(merge_sets)} merge sets of files")

            count_completed: int = 0
            for file_id, merge_files in merge_sets.items():
                progress.set_title(f"Merging {file_id}")
                progress(count_completed / len(merge_sets))

                _LOGGER.debug(f"Merging {len(merge_files.input_files)} files for {file_id}")
                merge_files.execute()
                count_completed += 1


class MergeFlatten(ExecuteStage):
    def __init__(
            self,
            execute: Execute, destination: typing.Optional[Path] = None,
            align_state: bool = False,
            round_times: bool = True,
    ):
        super().__init__(execute)
        self.destination = destination
        self.align_state = align_state
        self.round_times = round_times

    def _do_merge(self, output_file: Path) -> None:
        open_files: typing.List[Dataset] = list()
        try:
            unique_stations: typing.Set[str] = set()
            unique_archives: typing.Set[str] = set()

            merge_files: typing.List[typing.Tuple[Dataset, str, str, str]] = list()
            for file in self.data_files():
                file = Dataset(str(file), 'r')
                open_files.append(file)

                station_name = file.variables.get("station_name")
                if station_name is not None:
                    station_name = str(station_name[0]).upper()
                    unique_stations.add(station_name)
                else:
                    station_name = ""

                instrument_id = str(getattr(file, 'instrument_id', ""))
                forge_archive = str(getattr(file, 'forge_archive', "")).upper()
                if forge_archive:
                    unique_archives.add(forge_archive)

                merge_files.append((file, station_name, forge_archive, instrument_id))

            merge = FlattenFiles(self.align_state, self.round_times)
            for file, station, archive, instrument in merge_files:
                merge_name: typing.List[str] = list()
                if len(unique_stations) > 1 and station:
                    merge_name.append(station)
                if len(unique_archives) > 1 and archive:
                    merge_name.append(archive)
                merge_name.append(instrument)
                merge.add_source(file, '_'.join(merge_name))
            merge.execute(output_file).close()
        finally:
            for file in open_files:
                file.close()

    async def __call__(self) -> None:
        with self.progress("Flattening data"):
            if not self.destination:
                with self.data_replacement() as output_path:
                    self._do_merge(output_path / "data.nc")
            else:
                self._do_merge(self.destination)

