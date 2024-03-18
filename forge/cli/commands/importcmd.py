import typing
import asyncio
import logging
import argparse
from pathlib import Path
from netCDF4 import Dataset
from ..execute import Execute, ExecuteStage
from . import ParseCommand, ParseArguments

_LOGGER = logging.getLogger(__name__)


class Command(ParseCommand):
    COMMANDS: typing.List[str] = ["import"]
    HELP: str = "read data from NetCDF4 files"

    @classmethod
    def available(cls, cmd: ParseArguments.SubCommand, execute: "Execute") -> bool:
        return cmd.is_first

    @classmethod
    def install(cls, cmd: ParseArguments.SubCommand, execute: "Execute",
                parser: argparse.ArgumentParser) -> None:
        parser.add_argument('import_data',
                            help="data file or directory",
                            nargs='+')
        if cmd.is_last:
            from .export import Command as ExportCommand
            ExportCommand.install_pure(cmd, execute, parser)

    @classmethod
    def instantiate(cls, cmd: ParseArguments.SubCommand, execute: Execute,
                    parser: argparse.ArgumentParser,
                    args: argparse.Namespace, extra_args: typing.List[str]) -> None:
        cls.no_extra_args(parser, extra_args)

        execute.install(_ImportStage(execute, parser, [
            Path(p) for p in args.import_data
        ]))
        if cmd.is_last:
            from .export import Command as ExportCommand
            ExportCommand.instantiate_pure(cmd, execute, parser, args, extra_args)


class _ImportStage(ExecuteStage):
    def __init__(self, execute: Execute, parser: argparse.ArgumentParser,
                 sources: typing.List[Path]):
        super().__init__(execute)
        self._files: typing.List[Path] = list()

        def incorporate_file(p: Path):
            data_file = None
            try:
                data_file = Dataset(str(p), 'r')
            except Exception as e:
                _LOGGER.debug("Failed to open file %s", p, exc_info=True)
                parser.error(f"Invalid file '{p}': {e}")
            finally:
                if data_file is not None:
                    data_file.close()
            self._files.append(p)

        def recurse_dir(d: Path, depth: int = 0):
            if depth > 20:
                parser.error(f"Recursion depth exceeded for {d}")
            for s in d.iterdir():
                if s.is_dir():
                    recurse_dir(s, depth+1)
                    continue
                if s.suffix.lower() != '.nc':
                    continue
                incorporate_file(s)

        for s in sources:
            if s.is_dir():
                recurse_dir(s)
                continue
            incorporate_file(s)

        if len(self._files) == 0:
            parser.error("No input files found (be sure they end with '.nc' when recursing directories)")

    async def __call__(self) -> None:
        self.exec.attach_external_files(self._files)
