import typing
import asyncio
import logging
import sys
import os
from pathlib import Path
from math import floor
from shutil import copyfile
from tempfile import TemporaryDirectory, mkstemp
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from forge.archive.client.connection import Connection

_LOGGER = logging.getLogger(__name__)


def mkstemp_like(original: Path) -> typing.Tuple[int, str]:
    base, dot, suffix = original.name.partition('.')
    base += '_u'
    return mkstemp(
        prefix=base,
        suffix=dot + suffix,
        dir=str(original.parent)
    )


class Progress:
    def __init__(self, exec: "Execute", title: str):
        self.exec = exec
        self.title = title
        self.fraction: typing.Optional[float] = None
        self._done_output: bool = False
        self._enable = not exec.stderr_is_output and sys.stderr.isatty()

    def __enter__(self):
        self.exec._progress_stack.append(self)
        self._update()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        check = self.exec._progress_stack.pop()
        assert check == self
        if self.exec._progress_stack:
            self.exec._progress_stack[-1]._update()
            return
        if not self._done_output:
            return
        sys.stderr.write(f"\x1B[2K\r")
        sys.stderr.flush()

    def __call__(self, fraction: typing.Optional[float]) -> None:
        self.fraction = fraction
        self._update()

    def set_title(self, title: str, reset: bool = False) -> None:
        if reset:
            self.fraction = None
        self.title = title
        if self._done_output:
            self._update()

    def _update(self) -> None:
        if not self._enable:
            return
        if self.exec._progress_stack[-1] != self:
            return
        try:
            width, _ = os.get_terminal_size(sys.stderr.fileno())
        except OSError:
            return
        self._done_output = True
        sys.stderr.write(f"\x1B[2K\r")

        if self.fraction is None:
            sys.stderr.write(self.title[:width - 1])
            sys.stderr.flush()
            return

        prefix = self.title[:width - 13] + ": ["
        percent = max(1, min(round(self.fraction * 100), 99))
        suffix = f"] {percent:2d}%"

        sys.stderr.write(prefix)

        total_bars = (width - 1) - (len(prefix) + len(suffix))
        filled_bars = total_bars * self.fraction
        complete_bars = int(floor(filled_bars))
        fractional_bar = filled_bars - complete_bars
        if complete_bars:
            sys.stderr.write("=" * complete_bars)
        if fractional_bar >= 0.5:
            sys.stderr.write("-")
            complete_bars += 1
        empty_bars = total_bars - complete_bars
        if empty_bars:
            sys.stderr.write(" " * empty_bars)

        sys.stderr.write(suffix)
        sys.stderr.flush()


class ExecuteStage(ABC):
    def __init__(self, execute: "Execute"):
        self.exec = execute
        self.idx = len(execute.stages)

    async def before(self) -> None:
        pass

    async def after(self) -> None:
        pass

    @abstractmethod
    async def __call__(self) -> None:
        pass

    def progress(self, title: str) -> Progress:
        return self.exec.progress(title)

    @property
    def data_path(self) -> Path:
        return self.exec.get_data_path()

    def ensure_writable(self) -> None:
        self.exec.ensure_writable()

    def data_files(self, write: bool = False) -> typing.Iterator[Path]:
        if write:
            self.ensure_writable()
        return self.exec.data_files()

    def data_file_progress(self, title: str, write: bool = False) -> typing.Iterator[Path]:
        if write:
            self.ensure_writable()
        with self.progress(title) as progress:
            all_files = list(self.data_files(write=write))
            completed_files: int = 0
            for f in all_files:
                progress(completed_files / len(all_files))
                yield f
                completed_files += 1

    class ReplacementPath:
        def __init__(self, exec: "Execute"):
            self.exec = exec
            self._output: typing.Optional[TemporaryDirectory] = None

        def __enter__(self) -> Path:
            assert self._output is None
            self._output = TemporaryDirectory()
            return Path(self._output.name)

        def __exit__(self, exc_type, exc_val, exc_tb):
            assert self._output is not None
            if exc_type is not None:
                self._output.cleanup()
                self._output = None
                return

            if self.exec._writable_data_path:
                self.exec._writable_data_path.cleanup()
            self.exec._writable_data_path = self._output
            self._output = None

    def data_replacement(self) -> "ExecuteStage.ReplacementPath":
        return self.ReplacementPath(self.exec)

    @property
    def netcdf_executor(self) -> ThreadPoolExecutor:
        return self.exec.netcdf_executor


class Execute:
    def __init__(self):
        self.stages: typing.List[ExecuteStage] = list()
        self.stderr_is_output: bool = False

        self._progress_stack: typing.List[Progress] = list()

        self._override_archive_unix: typing.Optional[str] = None
        self._override_archive_tcp: typing.Optional[typing.Tuple[str, int]] = None

        self._read_only_input_files: typing.List[Path] = list()
        self._read_only_combined_input: typing.Optional[TemporaryDirectory] = None
        self._writable_data_path: typing.Optional[TemporaryDirectory] = None

        self._netcdf_executor: typing.Optional[ThreadPoolExecutor] = None

    @property
    def netcdf_executor(self) -> ThreadPoolExecutor:
        # This exists because NetCDF access is not reentrant:
        #  https://github.com/Unidata/netcdf4-python/issues/844
        #  https://github.com/Unidata/netcdf-c/projects/6
        if self._netcdf_executor is None:
            self._netcdf_executor = ThreadPoolExecutor(max_workers=1)
        return self._netcdf_executor

    def set_archive_unix(self, socket: str) -> None:
        self._override_archive_unix = socket

    def set_archive_tcp(self, server: str, port: typing.Optional[int] = None) -> None:
        if not port:
            from forge.archive import DEFAULT_ARCHIVE_TCP_PORT
            port = DEFAULT_ARCHIVE_TCP_PORT
        self._override_archive_tcp = (server, port)

    def install(self, stage: ExecuteStage) -> None:
        self.stages.append(stage)
        assert stage.idx == len(self.stages) - 1

    async def archive_connection(self) -> Connection:
        if self._override_archive_unix:
            _LOGGER.debug("Opening archive Unix socket '%s'", self._override_archive_unix)
            reader, writer = await asyncio.open_unix_connection(self._override_archive_unix)
            connection = Connection(reader, writer, "data command")
        elif self._override_archive_tcp:
            _LOGGER.debug("Opening archive connection to %s port %d",
                          self._override_archive_tcp[0], self._override_archive_tcp[1])
            reader, writer = await asyncio.open_connection(*self._override_archive_tcp)
            connection = Connection(reader, writer, "data command")
        else:
            _LOGGER.debug("Opening default archive connection")
            connection = await Connection.default_connection("data command")
        return connection

    async def __call__(self) -> None:
        for s in self.stages:
            await s.before()
        for s in self.stages:
            await s()
        for s in self.stages:
            await s.after()

        if self._read_only_combined_input:
            self._read_only_combined_input.cleanup()
            self._read_only_combined_input = None
        if self._writable_data_path:
            self._writable_data_path.cleanup()
            self._writable_data_path = None

    def progress(self, title: str) -> Progress:
        return Progress(self, title)

    def get_data_path(self) -> Path:
        self.ensure_readable()

        if self._writable_data_path:
            return Path(self._writable_data_path.name)
        if self._read_only_combined_input:
            return Path(self._read_only_combined_input.name)

        raise RuntimeError

    def ensure_readable(self) -> None:
        if self._writable_data_path:
            return
        if self._read_only_combined_input:
            return

        if self._read_only_input_files:
            self._read_only_combined_input = TemporaryDirectory()
            for file in self._read_only_input_files:
                if not file.exists() or not file.is_file():
                    _LOGGER.debug("Invalid file '%s' specified", file)
                    continue

                dest = Path(self._read_only_combined_input.name) / file.name
                if dest.exists():
                    fd, temp_name = mkstemp_like(dest)
                    os.close(fd)
                    dest = Path(temp_name)

                dest.symlink_to(file.absolute())

            self._read_only_input_files.clear()
            return

        self._writable_data_path = TemporaryDirectory()

    def ensure_writable(self) -> None:
        self.ensure_readable()
        if self._writable_data_path:
            return

        if self._read_only_combined_input:
            self._writable_data_path = TemporaryDirectory()
            copy_files: typing.List[Path] = [f for f in Path(self._read_only_combined_input.name).iterdir()]
            with self.progress("Copying files") as progress:
                completed_files: int = 0
                for file in copy_files:
                    progress(completed_files / len(copy_files))
                    copyfile(file, Path(self._writable_data_path.name) / file.name)
                    completed_files += 1

                if self._read_only_combined_input:
                    self._read_only_combined_input.cleanup()
                    self._read_only_combined_input = None

            return

    def attach_external_files(self, files: typing.Iterable[Path]) -> None:
        assert not self._writable_data_path
        assert not self._read_only_combined_input
        self._read_only_input_files.extend(files)

    def data_files(self) -> typing.Iterator[Path]:
        self.ensure_readable()
        source = self._writable_data_path or self._read_only_combined_input
        source = Path(source.name)
        return source.iterdir()
