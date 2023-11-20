import typing
import asyncio
import logging
from netCDF4 import Dataset
from math import floor
from tempfile import NamedTemporaryFile
from pathlib import Path
from forge.const import MAX_I64
from forge.archive import CONFIGURATION
from forge.archive.client.connection import Connection, LockDenied, LockBackoff
from forge.archive.client import passed_notification_key, passed_lock_key, data_lock_key
from forge.processing.clean.update import update_clean_data
from .manager import StationsController

_LOGGER = logging.getLogger(__name__)


class DataController(StationsController):
    class Manager(StationsController.ManagerPerDay):
        @property
        def state_file(self) -> Path:
            return self.controller.state_path / f"{self.station.lower()}.json"

        @property
        def listen_keys(self) -> typing.Iterable[str]:
            return [passed_notification_key(self.station)]

        @property
        def intent_keys(self) -> typing.Iterable[str]:
            return [data_lock_key(self.station, "clean")]

        def get_modified_passed(self, file_name: str, modified_after: float) -> typing.Iterable[typing.Tuple[int, int]]:
            try:
                data = Dataset(file_name, 'r')
            except OSError:
                _LOGGER.debug("Error opening passed file", exc_info=True)
                return []
            try:
                edits = data.groups.get("passed")
                if edits is None:
                    _LOGGER.debug("No passed group available")
                    return []

                start_time = edits.variables.get("start_time")
                end_time = edits.variables.get("end_time")
                pass_time = edits.variables.get("pass_time")
                if start_time is None or end_time is None or pass_time is None:
                    _LOGGER.debug("Invalid passed file structure")
                    return []

                if modified_after > 0.0:
                    modified_passed = pass_time > int(floor(modified_after * 1000))
                    start_time = start_time[modified_passed]
                    end_time = end_time[modified_passed]

                result: typing.List[typing.Tuple[int, int]] = list()
                for i in range(len(start_time)):
                    result.append(self.round_notification("", int(start_time[i]), int(end_time[i])))
                return result
            finally:
                data.close()

        async def get_modified(self, modified_after: float) -> typing.List[typing.Tuple[int, int]]:
            modified_ranges: typing.List[typing.Tuple[int, int]] = list()

            inspect_passed_files = await self.connection.list_files(f"passed/{self.station.lower()}", modified_after)
            if inspect_passed_files:
                _LOGGER.debug(f"Scanning {len(inspect_passed_files)} modified passed files for {self.station.upper()}")
                passed_modified: typing.List[typing.Tuple[int, int]] = list()
                backoff = LockBackoff()
                while True:
                    passed_modified.clear()
                    try:
                        async with self.controller.connection.transaction():
                            await self.connection.lock_read(passed_lock_key(self.station), -MAX_I64, MAX_I64)
                            for archive_path in inspect_passed_files:
                                with NamedTemporaryFile(suffix=".nc") as f:
                                    try:
                                        await self.connection.read_file(archive_path, f)
                                    except FileNotFoundError:
                                        continue
                                    f.flush()
                                    passed_modified.extend(self.get_modified_edits(f.name, modified_after))
                        break
                    except LockDenied as ld:
                        _LOGGER.debug("Initial passed read busy: %s", ld.status)
                        await backoff()
                modified_ranges.extend(passed_modified)

            return modified_ranges

        async def perform_update(self, start: int, end: int) -> None:
            await update_clean_data(self.connection, self.station, start / 1000.0, end / 1000.0)

    def __init__(self, connection: Connection, state_path: Path):
        super().__init__(connection)
        self.state_path = state_path

    @classmethod
    def create_updater(cls, connection: Connection, args):
        state_path = Path(args.state_path)
        state_path.mkdir(parents=True, exist_ok=True)
        return cls(connection, state_path)

    @classmethod
    def updater_control_socket(cls) -> typing.Optional[str]:
        return CONFIGURATION.get("ARCHIVE.UPDATE.CLEAN.CONTROL_SOCKET", "/run/forge-archive-clean.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get("ARCHIVE.UPDATE.CLEAN.STATE", "/var/lib/forge/state/archive/clean"),
                            help="set the state file directory")

    UPDATER_DESCRIPTION = "Forge archive clean data update."
    UPDATER_CONNECTION_NAME = "update clean data"
    FLUSH_DESCRIPTION = "Complete pending clean data updates."


def updater():
    DataController.run_updater()


def flush():
    DataController.run_flush()
