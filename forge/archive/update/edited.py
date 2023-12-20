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
from forge.archive.client import data_lock_key, data_notification_key, edit_directives_lock_key, edit_directives_notification_key
from forge.processing.editing.update import update_edited_data
from .manager import StationsController

_LOGGER = logging.getLogger(__name__)


class DataController(StationsController):
    class Manager(StationsController.ManagerPerDay):
        @property
        def state_file(self) -> Path:
            return self.controller.state_path / f"{self.station.lower()}.json"

        @property
        def listen_keys(self) -> typing.Iterable[str]:
            return [
                data_notification_key(self.station, "raw"),
                edit_directives_notification_key(self.station),
            ]

        @property
        def intent_keys(self) -> typing.Iterable[str]:
            return [data_lock_key(self.station, "edited")]

        def get_modified_edits(self, file_name: str, modified_after: float) -> typing.Iterable[typing.Tuple[int, int]]:
            try:
                data = Dataset(file_name, 'r')
            except OSError:
                _LOGGER.debug("Error opening edits file", exc_info=True)
                return []
            try:
                edits = data.groups.get("edits")
                if edits is None:
                    _LOGGER.debug("No edits group available")
                    return []

                start_time = edits.variables.get("start_time")
                end_time = edits.variables.get("end_time")
                modified_time = edits.variables.get("modified_time")
                if start_time is None or end_time is None or modified_time is None:
                    _LOGGER.debug("Invalid edits file structure")
                    return []
                start_time = start_time[:].data
                end_time = end_time[:].data
                modified_time = modified_time[:].data

                if modified_after > 0.0:
                    modified_edits = modified_time > int(floor(modified_after * 1000))
                    start_time = start_time[modified_edits]
                    end_time = end_time[modified_edits]

                result: typing.List[typing.Tuple[int, int]] = list()
                for i in range(len(start_time)):
                    result.append(self.round_notification("", int(start_time[i]), int(end_time[i])))
                return result
            finally:
                data.close()

        async def get_modified(self, modified_after: float) -> typing.List[typing.Tuple[int, int]]:
            modified_ranges: typing.List[typing.Tuple[int, int]] = await self.scan_modified_files(
                f"data/{self.station.lower()}/raw", modified_after,
                self.convert_day_file
            )

            inspect_edit_files = await self.connection.list_files(f"edits/{self.station.lower()}", modified_after)
            if inspect_edit_files:
                _LOGGER.debug(f"Scanning {len(inspect_edit_files)} modified edit files for {self.station.upper()}")
                edits_modified: typing.List[typing.Tuple[int, int]] = list()
                backoff = LockBackoff()
                while True:
                    edits_modified.clear()
                    try:
                        async with self.controller.connection.transaction():
                            await self.connection.lock_read(edit_directives_lock_key(self.station), -MAX_I64, MAX_I64)
                            for archive_path in inspect_edit_files:
                                with NamedTemporaryFile(suffix=".nc") as f:
                                    try:
                                        await self.connection.read_file(archive_path, f)
                                    except FileNotFoundError:
                                        continue
                                    f.flush()
                                    edits_modified.extend(self.get_modified_edits(f.name, modified_after))
                        break
                    except LockDenied as ld:
                        _LOGGER.debug("Initial edits read busy: %s", ld.status)
                        await backoff()
                modified_ranges.extend(edits_modified)

            return modified_ranges

        async def perform_update(self, start: int, end: int) -> None:
            await update_edited_data(self.connection, self.station, start / 1000.0, end / 1000.0)

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
        return CONFIGURATION.get("ARCHIVE.UPDATE.EDITED.CONTROL_SOCKET", "/run/forge-archive-edited.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get("ARCHIVE.UPDATE.EDITED.STATE", "/var/lib/forge/state/archive/edited"),
                            help="set the state file directory")

    UPDATER_DESCRIPTION = "Forge archive edited data update."
    UPDATER_CONNECTION_NAME = "update edited data"
    FLUSH_DESCRIPTION = "Complete pending edited data updates."


def updater():
    DataController.run_updater()


def flush():
    DataController.run_flush()
