import typing
import asyncio
import logging
from pathlib import Path
from forge.archive import CONFIGURATION
from forge.archive.client.connection import Connection
from forge.archive.client import data_lock_key, data_notification_key
from forge.processing.average.update import update_avgd_data
from .manager import StationsController

_LOGGER = logging.getLogger(__name__)


class DataController(StationsController):
    class Manager(StationsController.ManagerPerDay):
        @property
        def state_file(self) -> Path:
            return self.controller.state_path / f"{self.station.lower()}.json"

        @property
        def listen_keys(self) -> typing.Iterable[str]:
            return [data_notification_key(self.station, "avgh")]

        @property
        def intent_keys(self) -> typing.Iterable[str]:
            return [data_lock_key(self.station, "avgd")]

        async def get_modified(self, modified_after: float) -> typing.List[typing.Tuple[int, int]]:
            return await self.scan_modified_files(
                f"data/{self.station.lower()}/avgh", modified_after,
                self.convert_day_file
            )

        async def perform_update(self, start: int, end: int) -> None:
            await update_avgd_data(self.connection, self.station, start / 1000.0, end / 1000.0)

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
        return CONFIGURATION.get("ARCHIVE.UPDATE.AVGD.CONTROL_SOCKET", "/run/forge-archive-avgd.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get("ARCHIVE.UPDATE.AVGD.STATE", "/var/lib/forge/state/archive/avgd"),
                            help="set the state file directory")

    UPDATER_DESCRIPTION = "Forge archive daily averaged data update."
    UPDATER_CONNECTION_NAME = "update daily data"
    FLUSH_DESCRIPTION = "Complete pending daily averaged data updates."


def updater():
    DataController.run_updater()


def flush():
    DataController.run_flush()
