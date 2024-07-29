import typing
import asyncio
import logging
import time
from pathlib import Path
from forge.logicaltime import start_of_year_ms, end_of_year_ms
from forge.archive import CONFIGURATION
from forge.archive.client.connection import Connection
from forge.archive.client import data_lock_key, data_notification_key
from forge.processing.average.update import update_avgm_data
from .manager import StationsController

_LOGGER = logging.getLogger(__name__)


class DataController(StationsController):
    class Manager(StationsController.Manager):
        @property
        def state_file(self) -> Path:
            return self.controller.state_path / f"{self.station.lower()}.json"

        @property
        def listen_keys(self) -> typing.Iterable[str]:
            return [data_notification_key(self.station, "avgd")]

        @property
        def intent_keys(self) -> typing.Iterable[str]:
            return [data_lock_key(self.station, "avgm")]

        async def get_modified(self, modified_after: float) -> typing.List[typing.Tuple[int, int]]:
            return await self.scan_modified_files(
                f"data/{self.station.lower()}/avgd", modified_after,
                self.convert_year_file
            )

        def round_notification(self, key: str, start: int, end: int) -> typing.Tuple[int, int]:
            if start <= 0:
                start = 0
                start_year_number = 1970
            else:
                start_year_number = time.gmtime(start / 1000.0).tm_year
                start = start_of_year_ms(start_year_number)

            current_year_number = time.gmtime().tm_year
            next_year = end_of_year_ms(current_year_number)
            if end >= next_year:
                end = next_year
            else:
                end_year_number = time.gmtime(end / 1000.0).tm_year
                rounded_end = start_of_year_ms(end_year_number)
                if end <= rounded_end:
                    end = rounded_end
                else:
                    rounded_end = start_of_year_ms(end_year_number + 1)
                    assert rounded_end >= end
                    end = rounded_end
            if start >= end:
                end = end_of_year_ms(start_year_number)
            return start, end

        async def perform_update(self, start: int, end: int) -> None:
            await update_avgm_data(self.connection, self.station, start / 1000.0, end / 1000.0)
            await connection.set_transaction_status("Writing monthly averaged data")

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
        return CONFIGURATION.get("ARCHIVE.UPDATE.AVGM.CONTROL_SOCKET", "/run/forge-archive-avgm.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get("ARCHIVE.UPDATE.AVGM.STATE", "/var/lib/forge/state/archive/avgm"),
                            help="set the state file directory")

    UPDATER_DESCRIPTION = "Forge archive monthly averaged data update."
    UPDATER_CONNECTION_NAME = "update monthly data"
    FLUSH_DESCRIPTION = "Complete pending monthly averaged data updates."


def updater():
    DataController.run_updater()


def flush():
    DataController.run_flush()
