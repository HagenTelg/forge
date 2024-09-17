import typing
import logging
import asyncio
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from forge.archive.client.connection import Connection
from forge.product.update.tracker import FileModifiedTracker, CommitFailure
from forge.product.selection import InstrumentSelection
from forge.product.update import CONFIGURATION
from forge.product.update.manager import UpdateController

_LOGGER = logging.getLogger(__name__)


class Tracker(FileModifiedTracker):
    class Output(FileModifiedTracker.Output):
        @property
        def retain_after_commit(self) -> bool:
            return False

        @property
        def merge_contiguous(self) -> bool:
            return True

        async def commit(self) -> None:
            await self.tracker.perform_update(self.start_epoch_ms, self.end_epoch_ms)

    def __init__(self, connection: Connection, state_path: Path, station: str, archive: str, table: str,
                 selections: typing.List[InstrumentSelection]):
        super().__init__(connection, station, archive, selections)
        self._state_path = state_path
        self.table = table

    @property
    def update_key(self) -> typing.Optional[str]:
        return self.table

    def updated_to_outputs(self, start_epoch_ms: int, end_epoch_ms: int) -> typing.Iterable[typing.Tuple[int, int]]:
        yield start_epoch_ms, end_epoch_ms

    async def perform_update(self, start_epoch_ms: int, end_epoch_ms: int) -> None:
        _LOGGER.debug(f"Looking up {self.table} for {self.station} in {start_epoch_ms},{end_epoch_ms}")
        from forge.processing.station.lookup import station_data
        try:
            updater = station_data(self.station, 'sqldb', 'table_update')(
                self.station, self.table, start_epoch_ms, end_epoch_ms
            )
        except FileNotFoundError:
            _LOGGER.warning(f"SQL update table '{self.table}' not found for station and/or time")
            return

        async def get_connection():
            class Wrapper:
                def __init__(self, connection):
                    self.connection = connection

                async def __aenter__(self):
                    return self.connection

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass

            return Wrapper(self.connection)

        updater.get_archive_connection = get_connection
        try:
            await updater()
        except SQLAlchemyError as e:
            _LOGGER.warning("Error during SQL update", exc_info=True)
            raise CommitFailure from e

    @property
    def state_file(self) -> Path:
        return self._state_path / f"{self.station.upper()}-{self.table}.json"


class Controller(UpdateController):
    UPDATER_DESCRIPTION = "SQL database automatic data table updater."
    UPDATER_CONNECTION_NAME = "SQL database update"
    CONTROL_DESCRIPTION = "Control SQL database updates."

    AUTOMATIC_COMMIT = True

    @classmethod
    def updater_control_socket(cls) -> typing.Optional[str]:
        return CONFIGURATION.get("SQLDB.UPDATE.CONTROL_SOCKET", "/run/forge-sqldb-update.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get('SQLDB.UPDATE.STATE', '/var/lib/forge/state/sqldb-update'),
                            help="set the state file directory")

    @classmethod
    def create_updater(cls, connection: Connection, args):
        state_path = Path(args.state_path)
        state_path.mkdir(parents=True, exist_ok=True)
        return cls(connection, state_path)

    def __init__(self, connection: Connection, state_path: Path):
        super().__init__(connection)
        self.state_path = state_path

    async def create_trackers(self) -> typing.List[Tracker]:
        from forge.const import STATIONS
        from forge.processing.station.lookup import station_data

        trackers: typing.List[Tracker] = list()
        for station in STATIONS:
            for table_code, (archive, selections) in station_data(
                    station, 'sqldb', 'updates')(station).items():
                trackers.append(Tracker(self.connection, self.state_path, station, archive, table_code, selections))
        return trackers


def updater():
    Controller.run_updater()


def control():
    Controller.run_control()