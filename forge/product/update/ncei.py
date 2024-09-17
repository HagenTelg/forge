import typing
import logging
import asyncio
import shutil
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from abc import ABC, abstractmethod
from forge.archive.client.connection import Connection
from forge.product.update.tracker import YearModifiedTracker, CommitFailure
from forge.product.selection import InstrumentSelection
from forge.product.update import CONFIGURATION
from forge.product.update.manager import UpdateController

_LOGGER = logging.getLogger(__name__)


class Destination(ABC):
    @abstractmethod
    async def __call__(self, tracker: "Tracker", files: typing.List[Path]) -> None:
        pass


class SFTP(Destination):
    def __init__(self, user: str = None, host: str = None, keyfile: typing.Optional[str] = None):
        self.user = user
        self.host = host
        self.keyfile = keyfile

    async def __call__(self, tracker: "Tracker", files: typing.List[Path]) -> None:
        import paramiko

        def execute_upload():
            host = self.host or "arrival-external.ncdc.noaa.gov"
            host = CONFIGURATION.get('NCEI.UPDATE.ARCHIVE.SERVER', host)
            user = CONFIGURATION.get('NCEI.UPDATE.ARCHIVE.USER', self.user)
            keyfile = CONFIGURATION.get('NCEI.UPDATE.ARCHIVE.KEY', self.keyfile)

            if keyfile:
                try:
                    loader = paramiko.PKey.from_path
                except AttributeError:
                    loader = paramiko.RSAKey.from_private_key_file
                keyfile = loader(keyfile)
            else:
                keyfile = None

            ssh = paramiko.SSHClient()
            try:
                class IgnoreHostKey(paramiko.MissingHostKeyPolicy):
                    def missing_host_key(self, client, hostname, key):
                        pass

                ssh.set_missing_host_key_policy(IgnoreHostKey())

                ssh.connect(
                    hostname=host,
                    username=user,
                    pkey=keyfile,
                    timeout=120.0,
                )
                sftp = ssh.open_sftp()

                for input_file in files:
                    _LOGGER.info(f"Uploading {input_file.name}")
                    sftp.put(str(input_file), input_file.name)
            finally:
                ssh.close()

        try:
            await asyncio.get_event_loop().run_in_executor(None, execute_upload)
        except Exception as e:
            _LOGGER.warning("Upload failed", exc_info=True)
            raise CommitFailure from e


class Local(Destination):
    def __init__(self, destination: str):
        self.destination = destination

    async def __call__(self, tracker: "Tracker", files: typing.List[Path]) -> None:
        destination = self.destination.replace('{station}', tracker.station.lower())
        destination = Path(destination)

        def copy_files():
            destination.mkdir(parents=True, exist_ok=True)
            for input_file in files:
                shutil.copy(str(input_file), destination / input_file.name)

        try:
            await asyncio.get_event_loop().run_in_executor(None, copy_files)
        except Exception as e:
            _LOGGER.warning("Local copy failed", exc_info=True)
            raise CommitFailure from e


class Tracker(YearModifiedTracker):
    class Output(YearModifiedTracker.Output):
        async def commit(self) -> None:
            with TemporaryDirectory() as working_directory:
                working_directory = Path(working_directory)
                await self.tracker.make_output_files(self.start_epoch_ms, self.end_epoch_ms, working_directory)
                await self.tracker.perform_upload(working_directory)

    def __init__(self, connection: Connection, state_path: Path, station: str, archive: str, ncei_file: str,
                 selections: typing.List[InstrumentSelection], destinations: typing.List[Destination]):
        super().__init__(connection, station, archive, selections)
        self._state_path = state_path
        self.ncei_file = ncei_file
        self.destinations = destinations

    @property
    def update_key(self) -> typing.Optional[str]:
        return self.ncei_file

    async def make_output_files(self, start_epoch_ms: int, end_epoch_ms: int, destination: Path) -> None:
        _LOGGER.debug(f"Looking up {self.ncei_file} for {self.station} in {start_epoch_ms},{end_epoch_ms}")
        from forge.processing.station.lookup import station_data
        try:
            converter = station_data(self.station, 'ncei', 'file')(
                self.station, self.ncei_file, start_epoch_ms, end_epoch_ms
            )
        except FileNotFoundError:
            _LOGGER.warning(f"NCEI file type code '{self.ncei_file}' not found for station and/or time")
            return
        converter = converter(self.station, start_epoch_ms, end_epoch_ms)

        async def get_connection():
            class Wrapper:
                def __init__(self, connection):
                    self.connection = connection

                async def __aenter__(self):
                    return self.connection

                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass

            return Wrapper(self.connection)

        converter.get_archive_connection = get_connection
        await converter(destination)

    async def perform_upload(self, source: Path) -> None:
        upload_files: typing.List[Path] = [f for f in source.iterdir() if f.is_file()]
        if not upload_files:
            _LOGGER.debug("No output files generated")
            return

        def make_all_manifests():
            def make_manifest(data_file: Path) -> Path:
                d = hashlib.md5()
                with data_file.open('rb') as f:
                    while True:
                        data = f.read(4096)
                        if not data:
                            break
                        d.update(data)
                manifest_file = data_file.parent / f"{data_file.name}.mnf"
                with manifest_file.open('w') as f:
                    f.write(f"{data_file.name},{d.hexdigest()},{data_file.stat().st_size}\n")
                return data_file

            manifest_files = [make_manifest(f) for f in upload_files]
            upload_files.extend(manifest_files)

        _LOGGER.debug(f"Creating manifests for {len(upload_files)} files")
        await asyncio.get_event_loop().run_in_executor(None, make_all_manifests)

        _LOGGER.debug(f"Sending {len(upload_files)} total files")
        for d in self.destinations:
            await d(self, upload_files)

    @property
    def state_file(self) -> Path:
        return self._state_path / f"{self.station.upper()}-{self.ncei_file}.json"


class Controller(UpdateController):
    UPDATER_DESCRIPTION = "NCEI archive automatic data submission."
    UPDATER_CONNECTION_NAME = "NCEI archive submit"
    CONTROL_DESCRIPTION = "Control NCEI automatic archive submissions."

    @classmethod
    def updater_control_socket(cls) -> typing.Optional[str]:
        return CONFIGURATION.get("NCEI.UPDATE.ARCHIVE.CONTROL_SOCKET", "/run/forge-ncei-submit.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get('NCEI.UPDATE.ARCHIVE.STATE', '/var/lib/forge/state/ncei'),
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
            for ncei_file, (archive, selections, destinations) in station_data(station, 'ncei', 'submit')(station).items():
                trackers.append(Tracker(self.connection, self.state_path, station, archive, ncei_file, selections, destinations))
        return trackers


def updater():
    Controller.run_updater()


def control():
    Controller.run_control()
