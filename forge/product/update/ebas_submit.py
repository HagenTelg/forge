import typing
import logging
import asyncio
import shutil
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from forge.archive.client.connection import Connection
from forge.product.update.tracker import YearModifiedTracker, CommitFailure
from forge.product.selection import InstrumentSelection
from forge.product.update import CONFIGURATION
from forge.product.update.manager import UpdateController

_LOGGER = logging.getLogger(__name__)


class Tracker(YearModifiedTracker):
    _FILE_MATCH = re.compile(r"^[^.]+\.(\d{4})\d{10}\.\d{14}\..+\.(lev[0-9][a-i]?)\.")

    class Output(YearModifiedTracker.Output):
        async def commit(self) -> bool:
            with TemporaryDirectory() as working_directory:
                working_directory = Path(working_directory)
                await self.tracker.make_output_files(self.start_epoch_ms, self.end_epoch_ms, working_directory)
                await self.tracker.perform_upload(working_directory)
            return True

    def __init__(self, connection: Connection, state_path: Path, station: str, archive: str, ebas_file: str,
                 selections: typing.List[InstrumentSelection]):
        super().__init__(connection, station, archive, selections)
        self._state_path = state_path
        self.ebas_file = ebas_file

    @property
    def update_key(self) -> typing.Optional[str]:
        return self.ebas_file

    async def make_output_files(self, start_epoch_ms: int, end_epoch_ms: int, destination: Path) -> None:
        _LOGGER.debug(f"Looking up {self.ebas_file} for {self.station} in {start_epoch_ms},{end_epoch_ms}")
        from forge.processing.station.lookup import station_data
        try:
            converter = station_data(self.station, 'ebas', 'file')(
                self.station, self.ebas_file, start_epoch_ms, end_epoch_ms
            )
        except FileNotFoundError:
            _LOGGER.warning(f"EBAS file type code '{self.ebas_file}' no found for station and/or time")
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
        import paramiko

        any_files = False
        for input_file in source.iterdir():
            if not input_file.is_file():
                continue
            any_files = True

            _LOGGER.debug(f"Compressing {input_file.name}")
            process = await asyncio.create_subprocess_exec("gzip", str(input_file),
                                                           stdout=asyncio.subprocess.DEVNULL,
                                                           stdin=asyncio.subprocess.DEVNULL)
            await process.wait()
            if process.returncode != 0:
                _LOGGER.warning(f"Error on gzip file {input_file}")
                continue

        if not any_files:
            return

        def execute_upload():
            ssh = paramiko.SSHClient()

            try:
                class IgnoreHostKey(paramiko.MissingHostKeyPolicy):
                    def missing_host_key(self, client, hostname, key):
                        pass
                ssh.set_missing_host_key_policy(IgnoreHostKey())

                ssh.connect(
                    hostname=CONFIGURATION.get('EBAS.UPDATE.ARCHIVE.SERVER', "ebas-submissions.nilu.no"),
                    username=CONFIGURATION.get('EBAS.UPDATE.ARCHIVE.USER', "ebasftp"),
                    timeout=120.0,
                )
                sftp = ssh.open_sftp()
                remote_dir = CONFIGURATION.get('EBAS.UPDATE.ARCHIVE.DIRECTORY')
                if remote_dir:
                    sftp.chdir(remote_dir)
                for input_file in Path(source).iterdir():
                    if not input_file.is_file():
                        continue

                    _LOGGER.info(f"Uploading {input_file.name}")
                    sftp.put(str(input_file), input_file.name)

                    completed_directory = CONFIGURATION.get('EBAS.UPDATE.ARCHIVE.COMPLETED')
                    if completed_directory:
                        completed_directory = completed_directory.replace('{station}', self.station.lower())
                        file_match = self._FILE_MATCH.match(input_file.name)
                        if file_match:
                            completed_directory = completed_directory.replace('{year}', file_match.group(1))
                            completed_directory = completed_directory.replace('{level}', file_match.group(2))
                        else:
                            completed_directory = completed_directory.replace('{year}', "UNKNOWN")
                            completed_directory = completed_directory.replace('{level}', "UNKNOWN")
                        completed_directory = Path(completed_directory)
                        try:
                            completed_directory.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(input_file), completed_directory / input_file.name)
                            _LOGGER.debug(f"Retained {input_file.name} to {completed_directory}")
                        except:
                            _LOGGER.warning(f"Error retaining {input_file.name} to {completed_directory}", exc_info=True)
            finally:
                ssh.close()

        try:
            await asyncio.get_event_loop().run_in_executor(None, execute_upload)
        except Exception as e:
            _LOGGER.warning("Upload failed", exc_info=True)
            raise CommitFailure from e

    @property
    def state_file(self) -> Path:
        return self._state_path / f"{self.station.upper()}-{self.ebas_file}.json"


class Controller(UpdateController):
    UPDATER_DESCRIPTION = "EBAS archive automatic data submission."
    UPDATER_CONNECTION_NAME = "EBAS archive submit"
    CONTROL_DESCRIPTION = "Control EBAS automatic archive submissions."

    @classmethod
    def updater_control_socket(cls) -> typing.Optional[str]:
        return CONFIGURATION.get("EBAS.UPDATE.ARCHIVE.CONTROL_SOCKET", "/run/forge-ebas-submit.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get('EBAS.UPDATE.ARCHIVE.STATE', '/var/lib/forge/state/ebas'),
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
            for ebas_file, (archive, selections) in station_data(station, 'ebas', 'submit')(station).items():
                trackers.append(Tracker(self.connection, self.state_path, station, archive, ebas_file, selections))
        return trackers


def updater():
    Controller.run_updater()


def control():
    Controller.run_control()
