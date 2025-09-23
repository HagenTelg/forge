import typing
import logging
import asyncio
from pathlib import Path
from forge.temp import WorkingDirectory
from forge.archive.client.connection import Connection
from forge.product.update.tracker import NRTTracker
from forge.product.selection import InstrumentSelection
from forge.product.update import CONFIGURATION
from forge.product.update.manager import UpdateController

_LOGGER = logging.getLogger(__name__)


class Tracker(NRTTracker):
    class Output(NRTTracker.Output):
        async def commit(self) -> None:
            async with WorkingDirectory() as working_directory:
                working_directory = Path(working_directory)
                await self.tracker.make_output_files(self.start_epoch_ms, self.end_epoch_ms, working_directory)
                await self.tracker.perform_upload(working_directory)

    def __init__(self, connection: Connection, state_path: Path, station: str, archive: str, ebas_file: str,
                 selections: typing.List[InstrumentSelection], upload_user: str, upload_directory: str):
        super().__init__(connection, station, archive, selections)
        self._state_path = state_path
        self.ebas_file = ebas_file
        self.upload_user = upload_user
        self.upload_directory = upload_directory

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
            _LOGGER.warning(f"EBAS file type code '{self.ebas_file}' not found for station and/or time")
            return
        class NRTConverter(converter):
            @property
            def tags(self) -> typing.Optional[typing.Set[str]]:
                tags = set(super().tags)
                tags.add("nrt")
                return tags

        converter = NRTConverter(self.station, start_epoch_ms, end_epoch_ms)

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

        def execute_upload():
            ssh = paramiko.SSHClient()
            try:
                class IgnoreHostKey(paramiko.MissingHostKeyPolicy):
                    def missing_host_key(self, client, hostname, key):
                        pass
                ssh.set_missing_host_key_policy(IgnoreHostKey())

                ssh.connect(
                    hostname=CONFIGURATION.get('EBAS.UPDATE.NRT.SERVER', "upload.nilu.no"),
                    username=CONFIGURATION.get('EBAS.UPDATE.NRT.USER', self.upload_user),
                    key_filename=CONFIGURATION.get('EBAS.UPDATE.NRT.KEY'),
                    timeout=120.0,
                )
                sftp = ssh.open_sftp()
                remote_dir = CONFIGURATION.get('EBAS.UPDATE.NRT.DIRECTORY', self.upload_directory)
                if remote_dir:
                    sftp.chdir(remote_dir)
                for input_file in Path(source).iterdir():
                    if not input_file.is_file():
                        continue
                    _LOGGER.info(f"Uploading {input_file.name}")
                    sftp.put(str(input_file), input_file.name)
            finally:
                ssh.close()

        try:
            await asyncio.get_event_loop().run_in_executor(None, execute_upload)
        except:
            _LOGGER.warning(f"Upload failed for {self.station.upper()}/{self.ebas_file}", exc_info=True)
            # Just ignore NRT upload failures

    @property
    def state_file(self) -> Path:
        return self._state_path / f"{self.station.upper()}-{self.ebas_file}.json"


class Controller(UpdateController):
    UPDATER_DESCRIPTION = "EBAS NRT automatic data send."
    UPDATER_CONNECTION_NAME = "EBAS NRT send"
    CONTROL_DESCRIPTION = "Control EBAS NRT send."

    AUTOMATIC_COMMIT = True

    @classmethod
    def updater_control_socket(cls) -> typing.Optional[str]:
        return CONFIGURATION.get("EBAS.UPDATE.NRT.CONTROL_SOCKET", "/run/forge-ebas-nrt.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get('EBAS.UPDATE.NRT.STATE', '/var/lib/forge/state/ebas-nrt'),
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
        exclude_stations: typing.Set[str] = {s.lower() for s in CONFIGURATION.get('EBAS.UPDATE.NRT.EXCLUDE_STATIONS', ())}
        for station in STATIONS:
            if station in exclude_stations:
                continue
            for ebas_file, (archive, selections, user, directory) in station_data(
                    station, 'ebas', 'nrt')(station).items():
                trackers.append(Tracker(self.connection, self.state_path,
                                        station, archive, ebas_file, selections, user, directory))
        return trackers


def updater():
    Controller.run_updater()


def control():
    Controller.run_control()