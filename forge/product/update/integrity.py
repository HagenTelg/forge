import typing
import logging
import asyncio
import time
import datetime
import re
from pathlib import Path
from math import floor, ceil
from forge.const import STATIONS
from forge.logicaltime import containing_year_range, start_of_year_ms, year_bounds_ms
from forge.formattime import format_iso8601_time
from forge.range import intersects
from forge.archive.client.connection import Connection
from forge.product.update.tracker import Tracker, CommitFailure
from forge.product.update import CONFIGURATION
from forge.product.update.manager import UpdateController
from forge.archive.client import data_notification_key
from forge.product.integrity.archive import calculate_archive_integrity, apply_output_pattern

_LOGGER = logging.getLogger(__name__)


class IntegrityTracker(Tracker):
    _DAY_FILE_MATCH = re.compile(
        r'[A-Z][0-9A-Z_]{0,31}-[A-Z][A-Z0-9]*_'
        r's(\d{4})(\d{2})(\d{2})\.nc',
    )
    _YEAR_FILE_MATCH = re.compile(
        r'[A-Z][0-9A-Z_]{0,31}-[A-Z][A-Z0-9]*_'
        r's(\d{4})0101\.nc',
    )

    class Output(Tracker.Output):
        @property
        def retain_after_commit(self) -> bool:
            return False

        @property
        def merge_contiguous(self) -> bool:
            return True

        async def commit(self) -> None:
            await self.tracker.perform_update(self.start_epoch_ms, self.end_epoch_ms)

    def __init__(self, connection: Connection, state_path: Path, station: str, archive: str,
                 output_file: str, completion_command: typing.Optional[str]):
        super().__init__()
        self.connection = connection
        self._state_path = state_path
        self.station = station
        self.archive = archive
        self.output_file = output_file
        self.completion_command = completion_command
        self._scan_begin: typing.Optional[float] = None

    def matches_selection(
            self,
            station: typing.Optional[str] = None,
            archive: typing.Optional[str] = None,
            key: typing.Optional[re.Pattern] = None,
    ) -> bool:
        if station and self.station != station:
            return False
        if archive and self.archive != archive:
            return False
        return True

    @property
    def listen_notifications(self) -> typing.Iterable[str]:
        return [
            data_notification_key(self.station, self.archive),
        ]

    def _file_to_bounds(self, archive_file: str) -> typing.Optional[typing.Tuple[int, int]]:
        file = Path(archive_file).name
        if self.archive in ("avgd", "avgm"):
            match = self._YEAR_FILE_MATCH.fullmatch(file)
            if not match:
                return None
            return year_bounds_ms(int(match.group(1)))
        else:
            match = self._DAY_FILE_MATCH.fullmatch(file)
            if not match:
                return None
            file_start_ms = int(floor(datetime.datetime(
                int(match.group(1)), int(match.group(2)), int(match.group(3)),
                tzinfo=datetime.timezone.utc
            ).timestamp() * 1000.0))
            file_end_ms = file_start_ms + 24 * 60 * 60 * 1000
            return file_start_ms, file_end_ms

    def load_state(self) -> bool:
        if not super().load_state():
            return False
        self._scan_begin = self._candidate_scan_epoch_ms / 1000.0
        return True

    async def initial_scan(self) -> None:
        _LOGGER.debug(f"Scanning candidates for {self.station.upper()}/{self.archive.upper()}")
        for file in await self.connection.list_files(
                f"data/{self.station.lower()}/{self.archive.lower()}", self._scan_begin or 0
        ):
            bounds = self._file_to_bounds(file)
            if not bounds:
                continue
            self.notify_candidate(*bounds, save_state=False)

    def __str__(self) -> str:
        return f"{self.station.upper()}/{self.archive.upper()}"

    def round_candidate(self, start_epoch_ms: int, end_epoch_ms: int) -> typing.Tuple[int, int]:
        if self.archive in ("avgd", "avgm"):
            year_start, year_end = containing_year_range(start_epoch_ms / 1000.0, end_epoch_ms / 1000.0)
            return start_of_year_ms(year_start), start_of_year_ms(year_end)
        else:
            day_start = int(floor(start_epoch_ms / (24 * 60 * 60 * 1000)))
            day_end = int(ceil(end_epoch_ms / (24 * 60 * 60 * 1000)))
            day_end = max(day_end, day_start+1)
            return day_start * 24 * 60 * 60 * 1000, day_end * 24 * 60 * 60 * 1000

    async def candidate_to_updated(self, start_epoch_ms: int, end_epoch_ms: int,
                                   _modified_after_epoch_ms: int) -> "typing.AsyncIterable[typing.Tuple[int, int]]":
        yield start_epoch_ms, end_epoch_ms

    def updated_to_outputs(self, start_epoch_ms: int, end_epoch_ms: int) -> typing.Iterable[typing.Tuple[int, int]]:
        yield start_epoch_ms, end_epoch_ms

    async def perform_update(self, start_epoch_ms: int, end_epoch_ms: int) -> None:
        begin_time = time.monotonic()
        completed_files = 0

        updated_files: typing.Set[str] = set()
        current_file_name: typing.Optional[str] = None
        current_file_data: typing.Dict[str, str] = dict()

        def flush_current_file() -> None:
            if not current_file_name:
                return

            target_file = Path(current_file_name)
            target_file.parent.mkdir(parents=True, exist_ok=True)
            with target_file.open("wt") as f:
                for key in sorted(current_file_data.keys()):
                    f.write(f"{key},{current_file_data[key]}\n")
            _LOGGER.debug(f"Wrote {len(current_file_data)} integrity entries to {target_file}")

            updated_files.add(current_file_name)

        def switch_to_file(name: str) -> None:
            nonlocal current_file_name
            nonlocal current_file_data

            if current_file_name == name:
                return
            flush_current_file()
            current_file_data.clear()
            current_file_name = None

            # Don't remove again if we reload a file
            do_exclude = name not in updated_files
            try:
                with open(name, "rt") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        key, value = line.split(',', 1)
                        if do_exclude:
                            bounds = self._file_to_bounds(key)
                            if bounds and intersects(start_epoch_ms, end_epoch_ms, *bounds):
                                continue
                        current_file_data[key.strip()] = value
            except FileNotFoundError:
                pass
            except:
                _LOGGER.warning(f"Error parsing file {name}")
                raise

            current_file_name = name

        try:
            async for archive_file_name, file_creation_time, hash_time, file_hash, data_hash, qualitative_hash in calculate_archive_integrity(
                    self.connection, self.station, self.archive, set(), start_epoch_ms, end_epoch_ms,
            ):
                completed_files += 1

                target = apply_output_pattern(self.output_file, self.station, self.archive,
                                              archive_file_name, hash_time, file_creation_time)
                switch_to_file(target)
                current_file_data[archive_file_name] = f"{format_iso8601_time(file_creation_time) if file_creation_time else ''},{format_iso8601_time(hash_time)},{file_hash.hex()},{data_hash.hex()},{qualitative_hash.hex()}"

            flush_current_file()
        except Exception as e:
            _LOGGER.warning(f"Error during integrity calculation for {self.station.upper()}/{self.archive.upper()} in {start_epoch_ms},{end_epoch_ms}", exc_info=True)
            raise CommitFailure from e

        if updated_files and self.completion_command:
            if "{file}" in self.completion_command:
                for file_name in sorted(updated_files):
                    command = self.completion_command.replace("{file}", file_name)
                    try:
                        p = await asyncio.create_subprocess_shell(
                            command,
                            stdin=asyncio.subprocess.DEVNULL,
                        )
                        await p.wait()
                    except FileNotFoundError as e:
                        _LOGGER.warning(f"Completion command '{command}' not found", exc_info=True)
                        raise CommitFailure from e
                    if p.returncode != 0:
                        raise CommitFailure(f"Completion command for {file_name} failed with return code {p.returncode}")
            else:
                try:
                    p = await asyncio.create_subprocess_shell(
                        self.completion_command,
                        stdin=asyncio.subprocess.DEVNULL,
                    )
                    await p.wait()
                except FileNotFoundError as e:
                    _LOGGER.warning(f"Completion command '{self.completion_command}' not found", exc_info=True)
                    raise CommitFailure from e
                if p.returncode != 0:
                    raise CommitFailure(f"Completion command failed with return code {p.returncode}")

        _LOGGER.debug(f"Integrity calculation for {self.station.upper()}/{self.archive.upper()} in {start_epoch_ms},{end_epoch_ms} on {completed_files} files completed after {time.monotonic() - begin_time:.2f} seconds")

    @property
    def state_file(self) -> Path:
        return self._state_path / f"{self.station.upper()}-{self.archive.upper()}.json"


class Controller(UpdateController):
    UPDATER_DESCRIPTION = "Archive integrity file updater."
    UPDATER_CONNECTION_NAME = "archive integrity update"
    CONTROL_DESCRIPTION = "Control archive integrity updates."

    CANDIDATE_PROCESS_DELAY = 0

    @classmethod
    def updater_control_socket(cls) -> typing.Optional[str]:
        return CONFIGURATION.get("INTEGRITY.UPDATE.CONTROL_SOCKET", "/run/forge-integrity-update.sock")

    @classmethod
    def add_updater_arguments(cls, parser) -> None:
        parser.add_argument('--state-path',
                            dest='state_path',
                            default=CONFIGURATION.get('INTEGRITY.UPDATE.STATE', '/var/lib/forge/state/integrity-update'),
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
        archives: typing.Set[str] = set()
        for a in CONFIGURATION.get("INTEGRITY.UPDATE.ARCHIVES", ["raw", "clean"]):
            a = a.lower()
            if a not in ("raw", "edited", "clean", "avgh", "avgd", "avgm"):
                raise ValueError(f"Invalid archive: {a}")
            archives.add(a)
        stations: typing.Set[str] = set()
        for s in CONFIGURATION.get("INTEGRITY.UPDATE.STATIONS", STATIONS):
            s = s.lower()
            if s not in STATIONS:
                raise ValueError(f"Invalid station: {s}")
            stations.add(s)

        output = str(CONFIGURATION["INTEGRITY.UPDATE.OUTPUT"])
        command = CONFIGURATION.get("INTEGRITY.UPDATE.COMPLETION_COMMAND")

        trackers: typing.List[Tracker] = list()
        for station in stations:
            for archive in archives:
                trackers.append(IntegrityTracker(self.connection, self.state_path, station, archive, output, command))
        return trackers


def updater():
    Controller.run_updater()


def control():
    Controller.run_control()