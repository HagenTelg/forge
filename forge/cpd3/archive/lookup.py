import typing
import asyncio
import logging
import time
from math import floor, ceil
from tempfile import NamedTemporaryFile
from netCDF4 import Dataset
from forge.logicaltime import containing_year_range, start_of_year
from forge.temp import WorkingDirectory
from forge.archive.client import index_lock_key, index_file_name, data_lock_key, data_file_name
from forge.archive.client.connection import Connection
from forge.archive.client.archiveindex import ArchiveIndex
from .selection import Selection, FileMatch

_LOGGER = logging.getLogger(__name__)


class IndexLookup:
    def __init__(self, sources: typing.Dict[str, typing.Dict[str, typing.List[Selection]]]):
        self.sources = sources
        self._inf_end = time.time()
        self._year_start: int = 0
        self._year: typing.List[typing.Dict[str, typing.Dict[str, typing.Set[str]]]] = list()

    async def integrate_index(self, connection: Connection) -> None:
        for station, archive_selection in self.sources.items():
            for archive, selections in archive_selection.items():
                start = min([s.start if s.start else 1 for s in selections])
                end = max([s.end if s.end else self._inf_end for s in selections])

                await connection.lock_read(
                    index_lock_key(station, archive),
                    int(floor(start * 1000)), int(ceil(end * 1000))
                )

                for year in range(*containing_year_range(start, end)):
                    try:
                        index_contents = await connection.read_bytes(index_file_name(station, archive, start_of_year(year)))
                    except FileNotFoundError:
                        _LOGGER.debug("No index for %s/%s/%d", station, archive, year)
                        continue
                    index = ArchiveIndex(index_contents)
                    matched_instruments: typing.Set[str] = set()
                    for sel in selections:
                        matched_instruments.update(sel.match_index(index))
                    if not matched_instruments:
                        continue

                    if not self._year_start:
                        self._year_start = year
                        self._year.append(dict())
                    elif self._year_start > year:
                        add_years = self._year_start - year
                        self._year = [dict() for _ in range(add_years)] + self._year
                        self._year_start = year

                    year_index = year - self._year_start
                    while year_index >= len(self._year):
                        self._year.append(dict())
                    dest_year = self._year[year_index]
                    station_to_archive = dest_year.get(station)
                    if not station_to_archive:
                        station_to_archive = dict()
                        dest_year[station] = station_to_archive
                    archive_to_instrument = station_to_archive.get(archive)
                    if not archive_to_instrument:
                        archive_to_instrument = set()
                        station_to_archive[archive] = archive_to_instrument
                    archive_to_instrument.update(matched_instruments)

    async def acquire_locks(self, connection: Connection) -> None:
        for station, archive_selection in self.sources.items():
            for archive, selections in archive_selection.items():
                start_year: typing.Optional[int] = None
                end_year: typing.Optional[int] = None
                for year in range(len(self._year)):
                    year_data = self._year[year]
                    year += self._year_start

                    check_station = year_data.get(station)
                    if not check_station:
                        continue
                    check_archive = year_data.get(archive)
                    if not check_archive:
                        continue
                    if not start_year:
                        start_year = year
                    end_year = year
                if not start_year:
                    continue

                start = min([s.start if s.start else 1 for s in selections])
                end = min([s.end if s.end else self._inf_end for s in selections])
                start = max(start, start_of_year(start_year))
                end = min(end, start_of_year(end_year+1))

                await connection.lock_read(
                    data_lock_key(station, archive),
                    int(floor(start * 1000)), int(ceil(end * 1000))
                )

    async def files(self, connection: Connection):
        async with WorkingDirectory() as data_directory:
            for year_index in range(len(self._year)):
                year = year_index + self._year_start
                year_station = self._year[year_index]

                for day_start in range(start_of_year(year), start_of_year(year+1), 24 * 60 * 60):
                    day_end = day_start + 24 * 60 * 60

                    fetch_files: typing.List[NamedTemporaryFile] = list()
                    open_files: typing.List[Dataset] = list()
                    try:
                        day_contents: typing.List[typing.Tuple[Dataset, typing.List[FileMatch], str, str]] = list()
                        for station, archive_instrument in year_station.items():
                            for archive, instruments in archive_instrument.items():
                                possible_selections = self.sources[station][archive]
                                file_matches: typing.List[FileMatch] = list()
                                for check in possible_selections:
                                    if check.start and check.start >= day_end:
                                        continue
                                    if check.end and check.end <= day_start:
                                        continue
                                    file_matches.append(FileMatch(check, station, archive))
                                if not file_matches:
                                    continue

                                for inst in instruments:
                                    dest_file = NamedTemporaryFile(suffix=".nc", dir=data_directory)
                                    fetch_files.append(dest_file)
                                    try:
                                        await connection.read_file(data_file_name(station, archive, inst, day_start), dest_file)
                                    except FileNotFoundError:
                                        dest_file.close()
                                        fetch_files.pop()
                                        continue
                                    dest_file.flush()
                                    data = Dataset(dest_file.name, 'r')
                                    open_files.append(data)
                                    day_contents.append((data, file_matches, station, archive))
                        yield day_contents
                    finally:
                        for f in open_files:
                            f.close()
                        open_files.clear()
                        for f in fetch_files:
                            f.close()
