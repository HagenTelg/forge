import typing
import asyncio
import time
from pathlib import Path
from json import loads as from_json
from forge.logicaltime import start_of_year
from .connection import Connection
from .put import Index
from . import data_lock_key, index_lock_key, data_file_name, index_file_name


async def read_file_or_nothing(connection: Connection, archive_path: str, destination_dir: Path) -> None:
    output_file = destination_dir / Path(archive_path).name
    with output_file.open("wb") as f:
        try:
            await connection.read_file(archive_path, f)
            return
        except FileNotFoundError:
            pass
    try:
        output_file.unlink()
    except (OSError, FileNotFoundError):
        pass


async def get_all_daily_files(connection: Connection, station: str, archive: str, start: int, end: int,
                              destination: Path) -> None:
    assert start % (24 * 60 * 60) == 0
    assert end % (24 * 60 * 60) == 0
    await connection.lock_read(index_lock_key(station, archive), start * 1000, end * 1000)
    await connection.lock_read(data_lock_key(station, archive), start * 1000, end * 1000)

    current_year: typing.Optional[int] = None
    instrument_ids: typing.Set[str] = set()
    for fetch_start in range(start, end, 24 * 60 * 60):
        ts = time.gmtime(start)
        if ts.tm_year != current_year:
            current_year = ts.tm_year
            year_start = start_of_year(current_year)

            year_index = Index()
            try:
                index_contents = await connection.read_bytes(index_file_name(station, archive, year_start))
                year_index.integrate_existing(index_contents)
            except FileNotFoundError:
                pass
            instrument_ids = set(year_index.known_instrument_ids)

        for code in instrument_ids:
            await read_file_or_nothing(connection, data_file_name(station, archive, code, fetch_start), destination)


async def get_all_yearly_files(connection: Connection, station: str, archive: str, start: int, end: int,
                               destination: Path) -> None:
    await connection.lock_read(index_lock_key(station, archive), start * 1000, end * 1000)
    await connection.lock_read(data_lock_key(station, archive), start * 1000, end * 1000)

    ts = time.gmtime(start)
    current_year = ts.tm_year

    while True:
        year_start = start_of_year(current_year)
        if year_start >= end:
            break

        year_index = Index()
        try:
            index_contents = await connection.read_bytes(index_file_name(station, archive, year_start))
            year_index.integrate_existing(index_contents)
        except FileNotFoundError:
            pass

        for code in year_index.known_instrument_ids:
            await read_file_or_nothing(connection, data_file_name(station, archive, code, year_start), destination)

        current_year += 1


class ArchiveIndex:
    INDEX_VERSION = 1
    _EMPTY_SET = frozenset({})

    def __init__(self, json_data: bytes):
        self.tags: typing.Dict[str, typing.Set[str]] = dict()
        self.instrument_codes: typing.Dict[str, typing.Set[str]] = dict()
        self.standard_names: typing.Dict[str, typing.Set[str]] = dict()
        self.variable_ids: typing.Dict[str, typing.Dict[str, int]] = dict()
        self.variable_names: typing.Dict[str, typing.Set[str]] = dict()

        contents = from_json(json_data)
        try:
            version = contents['version']
        except KeyError:
            raise RuntimeError("No index version available")
        if version != self.INDEX_VERSION:
            raise RuntimeError(f"Index version mismatch ({version} vs {self.INDEX_VERSION})")

        self.tags: typing.Dict[str, typing.Set[str]] = {
            k: set(v) for k, v in contents['instrument_tags'].items()
        }
        self.instrument_codes: typing.Dict[str, typing.Set[str]] = {
            k: set(v) for k, v in contents['instrument_codes'].items()
        }
        self.standard_names: typing.Dict[str, typing.Set[str]] = {
            k: set(v) for k, v in contents['standard_names'].items()
        }
        self.variable_names: typing.Dict[str, typing.Set[str]] = {
            k: set(v) for k, v in contents['variable_names'].items()
        }
        self.variable_ids: typing.Dict[str, typing.Dict[str, int]] = {
            var: {
                instrument: int(count) for instrument, count in wl.items()
            } for var, wl in contents['variable_ids'].items()
        }

    def tags_for_instrument_id(self, instrument_id: str) -> typing.Set[str]:
        return self.tags.get(instrument_id, self._EMPTY_SET)

    def instrument_codes_for_instrument_id(self, instrument_id: str) -> typing.Set[str]:
        return self.instrument_codes.get(instrument_id, self._EMPTY_SET)

    def all_instrument_ids(self) -> typing.Set[str]:
        result: typing.Set[str] = set()
        for instruments in self.variable_names.values():
            result.update(instruments)
        return result
