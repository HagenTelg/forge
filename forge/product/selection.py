import typing
import asyncio
import logging
from pathlib import Path
from math import floor, ceil
from netCDF4 import Dataset, Variable
from forge.logicaltime import containing_year_range, year_bounds
from forge.archive.client import index_file_name, data_file_name
from forge.archive.client.connection import Connection
from forge.archive.client.archiveindex import ArchiveIndex

_LOGGER = logging.getLogger(__name__)


class InstrumentSelection:
    def __init__(
            self,
            require_tags: typing.Optional[typing.Iterable[str]] = None,
            exclude_tags: typing.Optional[typing.Iterable[str]] = None,
            instrument_id: typing.Optional[typing.Iterable[str]] = None,
            instrument_type: typing.Optional[typing.Iterable[str]] = None,
    ):
        self.require_tags = set(require_tags) if require_tags is not None else set()
        self.exclude_tags = set(exclude_tags) if exclude_tags is not None else set()
        self.instrument_id = set(instrument_id) if instrument_id is not None else set()
        self.instrument_type = set(instrument_type) if instrument_type is not None else set()

    async def fetch_files(self, connection: Connection, station: str, archive: str,
                          start_epoch_ms: int, end_epoch_ms: int,
                          output_directory: Path) -> None:
        def possible_instrument_ids(index: ArchiveIndex) -> typing.Set[str]:
            result: typing.Set[str] = set(index.known_instrument_ids)
            if self.instrument_id:
                result &= self.instrument_id
            if self.require_tags:
                for check in list(result):
                    tags = index.tags.get(check)
                    if not self.require_tags.issubset(tags):
                        result.remove(check)
                        continue
            if self.instrument_type:
                for check in list(result):
                    types = index.instrument_codes.get(check)
                    if not types or self.require_tags.isdisjoint(types):
                        result.remove(check)
                        continue
            return result

        async def fetch_instrument_file(instrument_id: str, file_time: float) -> typing.Optional[Path]:
            archive_path = data_file_name(station, archive, instrument_id, file_time)
            output_file = output_directory / Path(archive_path).name
            if output_file.exists():
                return None
            with output_file.open("wb") as f:
                try:
                    await connection.read_file(archive_path, f)
                    return output_file
                except FileNotFoundError:
                    _LOGGER.debug(f"No archive file for {output_file.name}")
                    pass
            try:
                output_file.unlink()
            except (OSError, FileNotFoundError):
                pass
            return None

        def filter_file(check: Path) -> None:
            root = Dataset(str(check), 'r')
            accepted = True
            try:
                if self.require_tags or self.exclude_tags:
                    tags = set(str(getattr(root, 'forge_tags', "")).split())
                    if self.require_tags and not self.require_tags.issubset(tags):
                        accepted = False
                        return
                    if self.exclude_tags and not self.exclude_tags.isdisjoint(tags):
                        accepted = False
                        return
                if self.instrument_type:
                    instrument = str(getattr(root, 'instrument', ""))
                    if not instrument or instrument not in self.instrument_type:
                        accepted = False
                        return
            finally:
                root.close()
                if not accepted:
                    _LOGGER.debug(f"Rejected instrument file {check.name}")
                    try:
                        check.unlink()
                    except (OSError, FileNotFoundError):
                        pass
                else:
                    _LOGGER.debug(f"Accepted instrument file {check.name}")

        for year in range(*containing_year_range(start_epoch_ms / 1000.0, end_epoch_ms / 1000.0)):
            year_start, year_end = year_bounds(year)
            try:
                index = await connection.read_bytes(index_file_name(station, archive, year_start))
            except FileNotFoundError:
                continue
            index = ArchiveIndex(index)

            if archive in ('avgd', 'avgm'):
                instruments = possible_instrument_ids(index)
                if not instruments:
                    _LOGGER.debug(f"No candidate instruments for {station.upper()}/{archive.upper()}/{year}")
                    continue
                _LOGGER.debug(f"Matched {len(instruments)} candidate instruments for {station.upper()}/{archive.upper()}/{year}")
                for instrument_id in instruments:
                    created_file = await fetch_instrument_file(instrument_id, year_start)
                    if not created_file:
                        continue
                    filter_file(created_file)
            else:
                instruments = possible_instrument_ids(index)
                if not instruments:
                    _LOGGER.debug(f"No candidate instruments for {station.upper()}/{archive.upper()}/{year}")
                    continue
                _LOGGER.debug(f"Matched {len(instruments)} candidate instruments for {station.upper()}/{archive.upper()}/{year}")
                start_day_ms = int(floor(start_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60 * 1000
                end_day_ms = int(ceil(end_epoch_ms / (24 * 60 * 60 * 1000))) * 24 * 60 * 60 * 1000
                start_day_ms = max(start_day_ms, int(floor(year_start * 1000)))
                end_day_ms = min(end_day_ms, int(ceil(year_end * 1000)))
                for file_time_ms in range(start_day_ms, end_day_ms, 24 * 60 * 60 * 1000):
                    for instrument_id in instruments:
                        created_file = await fetch_instrument_file(instrument_id, file_time_ms / 1000.0)
                        if not created_file:
                            continue
                        filter_file(created_file)


class VariableSelection:
    def __init__(
            self,
            variable_id: typing.Optional[str] = None,
            variable_name: typing.Optional[str] = None,
            standard_name: typing.Optional[str] = None,
    ):
        self.variable_id = variable_id
        self.variable_name = variable_name
        self.standard_name = standard_name

    def matches_variable(self, var: Variable) -> bool:
        if self.variable_name is not None:
            if var.name != self.variable_name:
                return False

        if self.variable_id is not None:
            check = getattr(var, 'variable_id', None)
            if check is None or check != self.variable_id:
                return False

        if self.standard_name is not None:
            check = getattr(var, 'standard_name', None)
            if check is None or check != self.standard_name:
                return False

        return True

    @classmethod
    def find_matching_variables(
            cls,
            root: Dataset,
            *selections: typing.Union[typing.Dict, "VariableSelection", str],
            statistics: typing.Optional[str] = None,
            allow_constant: bool = False,
    ) -> typing.Iterator[Variable]:
        effective_selections = list()
        for sel in selections:
            if isinstance(sel, str):
                sel = cls(variable_id=sel)
            elif isinstance(sel, dict):
                sel = cls(**sel)
            effective_selections.append(sel)

        def walk_group(g: Dataset):
            for var in g.variables.values():
                if len(var.dimensions) == 0 or var.dimensions[0] != "time":
                    if not allow_constant:
                        continue
                for sel in effective_selections:
                    if sel.matches_variable(var):
                        yield var
                        break

            for name, sub in g.groups.items():
                if name == "statistics":
                    continue
                yield from walk_group(sub)

        def find_statistics(g: Dataset):
            for name, sub in g.groups.items():
                if name == "statistics":
                    check = sub.groups.get(statistics)
                    if check is not None:
                        yield from walk_group(check)
                else:
                    yield from find_statistics(sub)

        if statistics:
            yield from find_statistics(root)
        else:
            yield from walk_group(root)
