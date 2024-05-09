import typing
import struct
import re
import enum
from math import nan, isfinite
from forge.const import STATIONS
from forge.formattime import format_iso8601_time
from forge.archive.client.archiveindex import ArchiveIndex
from forge.cpd3.identity import Name


_ARCHIVES = frozenset({
    "raw",
    "edited",
    "clean",
    "avgh",
    "passed",
    "events",
})


class Selection:
    def __init__(
            self,
            start: typing.Optional[float] = None,
            end: typing.Optional[float] = None,
            stations: typing.List[str] = None,
            archives: typing.List[str] = None,
            variables: typing.List[str] = None,
            has_flavors: typing.List[str] = None,
            lacks_flavors: typing.List[str] = None,
            exact_flavors: typing.List[str] = None,
            modified_after: typing.Optional[float] = None,
            include_default_station: bool = True,
            include_meta_archive: bool = True,
    ):
        self.start = start if start and isfinite(start) else None
        self.end = end if end and isfinite(end) else None
        self.stations: typing.List[str] = stations if stations is not None else list()
        self.archives: typing.List[str] = archives if archives is not None else list()
        self.variables: typing.List[str] = variables if variables is not None else list()
        self.has_flavors: typing.List[str] = has_flavors if has_flavors is not None else list()
        self.lacks_flavors: typing.List[str] = lacks_flavors if lacks_flavors is not None else list()
        self.exact_flavors: typing.List[str] = exact_flavors if exact_flavors is not None else list()
        self.modified_after = modified_after if modified_after and isfinite(modified_after) else None
        self.include_default_station = bool(include_default_station)
        self.include_meta_archive = bool(include_meta_archive)

    def __repr__(self) -> str:
        parts: typing.List[str] = list()
        if self.start:
            parts.append(f"start={format_iso8601_time(self.start)}")
        if self.end:
            parts.append(f"end={format_iso8601_time(self.end)}")
        for key in ('stations', 'archives', 'variables', 'has_flavors', 'lacks_flavors', 'exact_flavors'):
            value = getattr(self, key, None)
            if not value:
                continue
            parts.append(f"{key}={value}")
        if self.modified_after:
            parts.append(f"modified_after={format_iso8601_time(self.modified_after)}")
        if not self.include_default_station:
            parts.append("include_default_station=False")
        if not self.include_meta_archive:
            parts.append("include_meta_archive=False")

        return "Selection(" + (",".join(parts)) + ")"

    @staticmethod
    def _serialize_list(output: bytearray, data: typing.List[str]) -> None:
        output += struct.pack('<I', len(data))
        for v in data:
            encoded = v.encode('utf-8')
            output += struct.pack('<I', len(encoded))
            output += encoded

    def serialize(self) -> bytes:
        result = bytearray()
        result += struct.pack('<dd',
                              self.start if self.start else nan,
                              self.end if self.end else nan)
        self._serialize_list(result, self.stations)
        self._serialize_list(result, self.archives)
        self._serialize_list(result, self.variables)
        self._serialize_list(result, self.has_flavors)
        self._serialize_list(result, self.lacks_flavors)
        self._serialize_list(result, self.exact_flavors)
        result += struct.pack('<dBB',
                              self.modified_after if self.modified_after else nan,
                              1 if self.include_default_station else 0,
                              1 if self.include_meta_archive else 0)
        return bytes(result)

    @staticmethod
    def _deserialize_list(input: bytearray) -> typing.List[str]:
        result: typing.List[str] = list()
        n = struct.unpack('<I', input[:4])[0]
        del input[:4]
        for _ in range(n):
            l = struct.unpack('<I', input[:4])[0]
            result.append(input[4:4+l].decode('utf-8'))
            del input[:4+l]
        return result

    @classmethod
    def deserialize(cls, data: typing.Union[bytearray, bytes]) -> "Selection":
        if isinstance(data, bytes):
            data = bytearray(data)
        start, end = struct.unpack('<dd', data[:16])
        del data[:16]
        stations = cls._deserialize_list(data)
        archives = cls._deserialize_list(data)
        variables = cls._deserialize_list(data)
        has_flavors = cls._deserialize_list(data)
        lacks_flavors = cls._deserialize_list(data)
        exact_flavors = cls._deserialize_list(data)
        modified_after, include_default_station, include_meta_archive = struct.unpack('<dBB', data[:10])
        del data[:10]
        return cls(
            start=start, end=end,
            stations=stations, archives=archives, variables=variables,
            has_flavors=has_flavors, lacks_flavors=lacks_flavors, exact_flavors=exact_flavors,
            modified_after=modified_after,
            include_default_station=include_default_station, include_meta_archive=include_meta_archive,
        )

    @staticmethod
    def split_sources(selections: typing.Iterable["Selection"]) -> typing.Dict[str, typing.Dict[str, typing.List["Selection"]]]:
        result: typing.Dict[str, typing.Dict[str, typing.List["Selection"]]] = dict()
        for sel in selections:
            if sel.variables == ['alias']:
                continue
            match_stations = [re.compile(v, flags=re.IGNORECASE) for v in sel.stations]
            match_archives = [re.compile(v, flags=re.IGNORECASE) for v in sel.archives]

            for station in STATIONS:
                if match_stations:
                    for check in match_stations:
                        if check.fullmatch(station):
                            break
                    else:
                        continue
                for archive in _ARCHIVES:
                    if match_archives:
                        for check in match_archives:
                            if check.fullmatch(archive):
                                break
                            if sel.include_meta_archive and check.fullmatch(archive + "_meta"):
                                break
                        else:
                            continue

                    dest_station = result.get(station)
                    if not dest_station:
                        dest_station = dict()
                        result[station] = dest_station
                    dest_archive = dest_station.get(archive)
                    if not dest_archive:
                        dest_archive = list()
                        dest_station[archive] = dest_archive
                    dest_archive.append(sel)
        return result

    def match_index(self, index: ArchiveIndex) -> typing.Set[str]:
        match_variables = [re.compile(v) for v in self.variables]

        def matches_variable_id(v: str):
            if not match_variables:
                return True
            for check in match_variables:
                if check.fullmatch(v):
                    return True
            return False

        matched_instruments: typing.Set[str] = set()
        for var_id, instrument_wavelength in index.variable_ids.items():
            for instrument, wavelength_count in instrument_wavelength.items():
                if wavelength_count == 0:
                    if '_' not in var_id:
                        check_name = var_id + '_' + instrument
                    else:
                        check_name = var_id
                    if matches_variable_id(check_name):
                        matched_instruments.add(instrument)
                        continue

                if '_' not in var_id:
                    prefix = var_id
                    suffix = '_' + instrument
                else:
                    prefix, suffix = var_id.split('_', 1)
                    suffix = '_' + suffix

                for wl_code in ('B', 'G', 'R', 'Q'):
                    if matches_variable_id(prefix + wl_code + suffix):
                        matched_instruments.add(instrument)
                        break
                else:
                    for wl_idx in range(wavelength_count):
                        if matches_variable_id(prefix + str(wl_idx+1) + suffix):
                            matched_instruments.add(instrument)
                            break
        return matched_instruments


class FileMatch:
    def __init__(self, selection: Selection, station: str, archive: str):
        self.start = selection.start
        self.end = selection.end
        self.variables = [re.compile(v) for v in selection.variables]
        self.has_flavors = [re.compile(v, flags=re.IGNORECASE) for v in selection.has_flavors]
        self.lacks_flavors = [re.compile(v, flags=re.IGNORECASE) for v in selection.lacks_flavors]
        self.exact_flavors = [re.compile(v, flags=re.IGNORECASE) for v in selection.exact_flavors]

        self.include_meta: bool = False
        self.include_data: bool = False
        if selection.archives:
            meta_archive = archive + "_meta"
            for v in selection.archives:
                m = re.compile(v, flags=re.IGNORECASE)
                if m.fullmatch(archive):
                    self.include_data = True
                    if selection.include_meta_archive:
                        self.include_meta = True
                elif m.fullmatch(meta_archive):
                    self.include_meta = True
        else:
            self.include_meta = True
            self.include_data = True

    def matches_variable_id(self, variable_id: str, instrument_id: str) -> typing.Optional[str]:
        if '_' not in variable_id:
            variable_id = variable_id + '_' + instrument_id
        if not self.variables:
            return variable_id
        for m in self.variables:
            if m.fullmatch(variable_id):
                return variable_id
        return None

    @staticmethod
    def _wavelength_suffix(wl: float) -> typing.Optional[str]:
        if wl < 400:
            return None
        elif wl < 500:
            return "B"
        elif wl < 600:
            return "G"
        elif wl < 750:
            return "R"
        return "Q"

    def matches_variable_wavelengths(self, variable_id: str, instrument_id: str, wavelengths: typing.List[float]) -> typing.List[typing.Tuple[str, int]]:
        wavelength_suffixes: typing.List[str] = list()
        unique_suffixes: typing.Set[str] = set()

        for wl in wavelengths:
            suffix = self._wavelength_suffix(wl)
            if not suffix or suffix in unique_suffixes:
                break
            wavelength_suffixes.append(suffix)
        if len(wavelengths) != len(wavelengths):
            wavelength_suffixes = [str(i+1) for i in range(len(wavelengths))]

        result: typing.List[typing.Tuple[str, int]] = list()
        for wlidx in range(len(wavelengths)):
            if '_' not in variable_id:
                output_id = variable_id + wavelength_suffixes[wlidx] + '_' + instrument_id
            else:
                prefix, suffix = variable_id.split('_', 1)
                output_id = prefix + wavelength_suffixes[wlidx] + '_' + suffix
            if not self.variables:
                result.append((output_id, wlidx))
                continue
            for m in self.variables:
                if m.fullmatch(output_id):
                    result.append((output_id, wlidx))
                    break
        return result

    def _accept_flavors(self, flavors: typing.Set[str]) -> bool:
        if self.has_flavors:
            for m in self.has_flavors:
                for f in flavors:
                    if m.fullmatch(f):
                        break
                else:
                    return False
        if self.lacks_flavors:
            for m in self.lacks_flavors:
                for f in flavors:
                    if m.fullmatch(f):
                        return False
        if self.exact_flavors:
            if len(self.exact_flavors) == 1 and self.exact_flavors[0].pattern == "":
                if len(flavors) != 0:
                    return False
            else:
                for f in flavors:
                    for m in self.exact_flavors:
                        if m.fullmatch(f):
                            break
                    else:
                        return False
        return True

    class Statistics(enum.Enum):
        Root = enum.auto()
        Other = enum.auto()
        Quantiles = enum.auto()

    def matches_flavors(self, cut_size: float,
                        statistics: typing.Optional["FileMatch.Statistics"]) -> typing.Optional[typing.Set[str]]:
        emulated_flavors: typing.Set[str] = set()
        if statistics is not None:
            if statistics == self.Statistics.Quantiles:
                emulated_flavors.add('stats')
            else:
                return None
        if isfinite(cut_size):
            if cut_size < 2.5:
                emulated_flavors.add('pm1')
            elif cut_size < 10.0:
                emulated_flavors.add('pm25')
            else:
                emulated_flavors.add('pm10')
        if not self._accept_flavors(emulated_flavors):
            return None
        return emulated_flavors

    def accept_name(self, name: Name) -> bool:
        if not self._accept_flavors(name.flavors):
            return False
        if self.variables:
            for m in self.variables:
                if m.fullmatch(name.variable):
                    break
            else:
                return False
        if not name.metadata:
            if not self.include_data:
                return False
        else:
            if not self.include_meta:
                return False
        return True
