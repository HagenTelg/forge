import typing
import numpy as np
from abc import ABC, abstractmethod
from netCDF4 import Dataset
from forge.processing.station.lookup import station_data


class StationFileFilter(ABC):
    @abstractmethod
    def profile_accepts_file(self, profile: str, file: Dataset) -> bool:
        pass

    @staticmethod
    def load_station(station: typing.Optional[str], limit_start: int = None, limit_end: int = None) -> "StationFileFilter":
        if station is None:
            from forge.processing.station.default.clean import filter
            return filter("nil", limit_start, limit_end)
        else:
            return station_data(station or 'default', 'clean', 'filter')(station, limit_start, limit_end)

    @staticmethod
    def file_tags(file: Dataset) -> typing.Set[str]:
        return set(getattr(file, 'forge_tags', "").split())


class AcceptIntoClean:
    def __init__(self, station: str, passed_file: str, limit_start: int, limit_end: int):
        self._passed_file: typing.Optional[Dataset] = None
        passed_file = Dataset(passed_file, 'r')
        try:
            passed_data = passed_file.groups.get("passed")
            if passed_data is not None:
                self._pass_start: np.ndarray = passed_data.variables["start_time"][...].data
                self._pass_end: np.ndarray = passed_data.variables["end_time"][...].data
                profile = passed_data.variables["profile"]
                self._profile_map: typing.Dict[int, str] = dict()
                for name, value in profile.datatype.enum_dict.items():
                    self._profile_map[int(value)] = name.lower()
                self._pass_profile: np.ndarray = profile[...].data

                self._passed_file = passed_file
                passed_file = None
        finally:
            if passed_file is not None:
                passed_file.close()

        self.station = station

        self._filter: typing.Optional[StationFileFilter] = None
        if self._passed_file is not None:
            self._filter = StationFileFilter.load_station(station, limit_start, limit_end)

    def close(self) -> None:
        if self._passed_file is not None:
            self._passed_file.close()
            self._passed_file = None

    def accept_file(self, file_start: int, file_end: int, file: str) -> typing.Optional[Dataset]:
        if self._passed_file is None or self._filter is None:
            return None

        file_start_ms = file_start * 1000
        file_end_ms = file_end * 1000
        passed_indices = np.all((
            file_start_ms < self._pass_end,
            file_end_ms > self._pass_start,
        ), axis=0)
        passed_profiles = np.unique(self._pass_profile[passed_indices])
        if len(passed_profiles.shape) == 0 or passed_profiles.shape[0] == 0:
            return None

        data = Dataset(file, 'r+')
        try:
            for profile_number in passed_profiles:
                profile = self._profile_map[int(profile_number)]
                if not self._filter.profile_accepts_file(profile, data):
                    continue
                result = data
                data = None
                return result
        finally:
            if data is not None:
                data.close()
        return None
