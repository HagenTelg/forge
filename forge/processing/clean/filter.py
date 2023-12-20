import typing
from netCDF4 import Dataset


class StationFileFilter:
    def profile_accepts_file(self, profile: str, file: Dataset) -> bool:
        return True

    @staticmethod
    def load_station(station: typing.Optional[str], limit_start: int = None, limit_end: int = None) -> "StationFileFilter":
        return StationFileFilter()


class AcceptIntoClean:
    def __init__(self, station: str, passed_file: str):
        pass

    def close(self) -> None:
        pass

    def accept_file(self, file_start: int, file: str) -> typing.Optional[Dataset]:
        data = Dataset(file, 'r+')
        return data
