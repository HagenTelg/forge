import typing
from netCDF4 import Dataset


class AcceptIntoClean:
    def __init__(self, passed_file: str):
        pass

    def close(self) -> None:
        pass

    def accept_file(self, file_start: int, file: str) -> typing.Optional[Dataset]:
        data = Dataset(file, 'r+')
        return data
