import typing
from forge.vis.status.archive import read_latest_passed


def latest_passed(station: str, mode_name: str) -> typing.Awaitable[typing.Optional[int]]:
    from forge.vis.station.cpd3 import use_cpd3, latest_passed as cpd3_latest_passed
    if use_cpd3(station):
        return cpd3_latest_passed(station, mode_name)
    return read_latest_passed(station, mode_name)
