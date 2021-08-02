import typing


def latest_passed(station: str, mode_name: str) -> typing.Awaitable[typing.Optional[int]]:
    from forge.vis.station.cpd3 import latest_passed
    return latest_passed(station, mode_name)
