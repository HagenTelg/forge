import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 43.594983


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return -112.928567


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 1463.0


def country_code(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US"


def subdivision(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ID"


def name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'radiation' in tags:
        return "Idaho Falls"
    return "Idaho Falls, Idaho"