import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return -77.83011


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 166.66049


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 187.41


def country_code(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "NZ"


def name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Arrival Heights"
