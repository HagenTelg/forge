import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 43.93333


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return -60.01667


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 4.0


def country_code(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CA"


def subdivision(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "NS"


def name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Sable Island"
