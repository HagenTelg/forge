import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 30.39675


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return -84.32955


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 18.0


def country_code(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US"


def subdivision(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FL"


def name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'radiation' in tags:
        return "Tallahassee"
    return "Tallahassee, Florida"