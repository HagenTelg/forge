import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 36.2874984741


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 100.8963012695


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 3810


def country_code(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CN"


def name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mt. Waliguan"
