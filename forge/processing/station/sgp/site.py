import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 67.97361


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 24.11583


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 560.0


def country_code(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI"


def name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Pallas"

