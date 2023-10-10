import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 40.0499992371


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return -88.3666687012


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 213.0


def country_code(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US"


def subdivision(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "IL"


def name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Bondville, Illinois"
