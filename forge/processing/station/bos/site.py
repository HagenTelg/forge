import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 40.1250


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return -105.2369995117


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return 1689.0


def inlet_height(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    if tags and 'ozone' in tags:
        return 6.0
    return 10.0
