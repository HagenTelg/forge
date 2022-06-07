import typing


def latitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return None


def longitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return None


def altitude(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    return None


def inlet_height(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    if tags and 'met' in tags:
        return None
    return 10.0
