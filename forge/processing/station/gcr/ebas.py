import typing


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US0900R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US0900S"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Agricultural"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "C"
