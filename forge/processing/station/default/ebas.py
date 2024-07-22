import typing


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US06L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def wmo_region(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return None


def gaw_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def wdca_id(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    from forge.processing.station.lookup import station_data
    cc = station_data(station, 'site', 'country_code')(station, tags)
    if not cc:
        cc = "__"
    sd = station_data(station, 'site', 'subdivision')(station, tags)
    if not sd:
        sd = "__"
    return f"GAWA{cc}{sd}{station.upper()}"
