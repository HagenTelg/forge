import typing


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "BG0001R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "BG0001S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "BG02L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Remote Park"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 6


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def projects(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List[str]:
    if tags and 'nrt' in tags:
        return ["GAW-WDCA_NRT", "NOAA-ESRL_NRT", "ACTRIS_NRT"]
    return ["GAW-WDCA", "NOAA-ESRL", "ACTRIS"]


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="BG02L",
        OR_NAME="Institut of Nuclear Research and Nuclear Energy",
        OR_ACRONYM=None, OR_UNIT=None,
        OR_ADDR_LINE1="Tsarigradsko chaussee Blvd.", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="1784", OR_ADDR_CITY="Sofia", OR_ADDR_COUNTRY="Bulgaria"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Arsov", PS_FIRST_NAME="Todor Petkov",
        PS_EMAIL="arsoff@inrne.bas.bg",
        PS_ORG_NAME="Institut of Nuclear Research and Nuclear Energy",
        PS_ORG_ACR=None, PS_ORG_UNIT=None,
        PS_ADDR_LINE1="Tsarigradsko chaussee Blvd.", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="1784", PS_ADDR_CITY="Sofia",
        PS_ADDR_COUNTRY="Bulgaria",
        PS_ORCID=None,
    )]