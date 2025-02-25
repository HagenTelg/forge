import typing


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "DE0054R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "DE0054S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "DE08L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Not available"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 6


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "G"


def projects(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List[str]:
    if tags and 'nrt' in tags:
        return ["GAW-WDCA_NRT", "NOAA-ESRL_NRT", "ACTRIS_NRT"]
    return ["GAW-WDCA", "NOAA-ESRL", "ACTRIS"]


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="DE08L",
        OR_NAME="German Environment Agency",
        OR_ACRONYM="UBA", OR_UNIT=None,
        OR_ADDR_LINE1="Zugspitze 5", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="D-82475", OR_ADDR_CITY="Schneefernerhaus", OR_ADDR_COUNTRY="Germany"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Couric", PS_FIRST_NAME="Cedric",
        PS_EMAIL="cedric.couric@uba.de",
        PS_ORG_NAME="German Environment Agency",
        PS_ORG_ACR="UBA", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="Zugspitze 5", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="D-82475", PS_ADDR_CITY="Schneefernerhaus",
        PS_ADDR_COUNTRY="Germany",
        PS_ORCID=None,
    )]