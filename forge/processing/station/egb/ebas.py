import typing


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CA0011R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CA0011S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CA01L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Agricultural"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="CA01L",
        OR_NAME="Environment and Climate Change Canada",
        OR_ACRONYM="ECCC", OR_UNIT=None,
        OR_ADDR_LINE1="4905 Dufferin Street", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="M3H 5T4", OR_ADDR_CITY="Toronto, ON", OR_ADDR_COUNTRY="CA"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Sharma", PS_FIRST_NAME="Sangeeta",
        PS_EMAIL="sangeeta.sharma@canada.ca",
        PS_ORG_NAME="Environment and Climate Change Canada",
        PS_ORG_ACR="ECCC", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="4905 Dufferin Street", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="M3H 5T4", PS_ADDR_CITY="Toronto, ON",
        PS_ADDR_COUNTRY="CA",
        PS_ORCID=None,
    )]