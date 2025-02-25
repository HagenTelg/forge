import typing


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "TW0100R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "TW0100S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "TW01L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 2


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "C"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="TW01L",
        OR_NAME="National Central University Taiwan",
        OR_ACRONYM=None, OR_UNIT=None,
        OR_ADDR_LINE1="300 Jung-da Rd.", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="32001", OR_ADDR_CITY="Chung-li Tao-yuan", OR_ADDR_COUNTRY="Taiwan"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Lin", PS_FIRST_NAME="Neng-Huei",
        PS_EMAIL="nhlin@cc.ncu.edu.tw",
        PS_ORG_NAME="National Central University Taiwan",
        PS_ORG_ACR=None, PS_ORG_UNIT=None,
        PS_ADDR_LINE1="300 Jung-da Rd.", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="32001", PS_ADDR_CITY="Chung-li Tao-yuan",
        PS_ADDR_COUNTRY="Taiwan",
        PS_ORCID=None,
    )]