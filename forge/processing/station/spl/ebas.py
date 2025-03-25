import typing
from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US9050R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US9050S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US09L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Not available"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="US09L",
        OR_NAME="University of Utah",
        OR_ACRONYM="DRI-DAS-SPL", OR_UNIT=None,
        OR_ADDR_LINE1="William Browning Building, 135 S 1460 E. Rm 819", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="84112-0102", OR_ADDR_CITY="Salt Lake City, UT", OR_ADDR_COUNTRY="USA"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Hallar", PS_FIRST_NAME="Gannet",
        PS_EMAIL="gannet.hallar@utah.edu",
        PS_ORG_NAME="University of Utah",
        PS_ORG_ACR="DRI-DAS-SPL", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="William Browning Building, 135 S 1460 E. Rm 819", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="84112-0102", PS_ADDR_CITY="Salt Lake City, UT",
        PS_ADDR_COUNTRY="USA",
        PS_ORCID=None,
    )]


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List[InstrumentSelection]]]:
    from ..default.ebas import standard_submit
    return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List[InstrumentSelection], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)