import typing


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "KR0101R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "KR0101S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US06L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Remote Park"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Coastal"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 2


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="US06L",
        OR_NAME="School of Earth and Environmental Sciences, Seoul National University",
        OR_ACRONYM=None, OR_UNIT=None,
        OR_ADDR_LINE1="1 Gwanak-ro, Gwanak-gu", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="08826", OR_ADDR_CITY="Seoul", OR_ADDR_COUNTRY="Korea"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Kim", PS_FIRST_NAME="Sang-Woo",
        PS_EMAIL="sangwookim@snu.ac.kr",
        PS_ORG_NAME="School of Earth and Environmental Sciences, Seoul National University",
        PS_ORG_ACR=None, PS_ORG_UNIT=None,
        PS_ADDR_LINE1="1 Gwanak-ro, Gwanak-gu", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="08826", PS_ADDR_CITY="Seoul",
        PS_ADDR_COUNTRY="Korea",
        PS_ORCID=None,
    )]
