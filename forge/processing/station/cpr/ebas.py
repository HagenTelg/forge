import typing


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "PR0100C"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "PR0100S"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Costal"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "C"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="US06L",
        OR_NAME="University of Puerto Rico",
        OR_ACRONYM="UPR/ITES", OR_UNIT="Institute for Tropical Ecosystem Studies",
        OR_ADDR_LINE1="PO Box 21910", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="00931-1910", OR_ADDR_CITY="San Juan, PR", OR_ADDR_COUNTRY="USA"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Torres", PS_FIRST_NAME="Elvis",
        PS_EMAIL="elvis.torres810@gmail.com",
        PS_ORG_NAME="University of Puerto Rico",
        PS_ORG_ACR="UPR/ITES", PS_ORG_UNIT="Institute for Tropical Ecosystem Studies",
        PS_ADDR_LINE1="PO Box 21910", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="00931-1910", PS_ADDR_CITY="San Juan, PR",
        PS_ADDR_COUNTRY="USA",
        PS_ORCID=None,
    )]