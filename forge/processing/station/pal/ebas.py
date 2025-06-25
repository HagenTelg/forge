import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI0096G"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI0096S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI01L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Remote Park"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 6


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "G"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="FI01L",
        OR_NAME="Finnish Meteorological Institute",
        OR_ACRONYM="FMI", OR_UNIT=None,
        OR_ADDR_LINE1="Erik Palmenin aukio 1", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="FI-00560", OR_ADDR_CITY="Helsinki", OR_ADDR_COUNTRY="Finland"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Lihavainen", PS_FIRST_NAME="Heikki",
        PS_EMAIL="Heikki.lihavainen@fmi.fi",
        PS_ORG_NAME="Finnish Meteorological Institute",
        PS_ORG_ACR="FMI", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="Erik Palmenin aukio 1", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID=None,
    )]
