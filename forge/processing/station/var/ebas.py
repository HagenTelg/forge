import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI0050R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI0050S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI03L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "1(SMEARII)"


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
        OR_CODE="FI03L",
        OR_NAME="University of Helsinki",
        OR_ACRONYM="UHEL", OR_UNIT=None,
        OR_ADDR_LINE1="PO BOX 64", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="FI-00014", OR_ADDR_CITY="Helsinki", OR_ADDR_COUNTRY="Finland"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Banerji", PS_FIRST_NAME='Sujai',
        PS_EMAIL="sujai.banerji@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="PO BOX 64", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00014", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if end_epoch_ms <= 1632268800000 and type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
