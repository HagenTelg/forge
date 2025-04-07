import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ZA0001G"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ZA0001S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ZA02L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Remote Park"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Costal"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 1


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "G"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="ZA02L",
        OR_NAME="South African Weather Service",
        OR_ACRONYM="SAWS", OR_UNIT=None,
        OR_ADDR_LINE1="SAWS c/o CSIR", OR_ADDR_LINE2="PO Box 320",
        OR_ADDR_ZIP="7599", OR_ADDR_CITY="Stellenbosch", OR_ADDR_COUNTRY="South Africa"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Labuschagne", PS_FIRST_NAME="Casper",
        PS_EMAIL="Casper.Labuschagne@weathersa.co.za",
        PS_ORG_NAME="South African Weather Service",
        PS_ORG_ACR="SAWS", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="SAWS c/o CSIR", PS_ADDR_LINE2="PO Box 320",
        PS_ADDR_ZIP="7599", PS_ADDR_CITY="Stellenbosch",
        PS_ADDR_COUNTRY="South Africa",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if end_epoch_ms <= 1332633600000 and type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        type_code = "tsi3781cpc_" + type_code[4:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
