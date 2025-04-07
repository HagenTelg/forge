import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CN0101G"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CN0101S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CN01L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Desert"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 2


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "G"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="CN01L",
        OR_NAME="Chinese Academy of Meteorological Sciences",
        OR_ACRONYM="CAMS", OR_UNIT=None,
        OR_ADDR_LINE1="46 Zhong-Guan-Cun S. Ave.", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="100081", OR_ADDR_CITY="Beijing", OR_ADDR_COUNTRY="CN"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Sun", PS_FIRST_NAME="Junying",
        PS_EMAIL="jysun@camscma.cn",
        PS_ORG_NAME="Chinese Academy of Meteorological Sciences",
        PS_ORG_ACR="CAMS", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="46 Zhong-Guan-Cun S. Ave.", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="100081", PS_ADDR_CITY="Beijing",
        PS_ADDR_COUNTRY="CN",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        type_code = "tsi3010cpc_" + type_code[4:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
