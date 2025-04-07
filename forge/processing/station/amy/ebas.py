import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "KR0100R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "KR0100S"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Coastal"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE=None,
        OR_NAME="National Institute of Meteorological Research/Korea Meteorological Administration",
        OR_ACRONYM=None, OR_UNIT="Asian Dust Research Laboratory",
        OR_ADDR_LINE1="45 Gisangcheong-gil, Dongjak-gu", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="156-720", OR_ADDR_CITY="Seoul", OR_ADDR_COUNTRY="Republic of Korea"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Kim", PS_FIRST_NAME="Jeong-Eun",
        PS_EMAIL="jekim@kma.go.kr",
        PS_ORG_NAME="National Institute of Meteorological Research/Korea Meteorological Administration",
        PS_ORG_ACR=None, PS_ORG_UNIT="Asian Dust Research Laboratory",
        PS_ADDR_LINE1="45 Gisangcheong-gil, Dongjak-gu", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="156-720", PS_ADDR_CITY="Seoul",
        PS_ADDR_COUNTRY="Republic of Korea",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if type_code.startswith("cpc_"):
        type_code = "bmi1720cpc" + type_code[4:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
