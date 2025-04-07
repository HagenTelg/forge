import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES1778R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES1778S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES95L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


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
        OR_CODE="ES05L",
        OR_NAME="Institute of Environmental Assessment and Water Research",
        OR_ACRONYM="IDAEA/CSIC", OR_UNIT="Spanish Council for Scientific Research",
        OR_ADDR_LINE1="C/ Jordi Girona 18-26", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="08034", OR_ADDR_CITY="Barcelona", OR_ADDR_COUNTRY="Spain"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Alastuey", PS_FIRST_NAME="Andrés",
        PS_EMAIL="andres.alastuey@idaea.csic.es",
        PS_ORG_NAME="Institute of Environmental Assessment and Water Research",
        PS_ORG_ACR="IDAEA/CSIC", PS_ORG_UNIT="Spanish Council for Scientific Research",
        PS_ADDR_LINE1="C/ Jordi Girona 18-26", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="08034", PS_ADDR_CITY="Barcelona",
        PS_ADDR_COUNTRY="Spain",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if type_code.startswith("cpc_"):
        type_code = "tsi3772cpc_" + type_code[4:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
