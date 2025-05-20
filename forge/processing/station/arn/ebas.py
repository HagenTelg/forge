import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES0100R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES0100S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES06L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


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
        OR_CODE="ES06L",
        OR_NAME="National Institute for Aerospace Technology",
        OR_ACRONYM=None, OR_UNIT=None,
        OR_ADDR_LINE1="Atmospheric Sounding Station \"El Arenosillo\"",
        OR_ADDR_LINE2="San Juan del Puerto - Matalacañas Road, Km 33",
        OR_ADDR_ZIP="21130", OR_ADDR_CITY="Mazagon, Huelva", OR_ADDR_COUNTRY="Spain"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Sorribas Panero", PS_FIRST_NAME="Maria del Mar",
        PS_EMAIL="sorribasm@inta.es",
        PS_ORG_NAME="National Institute for Aerospace Technology",
        PS_ORG_ACR=None, PS_ORG_UNIT=None,
        PS_ADDR_LINE1="Atmospheric Sounding Station \"El Arenosillo\"",
        PS_ADDR_LINE2="San Juan del Puerto - Matalacañas Road, Km 33",
        PS_ADDR_ZIP="21130", PS_ADDR_CITY="Mazagon, Huelva",
        PS_ADDR_COUNTRY="Spain",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if type_code.startswith("cpc_"):
        type_code = "tsi3776cpc_" + type_code[4:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if isinstance(result, ScatteringLevel2File):
        return result.with_limits(
            (-1, None),
            (-1, None),
            (-1, None),
            (-1, None),
            (-1, None),
            (-1, None),
        )
    return result


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from ..default.ebas import standard_submit
    return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)
