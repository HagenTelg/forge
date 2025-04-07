import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "HU0002R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "HU0002S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "HU01L"


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
    return ["GAW-WDCA", "NOAA-ESRL", "ACTRIS", "EMEP"]


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="HU01L",
        OR_NAME="Air Chemistry Group of the Hungarian Academy of Sciences",
        OR_ACRONYM=None, OR_UNIT="Department of Earth and Environmental Sciences",
        OR_ADDR_LINE1="University of Pannonia", OR_ADDR_LINE2="P.O.Box 158",
        OR_ADDR_ZIP="H-8201", OR_ADDR_CITY="Veszprém", OR_ADDR_COUNTRY="Hungary"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Hoffer", PS_FIRST_NAME="András",
        PS_EMAIL="hoffera@almos.vein.hu",
        PS_ORG_NAME="Air Chemistry Group of the Hungarian Academy of Sciences",
        PS_ORG_ACR=None, PS_ORG_UNIT="Department of Earth and Environmental Sciences",
        PS_ADDR_LINE1="University of Pannonia", PS_ADDR_LINE2="P.O.Box 158",
        PS_ADDR_ZIP="H-8201", PS_ADDR_CITY="Veszprém", PS_ADDR_COUNTRY="Hungary",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if end_epoch_ms <= 1379894400000 and type_code.startswith("absorption_"):
        type_code = "psap1w_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        type_code = "tsi3010cpc_" + type_code[4:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if isinstance(result, ScatteringLevel2File):
        return result.with_limits_fine(
            (-0.1, None), (-5, None),
            (-0.1, None), (-5, None),
            (-0.1, None), (-5, None),
            (-1, None), (-3, None),
            (-1, None), (-3, None),
            (-1, None), (-3, None),
        )
    return result

