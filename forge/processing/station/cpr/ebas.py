import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


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
        OR_NAME="University of Puerto Rico - Rio Piedras",
        OR_ACRONYM="ACAR", OR_UNIT="Department of Environmental Science/Atmospheric Chemistry and Aerosols Research",
        OR_ADDR_LINE1="University of Puerto Rico - Rio Piedras", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="00925-2537", OR_ADDR_CITY="San Juan, PR", OR_ADDR_COUNTRY="USA"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Sarangi", PS_FIRST_NAME="Bighnaraj",
        PS_EMAIL="bighnarajsarangi1986@gmail.com",
        PS_ORG_NAME="University of Puerto Rico",
        PS_ORG_ACR="ACAR", PS_ORG_UNIT="Department of Environmental Science/Atmospheric Chemistry and Aerosols Research",
        PS_ADDR_LINE1="University of Puerto Rico - Rio Piedras", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="00925-2537", PS_ADDR_CITY="San Juan, PR",
        PS_ADDR_COUNTRY="USA",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if end_epoch_ms <= 1415232000000 and type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]
    elif end_epoch_ms <= 1499385600000 and type_code.startswith("cpc_"):
        type_code = "tsi3022cpc_" + type_code[4:]
    elif end_epoch_ms <= 1521590400000 and type_code.startswith("cpc_"):
        type_code = "tsi3772cpc_" + type_code[4:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if issubclass(result, ScatteringLevel2File):
        return result.with_limits_fine(
            (-8, None), (-25, None),
            (-8, None), (-25, None),
            (-8, None), (-25, None),
            (-5, None), (-15, None),
            (-5, None), (-15, None),
            (-5, None), (-15, None),
        )
    return result


# def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
#     from ..default.ebas import standard_submit
#     return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)
