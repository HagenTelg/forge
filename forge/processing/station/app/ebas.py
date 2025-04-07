import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US3446C"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US3446S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US10L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "C"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="US10L",
        OR_NAME="Appalachian Atmospheric Interdisciplinary Research Facility",
        OR_ACRONYM="AppalAIR", OR_UNIT=None,
        OR_ADDR_LINE1="c/o James Sherman - CAP Building Room 231", OR_ADDR_LINE2="525 Rivers St",
        OR_ADDR_ZIP="28608", OR_ADDR_CITY="Boone, NC", OR_ADDR_COUNTRY="USA"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Sherman", PS_FIRST_NAME="James",
        PS_EMAIL="shermanjp@appstate.edu",
        PS_ORG_NAME="Appalachian Atmospheric Interdisciplinary Research Facility",
        PS_ORG_ACR="AppalAIR", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="c/o James Sherman - CAP Building Room 231", PS_ADDR_LINE2="525 Rivers St",
        PS_ADDR_ZIP="28608", PS_ADDR_CITY="Boone, NC",
        PS_ADDR_COUNTRY="USA",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if end_epoch_ms <= 1284940800000 and type_code.startswith("absorption_"):
        type_code = "psap1w_" + type_code[11:]
    elif end_epoch_ms <= 1457308800000 and type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]
    elif type_code.startswith("scattering_"):
        if end_epoch_ms <= 1704758400000:
            type_code = "tsi3563nephelometer_" + type_code[11:]
        else:
            type_code = "ecotech3000nephelometer_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        if end_epoch_ms <= 1471392000000:
            type_code = "tsi3760cpc_" + type_code[4:]
        else:
            type_code = "tsi3010cpc_" + type_code[4:]

    if type_code == "psap1w_lev0" and end_epoch_ms <= 1496620800000:
        type_code = "psap1wlegacy_lev0"
    elif type_code == "psap3w_lev0" and end_epoch_ms <= 1496620800000:
        type_code = "psap3wlegacy_lev0"

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if isinstance(result, ScatteringLevel2File):
        return result.with_limits_fine(
            (-30, None), (-40, None),
            (-30, None), (-40, None),
            (-30, None), (-40, None),
            (-6, None), (-10, None),
            (-6, None), (-10, None),
            (-6, None), (-10, None),
        )
    return result


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List[InstrumentSelection]]]:
    from ..default.ebas import standard_submit
    return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List[InstrumentSelection], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)
