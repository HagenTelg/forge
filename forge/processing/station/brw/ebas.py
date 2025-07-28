import typing

if typing.TYPE_CHECKING:
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US0008R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US0008S"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Snowfield"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Costal"


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US840USA (na_iso_id)"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "G"


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if end_epoch_ms <= 1155340800000 and type_code.startswith("absorption_"):
        type_code = "psap1w_" + type_code[11:]
    elif end_epoch_ms <= 1408147200000 and type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        if end_epoch_ms <= 670377600000:
            type_code = "gerichcpc_" + type_code[4:]
        elif end_epoch_ms <= 1723161600000:
            type_code = "tsi3010cpc_" + type_code[4:]
        else:
            type_code = "admagic250cpc_" + type_code[4:]

    if type_code == "psap1w_lev0" and end_epoch_ms <= 1471478400000:
        type_code = "psap1wlegacy_lev0"
    elif type_code == "psap3w_lev0" and end_epoch_ms <= 1471478400000:
        type_code = "psap3wlegacy_lev0"

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if isinstance(result, ScatteringLevel2File):
        return result.with_limits_fine(
            (-15, None), (-10, None),
            (-15, None), (-10, None),
            (-15, None), (-10, None),
            (-10, None), (-10, None),
            (-10, None), (-10, None),
            (-10, None), (-10, None),
        )
    return result


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from ..default.ebas import standard_submit
    return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)
