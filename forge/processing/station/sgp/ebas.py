import typing

if typing.TYPE_CHECKING:
    from forge.product.ebas.file import EBASFile


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US6002C"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US6002S"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Agricultural"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "C"


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "E13(BSRN)"


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if end_epoch_ms <= 1118257020000 and type_code.startswith("absorption_"):
        type_code = "psap1w_" + type_code[11:]
    elif type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]

    if type_code == "psap1w_lev0" and end_epoch_ms <= 1299693660000:
        type_code = "psap1wlegacy_lev0"
    elif type_code == "psap3w_lev0" and end_epoch_ms <= 1299693660000:
        type_code = "psap3wlegacy_lev0"

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
