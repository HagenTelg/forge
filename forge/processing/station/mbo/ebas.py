import typing

if typing.TYPE_CHECKING:
    from forge.product.ebas.file import EBASFile


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Remote Park"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if type_code.startswith("absorption_"):
        if end_epoch_ms <= 1523318400000:
            type_code = "psap3w_" + type_code[11:]
        elif end_epoch_ms <= 1715299200000:
            type_code = "clap_" + type_code[11:]
        else:
            type_code = "bmitap_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        type_code = "tsi3010cpc_" + type_code[4:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
