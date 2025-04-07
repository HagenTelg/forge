import typing

if typing.TYPE_CHECKING:
    from forge.product.ebas.file import EBASFile


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US6005G"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US6005S"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Residential"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Coastal"


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US840USA (NA_ISO_ID)"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "G"


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if end_epoch_ms <= 1128643200000 and type_code.startswith("absorption_"):
        type_code = "psap1w_" + type_code[11:]
    elif end_epoch_ms <= 1370822400000 and type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
