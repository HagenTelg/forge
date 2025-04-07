import typing

if typing.TYPE_CHECKING:
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US6001R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US6004S"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Costal"


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US840USA (NA_ISO_ID)"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 5


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "G"


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if type_code.startswith("cpc_"):
        if end_epoch_ms <= 789955200000:
            type_code = "gerichcpc_" + type_code[4:]
        elif end_epoch_ms <= 1075161600000:
            type_code = "tsi3760cpc_" + type_code[4:]
        else:
            type_code = "tsi3010cpc_" + type_code[4:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)