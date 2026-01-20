import typing

if typing.TYPE_CHECKING:
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US6004G"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US6004S"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Snowfield"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Polar"


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US840USA (na_iso_id)"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 7


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "G"


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if end_epoch_ms <= 599616000000 and type_code.startswith("cpc_"):
        type_code = "gerichcpc_" + type_code[4:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)

    result = result.with_file_metadata({
        'hum_temp_ctrl': None,
    })

    if issubclass(result, ScatteringLevel2File):
        result = result.with_limits(
            (-20, None),
            (-20, None),
            (-20, None),
            (-5, None),
            (-5, None),
            (-5, None),
        )

    return result


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from ..default.ebas import standard_submit
    return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)
