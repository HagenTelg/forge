import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "TW0100R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "TW0100S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "TW01L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 2


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "C"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="TW01L",
        OR_NAME="National Central University Taiwan",
        OR_ACRONYM=None, OR_UNIT=None,
        OR_ADDR_LINE1="300 Jung-da Rd.", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="32001", OR_ADDR_CITY="Chung-li Tao-yuan", OR_ADDR_COUNTRY="Taiwan"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Lin", PS_FIRST_NAME="Neng-Huei",
        PS_EMAIL="nhlin@cc.ncu.edu.tw",
        PS_ORG_NAME="National Central University Taiwan",
        PS_ORG_ACR=None, PS_ORG_UNIT=None,
        PS_ADDR_LINE1="300 Jung-da Rd.", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="32001", PS_ADDR_CITY="Chung-li Tao-yuan",
        PS_ADDR_COUNTRY="Taiwan",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if end_epoch_ms <= 1701734400000 and type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        if end_epoch_ms <= 1677542400000:
            type_code = "tsi3010cpc_" + type_code[4:]
        else:
            type_code = "admagic200cpc_" + type_code[4:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if isinstance(result, ScatteringLevel2File):
        return result.with_limits_fine(
            (-25, None), (-7, None),
            (-25, None), (-30, None),
            (-25, None), (-7, None),
            (-14, None), (-20, None),
            (-14, None), (-20, None),
            (-14, None), (-20, None),
        )
    return result


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from ..default.ebas import standard_submit
    return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)
