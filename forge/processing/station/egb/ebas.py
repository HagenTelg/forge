import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CA0011R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CA0011S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "CA01L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Agricultural"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 4


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="CA01L",
        OR_NAME="Environment and Climate Change Canada",
        OR_ACRONYM="ECCC", OR_UNIT=None,
        OR_ADDR_LINE1="4905 Dufferin Street", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="M3H 5T4", OR_ADDR_CITY="Toronto, ON", OR_ADDR_COUNTRY="CA"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Sharma", PS_FIRST_NAME="Sangeeta",
        PS_EMAIL="sangeeta.sharma@canada.ca",
        PS_ORG_NAME="Environment and Climate Change Canada",
        PS_ORG_ACR="ECCC", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="4905 Dufferin Street", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="M3H 5T4", PS_ADDR_CITY="Toronto, ON",
        PS_ADDR_COUNTRY="CA",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if end_epoch_ms <= 1335744000000 and type_code.startswith("absorption_"):
        type_code = "psap1w_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        if end_epoch_ms <= 1351123200000:
            type_code = "tsi3775cpc_" + type_code[4:]
        elif end_epoch_ms <= 1427241600000:
            type_code = "tsi3772cpc_" + type_code[4:]
        elif end_epoch_ms <= 1679961600000:
            type_code = "tsi3775cpc_" + type_code[4:]
        else:
            type_code = "tsi3772cpc_" + type_code[4:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if isinstance(result, ScatteringLevel2File):
        return result.with_limits(
            (-4, None),
            (-4, None),
            (-4, None),
            (-2, None),
            (-2, None),
            (-2, None),
        )
    return result


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from ..default.ebas import standard_submit
    return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)
