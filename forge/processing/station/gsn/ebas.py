import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "KR0101R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "KR0101S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US06L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Remote Park"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Coastal"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 2


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="US06L",
        OR_NAME="School of Earth and Environmental Sciences, Seoul National University",
        OR_ACRONYM=None, OR_UNIT=None,
        OR_ADDR_LINE1="1 Gwanak-ro, Gwanak-gu", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="08826", OR_ADDR_CITY="Seoul", OR_ADDR_COUNTRY="Korea"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Kim", PS_FIRST_NAME="Sang-Woo",
        PS_EMAIL="sangwookim@snu.ac.kr",
        PS_ORG_NAME="School of Earth and Environmental Sciences, Seoul National University",
        PS_ORG_ACR=None, PS_ORG_UNIT=None,
        PS_ADDR_LINE1="1 Gwanak-ro, Gwanak-gu", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="08826", PS_ADDR_CITY="Seoul",
        PS_ADDR_COUNTRY="Korea",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if end_epoch_ms <= 1013126400000 and type_code.startswith("absorption_"):
        type_code = "psap1w_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        if end_epoch_ms <= 1013126400000:
            type_code = "tsi3760cpc_" + type_code[4:]
        else:
            type_code = "tsi3776cpc_" + type_code[4:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if isinstance(result, ScatteringLevel2File):
        return result.with_limits_fine(
            (-5, None), (-10, None),
            (-5, None), (-10, None),
            (-5, None), (-10, None),
            (-12, None), (-3, None),
            (-12, None), (-3, None),
            (-12, None), (-3, None),
        )
    return result


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from ..default.ebas import standard_submit
    return standard_submit(gaw_station)


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from ..default.ebas import standard_nrt
    return standard_nrt(gaw_station)
