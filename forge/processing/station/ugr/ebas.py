import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES0020U"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES0020S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "ES08L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Residential"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Suburban"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 6


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "C"


def projects(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List[str]:
    if tags and 'nrt' in tags:
        return ["GAW-WDCA_NRT", "NOAA-ESRL_NRT", "ACTRIS_NRT"]
    return ["GAW-WDCA", "NOAA-ESRL", "ACTRIS"]


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="ES08L",
        OR_NAME="University of Granada",
        OR_ACRONYM=None, OR_UNIT="Atmospheric Physics Group - Andalusian Center for Environmental Studies - Dept. Applied Physics",
        OR_ADDR_LINE1="Avda. del Mediterráneo s/n", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="18006", OR_ADDR_CITY="Granada", OR_ADDR_COUNTRY="Spain"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Alados-Arboledas", PS_FIRST_NAME="Lucas",
        PS_EMAIL="alados@ugr.es",
        PS_ORG_NAME="University of Granada",
        PS_ORG_ACR=None, PS_ORG_UNIT="Atmospheric Physics Group - Andalusian Center for Environmental Studies - Dept. Applied Physics",
        PS_ADDR_LINE1="Avda. del Mediterráneo s/n", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="18006", PS_ADDR_CITY="Granada",
        PS_ADDR_COUNTRY="Spain",
        PS_ORCID=None,
    )]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File

    if type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
    if isinstance(result, ScatteringLevel2File):
        return result.with_limits(
            (-12, None),
            (-12, None),
            (-12, None),
            (-25, None),
            (-25, None),
            (-25, None),
        )
    return result



def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from ..default.ebas import standard_submit
    from forge.product.selection import InstrumentSelection
    result = standard_submit(gaw_station)
    result.update({
        "maap_lev0": ("clean", [InstrumentSelection(
            instrument_id=["A31"],
        )]),
        "maap_lev1": ("clean", [InstrumentSelection(
            instrument_id=["A31"],
        )]),
        "maap_lev2": ("avgh", [InstrumentSelection(
            instrument_id=["A31"],
        )]),
    })
    return result


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from ..default.ebas import standard_nrt
    from forge.product.selection import InstrumentSelection
    from forge.processing.station.lookup import station_data

    user = station_data(gaw_station, 'ebas', 'platform')(gaw_station)
    if user.endswith('S'):
        user = user[:-1]

    result = standard_nrt(gaw_station)
    result.update({
        "maap_lev0": ("raw", [InstrumentSelection(
            instrument_id=["A31"],
        )], user, ""),
    })
    return result
