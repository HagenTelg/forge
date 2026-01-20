import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI0023R"


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI0023S"


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "FI03L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Forest"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Rural"


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "2(SMEARI)"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 6


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def projects(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List[str]:
    if tags and 'nrt' in tags:
        return ["GAW-WDCA_NRT", "NOAA-ESRL_NRT", "ACTRIS_NRT"]
    return ["GAW-WDCA", "NOAA-ESRL", "ACTRIS"]


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="FI03L",
        OR_NAME="University of Helsinki",
        OR_ACRONYM="UHEL", OR_UNIT="Institute for Atmospheric and Earth System Research",
        OR_ADDR_LINE1="Gustaf Hällströmin katu 2", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="FI-00560", OR_ADDR_CITY="Helsinki", OR_ADDR_COUNTRY="Finland"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Kulmala", PS_FIRST_NAME="Markku",
        PS_EMAIL="markku.kulmala@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT="Institute for Atmospheric and Earth System Research",
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID="0000-0003-3464-7825",
    ), DataObject(
        PS_LAST_NAME="Petäjä", PS_FIRST_NAME="Tuukka",
        PS_EMAIL="tuukka.petaja@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT="Institute for Atmospheric and Earth System Research",
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2a", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID="0000-0002-1881-9044",
    )]


def submitter(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Kulmala", PS_FIRST_NAME="Markku",
        PS_EMAIL="markku.kulmala@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT="Institute for Atmospheric and Earth System Research",
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID="0000-0003-3464-7825",
    ), DataObject(
        PS_LAST_NAME="Petäjä", PS_FIRST_NAME="Tuukka",
        PS_EMAIL="tuukka.petaja@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT="Institute for Atmospheric and Earth System Research",
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2a", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID="0000-0002-1881-9044",
    ), DataObject(
        PS_LAST_NAME="Tapio", PS_FIRST_NAME="Elomaa",
        PS_EMAIL="tapio.elomaa@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT="Institute for Atmospheric and Earth System Research",
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2a", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID=None,
    ), DataObject(
        PS_LAST_NAME="Hageman", PS_FIRST_NAME="Derek",
        PS_EMAIL="derek.hageman@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT="Institute for Atmospheric and Earth System Research",
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2a", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID=None,
    ),]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file
    from forge.product.ebas.file.aerosol_instrument import AerosolInstrument
    from forge.product.ebas.file.scattering import Level2File as ScatteringLevel2File
    from forge.product.ebas.file.mageeae33_lev0 import File as AE33Level0File
    from forge.product.ebas.file.mageeae33_lev2 import Level2File as AE33Level2File

    if type_code.startswith("scattering_"):
        if end_epoch_ms <= 1734040800000:
            type_code = "tsi3563nephelometer_" + type_code[11:]
        else:
            type_code = "acoemnex00nephelometer_" + type_code[11:]

    result = file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)


    result = result.with_file_metadata({
        'hum_temp_ctrl': "Nafion dryer",
        'hum_temp_ctrl_desc': "air dried from ambient with nafion drier",
    })

    # Changed to a static impactor 2024-12-13
    if issubclass(result, AerosolInstrument) and end_epoch_ms > 1734040800000:
        result = result.with_inlet({
            'pm1': ('Impactor--direct', 'Impactor at 1 um'),
            'pm10': ("Impactor--direct", 'Impactor at 10 um'),
            'pm25': ("Impactor--direct", 'Impactor at 2.5 um'),
            'aerosol': ('Hat or hood', None),
        })

    if issubclass(result, ScatteringLevel2File):
        return result.with_limits_fine(
            (-50, None), (-10, None),
            (-50, None), (-10, None),
            (-50, None), (-10, None),
            (-20, None), (-1, None),
            (-20, None), (-1.5, None),
            (-20, None), (-0.1, None),
        )
    elif issubclass(result, AE33Level0File):
        return result.with_file_metadata({
            'leakage_factor_zeta': 0.01,
        })
    elif issubclass(result, AE33Level2File):
        return result.with_file_metadata({
            'flow_rate': [5.0, "l/min"],
        })
    return result


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
    from forge.processing.station.lookup import station_data
    from forge.product.selection import InstrumentSelection

    user = station_data(gaw_station, 'ebas', 'platform')(gaw_station)
    if user.endswith('S'):
        user = user[:-1]

    return {
        "absorption_lev0": ("raw", [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        )], user, "PSAP"),
    }