import typing
import datetime
from forge.const import __version__

if typing.TYPE_CHECKING:
    from ebas.io.file import nasa_ames
    from nilutility.datatypes import DataObject
    from forge.product.ebas.file import EBASFile
    from forge.product.selection import InstrumentSelection


def station(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def platform(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def lab_code(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "US06L"


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def other_identifiers(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def wmo_region(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return None


def gaw_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def projects(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List[str]:
    if tags and 'ozone' in tags:
        if 'nrt' in tags:
            return ["GAW-WDCRG_NRT", "NOAA-ESRL_NRT", "EMEP_NRT"]
        return ["GAW-WDCRG", "NOAA-ESRL", "EMEP"]
    if tags and 'nrt' in tags:
        return ["GAW-WDCA_NRT", "NOAA-ESRL_NRT"]
    return ["GAW-WDCA", "NOAA-ESRL"]


def wdca_id(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    from forge.processing.station.lookup import station_data
    cc = station_data(station, 'site', 'country_code')(station, tags)
    if not cc:
        cc = "__"
    sd = station_data(station, 'site', 'subdivision')(station, tags)
    if not sd:
        sd = "__"
    return f"GAWA{cc}{sd}{station.upper()}"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="US06L",
        OR_NAME="National Oceanic and Atmospheric Administration/Earth System Research Laboratory/Global Monitoring Division",
        OR_ACRONYM="NOAA/ESRL/GMD", OR_UNIT=None,
        OR_ADDR_LINE1="325 Broadway", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="80305", OR_ADDR_CITY="Boulder, CO", OR_ADDR_COUNTRY="USA"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    if tags and 'aod' in tags:
        return [DataObject(
            PS_LAST_NAME="Augustine", PS_FIRST_NAME="John",
            PS_EMAIL="John.A.Augustine@noaa.gov",
            PS_ORG_NAME="National Oceanic and Atmospheric Administration/Earth System Research Laboratory/Global Monitoring Division",
            PS_ORG_ACR="NOAA/ESRL/GMD", PS_ORG_UNIT=None,
            PS_ADDR_LINE1="325 Broadway", PS_ADDR_LINE2=None,
            PS_ADDR_ZIP="80305", PS_ADDR_CITY="Boulder, CO",
            PS_ADDR_COUNTRY="USA",
            PS_ORCID="0000-0002-6645-7404",
        )]
    if tags and 'ozone' in tags:
        return [DataObject(
            PS_LAST_NAME="Effertz", PS_FIRST_NAME="Peter",
            PS_EMAIL="peter.effertz@noaa.gov",
            PS_ORG_NAME="National Oceanic and Atmospheric Administration/Earth System Research Laboratory/Global Monitoring Division",
            PS_ORG_ACR="NOAA/ESRL/GMD", PS_ORG_UNIT=None,
            PS_ADDR_LINE1="325 Broadway", PS_ADDR_LINE2=None,
            PS_ADDR_ZIP="80305", PS_ADDR_CITY="Boulder, CO",
            PS_ADDR_COUNTRY="USA",
            PS_ORCID="0000-0002-5147-763X",
        ), DataObject(
            PS_LAST_NAME="Irina", PS_FIRST_NAME="Petropavlovskikh",
            PS_EMAIL="Irina.petro@noaa.gov",
            PS_ORG_NAME='National Oceanic and Atmospheric Administration/Earth System Research Laboratory/Global Monitoring Division',
            PS_ORG_ACR="NOAA/ESRL/GMD", PS_ORG_UNIT=None,
            PS_ADDR_LINE1="325 Broadway", PS_ADDR_LINE2=None,
            PS_ADDR_ZIP="80305", PS_ADDR_CITY="Boulder, CO",
            PS_ADDR_COUNTRY="USA",
            PS_ORCID="0000-0001-5352-1369",
        )]
    if tags and 'met' in tags and 'aerosol' not in tags:
        return [DataObject(
            PS_LAST_NAME="Schultz", PS_FIRST_NAME="Christine",
            PS_EMAIL="christine.schultz@noaa.gov",
            PS_ORG_NAME="National Oceanic and Atmospheric Administration/Earth System Research Laboratory/Global Monitoring Division",
            PS_ORG_ACR="NOAA/ESRL/GMD", PS_ORG_UNIT=None,
            PS_ADDR_LINE1="325 Broadway", PS_ADDR_LINE2=None,
            PS_ADDR_ZIP="80305", PS_ADDR_CITY="Boulder, CO",
            PS_ADDR_COUNTRY="USA",
            PS_ORCID=None,
        )]

    return [DataObject(
        PS_LAST_NAME="Andrews", PS_FIRST_NAME="Betsy",
        PS_EMAIL="betsy.andrews@noaa.gov",
        PS_ORG_NAME="National Oceanic and Atmospheric Administration/Earth System Research Laboratory/Global Monitoring Division",
        PS_ORG_ACR="NOAA/ESRL/GMD", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="325 Broadway", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="80305", PS_ADDR_CITY="Boulder, CO",
        PS_ADDR_COUNTRY="USA",
        PS_ORCID="0000-0002-9394-024X",
    )]


def submitter(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject
    from forge.processing.station.lookup import station_data

    """
    DataObject(
        PS_LAST_NAME="Hageman", PS_FIRST_NAME="Derek",
        PS_EMAIL="Derek.Hageman@noaa.gov",
        PS_ORG_NAME="National Oceanic and Atmospheric Administration/Earth System Research Laboratory/Global Monitoring Division",
        PS_ORG_ACR="NOAA/ESRL/GMD", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="325 Broadway", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="80305", PS_ADDR_CITY="Boulder, CO",
        PS_ADDR_COUNTRY="USA",
        PS_ORCID="0000-0002-4727-5410",
    )
    """

    return station_data(gaw_station, 'ebas', 'originator')(station, tags)


def set_metadata(nas: "nasa_ames.EbasNasaAmes", gaw_station: str,
                 tags: typing.Optional[typing.Set[str]] = None) -> None:
    from forge.processing.station.lookup import station_data
    from ebas import __version__ as ebas_version

    nas.metadata.timezone = "UTC"

    nas.metadata.revdate = datetime.datetime.now(tz=datetime.timezone.utc)
    nas.metadata.revision = "1"
    nas.metadata.revdesc = f"Version numbering not tracked, generated by Forge {__version__} and EBAS-IO {ebas_version}"

    nas.metadata.org = station_data(gaw_station, 'ebas', 'organization')(gaw_station, tags)
    nas.metadata.projects = station_data(gaw_station, 'ebas', 'projects')(gaw_station, tags)
    nas.metadata.originator = station_data(gaw_station, 'ebas', 'originator')(gaw_station, tags)
    nas.metadata.submitter = station_data(gaw_station, 'ebas', 'submitter')(gaw_station, tags)
    nas.metadata.station_code = station_data(gaw_station, 'ebas', 'station')(gaw_station, tags)
    nas.metadata.platform_code = station_data(gaw_station, 'ebas', 'platform')(gaw_station, tags)
    nas.metadata.station_name = station_data(gaw_station, 'site', 'name')(gaw_station, tags)
    nas.metadata.station_wdca_id = station_data(gaw_station, 'ebas', 'wdca_id')(gaw_station, tags)
    nas.metadata.station_gaw_id = gaw_station.upper()
    nas.metadata.station_gaw_name = station_data(gaw_station, 'site', 'name')(gaw_station, tags)
    nas.metadata.station_other_ids = station_data(gaw_station, 'ebas', 'other_identifiers')(gaw_station, tags)
    nas.metadata.station_state_code = station_data(gaw_station, 'site', 'subdivision')(gaw_station, tags)
    nas.metadata.station_landuse = station_data(gaw_station, 'ebas', 'land_use')(gaw_station, tags)
    nas.metadata.station_setting = station_data(gaw_station, 'ebas', 'setting')(gaw_station, tags)
    nas.metadata.station_gaw_type = station_data(gaw_station, 'ebas', 'gaw_type')(gaw_station, tags)
    nas.metadata.station_wmo_region = station_data(gaw_station, 'ebas', 'wmo_region')(gaw_station, tags)
    nas.metadata.station_latitude = station_data(gaw_station, 'site', 'latitude')(gaw_station, tags)
    nas.metadata.station_longitude = station_data(gaw_station, 'site', 'longitude')(gaw_station, tags)
    nas.metadata.station_altitude = station_data(gaw_station, 'site', 'altitude')(gaw_station, tags)

    nas.metadata.mea_latitude = nas.metadata.station_latitude
    nas.metadata.mea_longitude = nas.metadata.station_longitude
    nas.metadata.mea_altitude = nas.metadata.station_altitude
    nas.metadata.mea_height = station_data(gaw_station, 'site', 'inlet_height')(gaw_station, tags)


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from forge.product.ebas.file import EBASFile
    if type_code.startswith("absorption_"):
        type_code = "clap_" + type_code[11:]
    elif type_code.startswith("scattering_"):
        type_code = "tsi3563nephelometer_" + type_code[11:]
    elif type_code.startswith("cpc_"):
        type_code = "tsi3760cpc_" + type_code[4:]
    elif type_code.startswith("aethalometer_"):
        type_code = "mageeae33_" + type_code[13:]
    return EBASFile.from_type_code(type_code)


def submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    return dict()


def standard_submit(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"]]]:
    from forge.product.selection import InstrumentSelection

    return {
        "absorption_lev0": ("clean", [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        )]),
        "absorption_lev1": ("clean", [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        )]),
        "absorption_lev2": ("avgh", [InstrumentSelection(
            require_tags=["absorption"],
            exclude_tags=["secondary", "aethalometer", "thermomaap"],
        )]),
        "scattering_lev0": ("clean", [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )]),
        "scattering_lev1": ("clean", [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )]),
        "scattering_lev2": ("avgh", [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )]),
        "cpc_lev0": ("clean", [InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )]),
        "cpc_lev1": ("clean", [InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )]),
        "cpc_lev2": ("avgh", [InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )]),
    }


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, str, str, typing.List["InstrumentSelection"]]]:
    return dict()


def standard_nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List["InstrumentSelection"], str, str]]:
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
        "scattering_lev0": ("raw", [InstrumentSelection(
            require_tags=["scattering"],
            exclude_tags=["secondary"],
        )], user, "neph"),
        "cpc_lev0": ("raw", [InstrumentSelection(
            require_tags=["cpc"],
            exclude_tags=["secondary"],
        )], user, "CPC"),
    }