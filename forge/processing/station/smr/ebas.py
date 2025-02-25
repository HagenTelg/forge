import typing
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
    return "1(SMEARI)"


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
        OR_NAME="University of Helsinki/Division of Atmospheric Sciences",
        OR_ACRONYM="UHEL", OR_UNIT=None,
        OR_ADDR_LINE1="PO BOX 64", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="FI-00014", OR_ADDR_CITY="Helsinki", OR_ADDR_COUNTRY="Finland"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Luoma", PS_FIRST_NAME='Krista',
        PS_EMAIL="krista.q.luoma@helsinki.fi",
        PS_ORG_NAME="University of Helsinki/Division of Atmospheric Sciences",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT=None,
        PS_ADDR_LINE1="PO BOX 64", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00014", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID=None,
    )]


def nrt(gaw_station: str) -> typing.Dict[str, typing.Tuple[str, typing.List[InstrumentSelection], str, str]]:
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