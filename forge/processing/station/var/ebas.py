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
    return "1(SMEARI)"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 6


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def projects(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List[str]:
    if tags and 'nrt' in tags:
        return ["GAW-WDCA_NRT", "NOAA-ESRL_NRT", "ACTRIS_NRT"]
    return ["GAW-WDCA", "NOAA-ESRL", "ACTRIS"]


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
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2", PS_ADDR_LINE2=None,
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
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID="0000-0002-1881-9044",
    ), DataObject(
        PS_LAST_NAME="Tapio", PS_FIRST_NAME="Elomaa",
        PS_EMAIL="tapio.elomaa@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT="Institute for Atmospheric and Earth System Research",
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID=None,
    ), DataObject(
        PS_LAST_NAME="Hageman", PS_FIRST_NAME="Derek",
        PS_EMAIL="derek.hageman@helsinki.fi",
        PS_ORG_NAME="University of Helsinki",
        PS_ORG_ACR="UHEL", PS_ORG_UNIT="Institute for Atmospheric and Earth System Research",
        PS_ADDR_LINE1="Gustaf Hällströmin katu 2", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="FI-00560", PS_ADDR_CITY="Helsinki", PS_ADDR_COUNTRY="Finland",
        PS_ORCID=None,
    ),]


def file(gaw_station: str, type_code: str, start_epoch_ms: int, end_epoch_ms: int) -> typing.Type["EBASFile"]:
    from ..default.ebas import file

    if end_epoch_ms <= 1632268800000 and type_code.startswith("absorption_"):
        type_code = "psap3w_" + type_code[11:]

    return file(gaw_station, type_code, start_epoch_ms, end_epoch_ms)
