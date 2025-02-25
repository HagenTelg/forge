import typing


def land_use(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Not available"


def setting(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Mountain"


def wmo_region(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[int]:
    return 6


def gaw_type(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "R"


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE=None,
        OR_NAME="Environmental Radioactivity Laboratory Institute of Nuclear and Radiological Science and Technology \"Demokritos\" Ag. Paraskevi",
        OR_ACRONYM=None, OR_UNIT=None,
        OR_ADDR_LINE1="Attiki", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP=None, OR_ADDR_CITY="Athens", OR_ADDR_COUNTRY="Greece"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Eleftheriadis", PS_FIRST_NAME="Kostas",
        PS_EMAIL="elefther@ipta.demokritos.gr",
        PS_ORG_NAME="Environmental Radioactivity Laboratory Institute of Nuclear and Radiological Science and Technology \"Demokritos\" Ag. Paraskevi",
        PS_ORG_ACR=None, PS_ORG_UNIT=None,
        PS_ADDR_LINE1="Attiki", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP=None, PS_ADDR_CITY="Athens",
        PS_ADDR_COUNTRY="Greece",
        PS_ORCID=None,
    )]