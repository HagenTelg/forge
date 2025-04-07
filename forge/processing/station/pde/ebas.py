import typing

if typing.TYPE_CHECKING:
    from nilutility.datatypes import DataObject
    from forge.product.selection import InstrumentSelection


def organization(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> "DataObject":
    from nilutility.datatypes import DataObject

    return DataObject(
        OR_CODE="US06L",
        OR_NAME="University of Puerto Rico - Rio Piedras",
        OR_ACRONYM="ACAR", OR_UNIT="Department of Environmental Science/Atmospheric Chemistry and Aerosols Research",
        OR_ADDR_LINE1="University of Puerto Rico - Rio Piedras", OR_ADDR_LINE2=None,
        OR_ADDR_ZIP="00925-2537", OR_ADDR_CITY="San Juan, PR", OR_ADDR_COUNTRY="USA"
    )


def originator(gaw_station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.List["DataObject"]:
    from nilutility.datatypes import DataObject

    return [DataObject(
        PS_LAST_NAME="Sarangi", PS_FIRST_NAME="Bighnaraj",
        PS_EMAIL="bighnarajsarangi1986@gmail.com",
        PS_ORG_NAME="University of Puerto Rico",
        PS_ORG_ACR="ACAR", PS_ORG_UNIT="Department of Environmental Science/Atmospheric Chemistry and Aerosols Research",
        PS_ADDR_LINE1="University of Puerto Rico - Rio Piedras", PS_ADDR_LINE2=None,
        PS_ADDR_ZIP="00925-2537", PS_ADDR_CITY="San Juan, PR",
        PS_ADDR_COUNTRY="USA",
        PS_ORCID=None,
    )]