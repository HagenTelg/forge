import typing
from ..default import dataset as default_dataset


def doi(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def license(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def address(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "C/ Jordi Girona 18-26\n08034 Barcelona\nSpain"


def creator_name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Andrés Alastuey"


def creator_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "person"


def creator_email(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "andres.alastuey@idaea.csic.es"


def creator_institution(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Institute of Environmental Assessment and Water Research/Spanish Council for Scientific Research"


def creator_url(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None
