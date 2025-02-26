import typing
from ..default import dataset as default_dataset


def doi(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def license(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def address(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Atmospheric Sounding Station \"El Arenosillo\"\nSan Juan del Puerto - Matalacañas Road, Km 33\n21130 Mazagon, Huelva\nSpain"


def creator_name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Maria del Mar Sorribas Panero"


def creator_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "person"


def creator_email(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "sorribasm@inta.es"


def creator_institution(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "National Institute for Aerospace Technology"


def creator_url(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None
