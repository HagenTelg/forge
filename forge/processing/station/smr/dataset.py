import typing
from ..default import dataset as default_dataset


def doi(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def address(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Gustaf Hällströmin katu 2\nFI-00560, Helsinki\nFinland"


def creator_name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Markku Kulmala"


def creator_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "person"


def creator_email(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "markku.kulmala@helsinki.fi"


def creator_institution(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "University of Helsinki/Institute for Atmospheric and Earth System Research (UHEL/INAR)"


def creator_url(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None
