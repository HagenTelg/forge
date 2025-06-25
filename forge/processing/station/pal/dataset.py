import typing
from ..default import dataset as default_dataset


def doi(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def license(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None


def address(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Erik Palmenin aukio 1\nFI-00560, Helsinki\nFinland"


def creator_name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Heikki Lihavainen"


def creator_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "group"


def creator_email(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Heikki.lihavainen@fmi.fi"


def creator_institution(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "Finnish Meteorological Institute (FMI)"


def creator_url(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return None
