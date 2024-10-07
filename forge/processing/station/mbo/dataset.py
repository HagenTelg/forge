import typing
from ..default import dataset as default_dataset


def title(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'ccgg' in tags:
        return "Imported data from CCGG Picarro measurements"
    return default_dataset.title(station, tags)


def keywords(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'ccgg' in tags:
        return ""
    return default_dataset.keywords(station, tags)


def doi(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return default_dataset.address(station, tags)
    if tags and 'ccgg' in tags:
        return None
    return None


def license(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return "https://creativecommons.org/publicdomain/zero/1.0/"
    if tags and 'ccgg' in tags:
        return None
    return None


def summary(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'ccgg' in tags:
        return "This dataset is constructed from preliminary data imported from CCGG measurements"
    return default_dataset.summary(station, tags)

def references(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'ccgg' in tags:
        return None
    return default_dataset.references(station, tags)


def address(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return default_dataset.address(station, tags)
    if tags and 'ccgg' in tags:
        return default_dataset.address(station, tags)
    return "18115 Campus Way NE\nBothell, WA, USA, 98011-8246"


def creator_name(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return default_dataset.creator_name(station, tags)
    if tags and 'ccgg' in tags:
        return "Carbon Cycle Greenhouse Gasses (CCGG)"
    return "Daniel Jaffe"


def creator_type(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return default_dataset.creator_type(station, tags)
    if tags and 'ccgg' in tags:
        return "group"
    return "person"


def creator_email(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return default_dataset.creator_email(station, tags)
    if tags and 'ccgg' in tags:
        return None
    return "djaffe@uw.edu"


def creator_institution(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return default_dataset.creator_institution(station, tags)
    if tags and 'ccgg' in tags:
        return None
    return "University of Washington-Bothell/School of Science and Technology"


def creator_url(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and ('ozone' in tags and 'aerosol' not in tags):
        return default_dataset.creator_url(station, tags)
    if tags and 'ccgg' in tags:
        return "https://gml.noaa.gov/ccgg/"
    return None
