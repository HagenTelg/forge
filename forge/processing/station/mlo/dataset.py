import typing
from ..default.dataset import doi as default_doi


def doi(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    if tags and 'eventlog' in tags:
        return default_doi(station, tags)
    if tags and 'edits' in tags:
        return default_doi(station, tags)
    if tags and 'passed' in tags:
        return default_doi(station, tags)
    if tags and 'ozone' in tags:
        return default_doi(station, tags)
    if tags and 'met' in tags:
        return default_doi(station, tags)
    return "10.7289/V55T3HJF"
