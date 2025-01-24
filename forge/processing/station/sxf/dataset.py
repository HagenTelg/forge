import typing
from ..default.dataset import doi as default_doi


def license(station: str, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    return "https://creativecommons.org/publicdomain/zero/1.0/"
