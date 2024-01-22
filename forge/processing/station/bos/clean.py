import typing
from ..default.clean import ProfileTagEditing


class StationProfileEditing(ProfileTagEditing):
    # Met comes from radiation, but is used by aerosol
    TAGS_REQUIRES_PROFILE: typing.List[typing.Tuple[typing.Set[str], str]] = [
        (frozenset({"ozone"}), "ozone"),
        (frozenset({"radiation"}), "radiation"),
    ]


def filter(station: str, start: int, end: int):
    return StationProfileEditing()
