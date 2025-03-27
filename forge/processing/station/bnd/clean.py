import typing
from ..default.clean import ProfileTagEditing


class StationProfileEditing(ProfileTagEditing):
    # Met comes from radiation, but is used by aerosol, it's also part of the radiation profile, so
    # make sure it's explict
    TAGS_REQUIRES_PROFILE: typing.List[typing.Tuple[typing.Set[str], str]] = [
        (frozenset({"met"}), "aerosol"),
        (frozenset({"ozone"}), "ozone"),
        (frozenset({"radiation"}), "radiation"),
    ]

    def profile_filter_tags(self, profile: str, tags: typing.Set[str]) -> typing.Optional[bool]:
        if "met" in tags:
            return profile in ("aerosol", "radiation")
        return super().profile_filter_tags(profile, tags)


def filter(station: str, start: int, end: int):
    return StationProfileEditing()