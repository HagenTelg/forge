import typing
from forge.processing.clean.filter import StationFileFilter


class SingleProfileEditing(StationFileFilter):
    def profile_accepts_file(self, _profile: str, _file) -> bool:
        return True


class ProfileTagEditing(StationFileFilter):
    TAGS_REQUIRES_PROFILE: typing.List[typing.Tuple[typing.Set[str], str]] = [
        (frozenset({"met"}), "met"),
        (frozenset({"ozone"}), "ozone"),
        (frozenset({"radiation"}), "radiation"),
    ]
    CATCH_ALL_PROFILE: str = "aerosol"

    def profile_accepts_file(self, profile: str, file) -> bool:
        tags = self.file_tags(file)
        for require_tags, profile_match in self.TAGS_REQUIRES_PROFILE:
            if require_tags.issubset(tags):
                return profile_match == profile
        return profile == self.CATCH_ALL_PROFILE


def filter(station: str, start: int, end: int) -> StationFileFilter:
    return SingleProfileEditing()
