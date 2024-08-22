import typing
from netCDF4 import Dataset
from forge.processing.station.lookup import station_data
from . import apply_attribute


def feature_type(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "featureType", value, change_value=change_value)


def source(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "source", value, change_value=change_value)


def title(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "title", value, change_value=change_value)


def summary(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "summary", value, change_value=change_value)


def keywords(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "keywords", value, change_value=change_value)


def doi(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "doi", value, change_value=change_value)


def license(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "license", value, change_value=change_value)


def acknowledgement(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "acknowledgement", value, change_value=change_value)


def address(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "address", value, change_value=change_value)


def creator_type(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "creator_type", value, change_value=change_value)


def creator_name(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "creator_name", value, change_value=change_value)


def creator_email(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "creator_email", value, change_value=change_value)


def creator_institution(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "creator_institution", value, change_value=change_value)


def creator_url(root: Dataset, value: str, change_value: bool = True) -> None:
    apply_attribute(root, "creator_url", value, change_value=change_value)


def set_dataset(root: Dataset, station: str, tags: typing.Optional[typing.Set[str]] = None,
                override: typing.Optional[typing.Callable[[str], typing.Any]] = None) -> None:
    for code in ('feature_type', 'source', 'title', 'summary', 'keywords', 'doi', 'license', 'acknowledgement',
                 'address', 'creator_type', 'creator_name', 'creator_email', 'creator_institution', 'creator_url'):
        if override:
            value = override(code)
        else:
            value = None
        if value is None:
            value = station_data(station, 'dataset', code)(station, tags)
        if value is None:
            continue
        globals()[code](root, value, change_value=False)
