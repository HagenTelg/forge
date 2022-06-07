import typing
import time
from netCDF4 import Dataset
from forge.formattime import format_iso8601_time
from . import apply_attribute


def conventions(root: Dataset, value: str = "ACDD-1.3,CF-1.9", change_value: bool = True) -> None:
    apply_attribute(root, "Conventions", value, change_value=change_value)


def institution(root: Dataset, value: str = "National Oceanic and Atmospheric Administration/Global Monitoring Laboratory (NOAA/GML)",
                change_value: bool = True) -> None:
    apply_attribute(root, "institution", value, change_value=change_value)


def naming_authority(root: Dataset, value: str = "gov.noaa.gml.grad.forge", change_value: bool = True) -> None:
    apply_attribute(root, "naming_authority", value, change_value=change_value)


def keywords_vocabulary(root: Dataset,
                        value: str = "GCMD:GCMD Keywords, CF:NetCDF COARDS Climate and Forecast Standard Names",
                        change_value: bool = True) -> None:
    apply_attribute(root, "keywords_vocabulary", value, change_value=change_value)


def standard_name_vocabulary(root: Dataset,
                             value: str = "CF Standard Name Table v79",
                             change_value: bool = True) -> None:
    apply_attribute(root, "standard_name_vocabulary", value, change_value=change_value)


def date_created(root: Dataset, now: typing.Optional[float] = None) -> None:
    root.setncattr("date_created", format_iso8601_time(now or time.time()))


def set_basic(root: Dataset, change_value: bool = True) -> None:
    conventions(root, change_value=change_value)
    institution(root, change_value=change_value)
    naming_authority(root, change_value=change_value)
    keywords_vocabulary(root, change_value=change_value)
    standard_name_vocabulary(root, change_value=change_value)
