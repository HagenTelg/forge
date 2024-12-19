#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.climatology import vaisala_hmp_limits
from forge.processing.station.default.editing import standard_corrections, standard_intensives, standard_meteorological


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="2005-08-12",
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((345, 55),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=20*60*1000,
            extend_after_ms=20*60*1000,
        )


def run(data: AvailableData) -> None:
    aerosol_contamination(data)

    standard_corrections(data)
    standard_intensives(data)
    standard_meteorological(data)

    for met in data.select_instrument({"instrument_id": "XM1"}, start="2016-07-05T17:16:00Z"):
        vaisala_hmp_limits(met)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
