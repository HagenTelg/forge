#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
    ):
        # Tank Farm and Waste Management Area/Landfill
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((79, 91),),
            extend_after_ms=5 * 60 * 1000,
            extend_before_ms=5 * 60 * 1000,
        )
        # Contamination from power plant
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((71, 77),),
            extend_after_ms=5 * 60 * 1000,
            extend_before_ms=5 * 60 * 1000,
        )
        # Contamination from airport
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((138, 185),),
            extend_after_ms=5 * 60 * 1000,
            extend_before_ms=5 * 60 * 1000,
        )


def run(data: AvailableData) -> None:
    aerosol_contamination(data)

    standard_stp_corrections(data)
    standard_absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
