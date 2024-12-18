#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.filter_absorption import weiss_undo, spot_area_adjustment
from forge.processing.corrections.climatology import vaisala_hmp_limits
from forge.processing.derived.average import hourly_median
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags, declare_flag
from forge.data.merge.extend import extend_selected


def absorption_corrections(data: AvailableData) -> None:
    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="2000-04-29", end="2015-05-07T19:45:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, start="2000-04-29", end="2015-05-07T19:45:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2015-05-07T19:45:00Z")


def scattering_corrections(data: AvailableData) -> None:
    for scattering in data.select_instrument((
            {"instrument": "mrinephelometer"},
            {"instrument": "msenephelometer"},
    ), end="2000-04-07"):
        anderson_ogren_1998(scattering)
    standard_scattering_corrections(data)


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="1977-01-01",
            end="2007-02-15",
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((270, 90),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=10 * 60 * 1000,
            extend_after_ms=10 * 60 * 1000,
        )

    # Met wind invalid, use aerosol
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "X1"},
            always_tuple=True,
            start="2007-02-15",
            end="2007-03-09",
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((270, 90),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=10 * 60 * 1000,
            extend_after_ms=10 * 60 * 1000,
        )

    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="2007-03-09",
            end="2015-05-29",
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((270, 90),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=10 * 60 * 1000,
            extend_after_ms=10 * 60 * 1000,
        )

    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="2015-05-29",
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((270, 90),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=20 * 60 * 1000,
            extend_after_ms=20 * 60 * 1000,
        )


def run(data: AvailableData) -> None:
    # 10 meter wind direction is 180 degrees off - TKM
    for met in data.select_instrument({"instrument_id": "XM1"}, start="2012-01-23", end="2012-01-26"):
        for wind_direction in met.select_variable(
                {"variable_id": "WD1"},
        ):
            wind_direction[:] = (wind_direction[:] + 180) % 360
    for met in data.select_instrument({"instrument_id": "XM1"}, start="2007-01-01"):
        meteorological_climatology_limits(
            met,
            temperature_range=(-10, 25),
            dewpoint_range=(-40, 22),
            pressure_range=(660, 690),
            precipitation_range=(0, 200),
            normalized_temperature_rate_of_change=(-0.05, 0.05),
            maximum_wind_speed=50,
        )

    aerosol_contamination(data)

    # MRI/MsE neph data is already STP corrected, but the default excludes it anyway
    standard_stp_corrections(data)
    absorption_corrections(data)
    scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)

    for met in data.select_instrument({"instrument_id": "XM1"}, start="2018-01-26"):
        vaisala_hmp_limits(met)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
