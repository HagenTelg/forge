#!/usr/bin/env python3
import typing
from math import nan
import numpy as np
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.climatology import vaisala_hmp_limits
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.derived.average import hourly_median
from forge.data.merge.extend import extend_selected
from forge.data.flags import parse_flags, declare_flag


def stp_corrections(data: AvailableData) -> None:
    for instrument in data.select_instrument({"instrument": "mrinephelometer"}, end="2002-12-07"):
        # Early neph data uses assumed T/P
        to_stp(instrument, temperature=28, pressure=680)

    standard_stp_corrections(data, start="2002-12-07")

    for instrument in data.select_instrument({"instrument_id": "N11"}, start="2023-01-23"):
        to_stp(instrument)


def scattering_corrections(data: AvailableData) -> None:
    for scattering in data.select_instrument({"instrument": "mrinephelometer"}, end="2002-12-07"):
        anderson_ogren_1998(scattering)
    standard_scattering_corrections(data, start="2002-12-07")


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="1994-01-18",
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((110, 330),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=10 * 60 * 1000,
            extend_after_ms=10 * 60 * 1000,
        )

    # CPC spike detection
    for aerosol, cpc in data.select_instrument((
            {"tags": "aerosol -met"},
    ), {"instrument_id": "N41"}, start="2016-05-23"):
        for system_flags in aerosol.system_flags():
            try:
                cpc_data = cpc.get_input(system_flags, {"variable_id": "N1?"})
            except FileNotFoundError:
                continue
            apply_bits = np.full(cpc_data.values.shape, False, dtype=np.bool_)

            # CPC spike (> 500 and > 2.5x median)
            threshold_cpc_values = hourly_median(cpc_data)
            threshold_cpc_values *= 2.5
            apply_bits[np.logical_and(
                cpc_data.values > 500.0,
                cpc_data.values > threshold_cpc_values
            )] = True

            if not np.any(apply_bits):
                continue
            apply_bits = extend_selected(apply_bits, system_flags.times, 3*60*1000, 3*60*1000)
            bit = declare_flag(system_flags.variable, "data_contamination_cpc", 0x01)
            system_flags[apply_bits] = np.bitwise_or(system_flags[apply_bits], bit)


def run(data: AvailableData) -> None:
    for met in data.select_instrument({"instrument_id": "XM1"}, start="2007-01-01"):
        # Dewpoint sensor heating phase removal.  This removes data that differs from the recent median by a significant amount.
        for dewpoint in met.select_variable((
                {"variable_id": r"TD\d*"},
        )):
            median_dewpoint = hourly_median(dewpoint)
            do_remove = np.full(dewpoint.shape, False, dtype=np.bool_)
            median_delta = np.abs(dewpoint.values - median_dewpoint)
            do_remove[median_delta > 5.0] = True
            do_remove = extend_selected(do_remove, dewpoint.times, 7 * 60 * 60, 7 * 60 * 60)
            dewpoint[do_remove] = nan

        meteorological_climatology_limits(
            met,
            temperature_range=(-85, 0),
            dewpoint_range=(-90, -10),
            pressure_range=(635, 725),
            normalized_temperature_rate_of_change=(-0.05, 0.05),
            maximum_wind_speed=35,
        )

    aerosol_contamination(data)

    stp_corrections(data)
    standard_absorption_corrections(data)
    scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)

    for met in data.select_instrument({"instrument_id": "XM1"}, start="2016-12-20", end="2025-03-29T00:00:00Z"):
        vaisala_hmp_limits(met)
    for met in data.select_instrument({"instrument_id": "XM1"}, start="2025-03-29T00:00:00Z"):
        vaisala_hmp_limits(met, minimum_dewpoint=None)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
