#!/usr/bin/env python3
import typing
import numpy as np
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.climatology import vaisala_hmp_limits
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.derived.average import hourly_median
from forge.data.flags import parse_flags, declare_flag
from forge.data.merge.extend import extend_selected


def scattering_corrections(data: AvailableData) -> None:
    for scattering in data.select_instrument({"instrument": "mrinephelometer"}, end="1991-03-29"):
        anderson_ogren_1998(scattering)
    standard_scattering_corrections(data)


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="2007-01-01", end="2017-01-28T08:26:37Z"
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((165, 285),),
            contaminated_minimum_speed=1.0,
            extend_before_ms=10 * 60 * 1000,
            extend_after_ms=10 * 60 * 1000,
        )
    # "Disable WS contamination while anemometer is broken" - DCH @ 2017-01-28T08:26:37Z to 2017-04-20
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="2017-04-20"
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((165, 285),),
            contaminated_minimum_speed=1.0,
            extend_before_ms=10 * 60 * 1000,
            extend_after_ms=10 * 60 * 1000,
        )

    # High CPC spike detection
    for aerosol, cpc in data.select_instrument((
            {"tags": "aerosol -met"},
    ), {"instrument_id": "N71"}, start="2015-04-30"):
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
            apply_bits = extend_selected(apply_bits, system_flags.times, 3 * 60 * 1000, 3 * 60 * 1000)
            bit = declare_flag(system_flags.variable, "data_contamination_cpc", 0x01)
            system_flags[apply_bits] = np.bitwise_or(system_flags[apply_bits], bit)


def run(data: AvailableData) -> None:
    for met in data.select_instrument({"instrument_id": "XM1"}, start="2007-01-01"):
        meteorological_climatology_limits(
            met,
            temperature_range=(-18, 39),
            dewpoint_range=(11, 33),
            pressure_range=(935, 1012),
            precipitation_range=(0, 200),
            normalized_temperature_rate_of_change=(-0.05, 0.05),
        )

    aerosol_contamination(data)

    # MRI neph data is already STP corrected, but the default excludes it anyway
    standard_stp_corrections(data)
    standard_absorption_corrections(data)
    scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)

    for met in data.select_instrument({"instrument_id": "XM1"}, start="2018-01-18T21:59:00Z"):
        vaisala_hmp_limits(met)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
