#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.filter_absorption import weiss_undo
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.derived.intensives import generate_intensives, AdjustWavelengthParameters
from forge.data.flags import parse_flags, declare_flag


def absorption_corrections(data: AvailableData) -> None:
    # CPD1/2 data: already has Weiss applied for PSAP-1W
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="2008-09-28", end="2010-06-27T21:36:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)

    # Incorrect Weiss constants initially on PSAP-3W
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="2010-06-27T21:36:00Z", end="2011-01-05T22:08:00Z"):
        remove_low_transmittance(absorption)
        weiss_undo(absorption, 0.866, 1.317)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="2011-01-05T22:08:00Z", end="2016-04-24T12:00:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, start="2011-01-05T22:08:00Z", end="2016-04-24T12:00:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2016-04-24T12:00:00Z")


def aerosol_contamination(data: AvailableData) -> None:
    # Apply air quality flagging to global contamination
    for aerosol, air_quality in data.select_instrument((
            {"tags": "aerosol -met", "instrument_id": r"(?!XAQ)"},
    ), {"instrument_id": "XAQ"}):
        for system_flags in aerosol.system_flags():
            try:
                source_flags = air_quality.get_input(system_flags, {
                    "variable_name": "system_flags",
                })
            except FileNotFoundError:
                continue
            if not np.issubdtype(source_flags.values.dtype, np.integer):
                continue
            flags = parse_flags(source_flags.variable)
            matched_bits = 0
            for bits, name in flags.items():
                if name not in ("data_contamination_air_quality", ):
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            apply_bits = np.bitwise_and(source_flags.values, matched_bits) != 0
            if not np.any(apply_bits):
                continue
            bit = declare_flag(system_flags.variable, "data_contamination_air_quality")
            system_flags[apply_bits] = np.bitwise_or(system_flags[apply_bits], bit)

    # Invalidate data when the air quality contamination flag is set.
    # Requested by Sangetta in 2022-06-30 email.
    for aerosol, air_quality in data.select_instrument((
            {"tags": "aerosol -met", "instrument_id": r"(?!XAQ)"},
    ), {"instrument_id": "XAQ"}, start="2017-01-01", end="2021-01-01"):
        for values in aerosol.select_variable((
                {"variable_name": "number_concentration"},
                {"standard_name": "number_concentration_of_ambient_aerosol_particles_in_air"},
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"variable_name": "light_absorption"},
        )):
            try:
                source_flags = air_quality.get_input(values, {
                    "variable_name": "system_flags",
                })
            except FileNotFoundError:
                continue
            if not np.issubdtype(source_flags.values.dtype, np.integer):
                continue
            flags = parse_flags(source_flags.variable)
            matched_bits = 0
            for bits, name in flags.items():
                if name not in ("data_contamination_air_quality", ):
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            apply_bits = np.bitwise_and(source_flags.values, matched_bits) != 0
            if not np.any(apply_bits):
                continue
            values[apply_bits, ...] = nan


    def _cpc_threshold_calculate(source) -> typing.Tuple[np.ndarray, np.ndarray]:
        from forge.data.merge.timealign import incoming_before
        from forge.processing.average.calculate import fixed_interval_weighted_average, fixed_interval_stddev

        average_output = np.full_like(source.values, nan)
        stddev_output = np.full_like(source.values, nan)
        for _, value_select, time_select in source.select_cut_size():
            selected_times = source.times[time_select]
            selected_values = source.values[value_select]

            smoothed_values, smoothed_start = fixed_interval_weighted_average(
                selected_times,
                selected_values,
                source.average_weights[time_select],
                6 * 60 * 60 * 1000,
            )
            smoothed_targets = incoming_before(selected_times, smoothed_start)
            average_output[value_select] = smoothed_values[smoothed_targets]

            stddev_values, smoothed_start = fixed_interval_stddev(
                selected_times,
                selected_values,
                2 * 60 * 60 * 1000,
            )
            smoothed_targets = incoming_before(selected_times, smoothed_start)
            stddev_output[value_select] = stddev_values[smoothed_targets]
        return average_output, stddev_output

    # High CPC contamination and CPC spike detection
    for aerosol, cpc in data.select_instrument((
            {"tags": "aerosol"},
    ), {"instrument_id": "N61"}, start="2016-01-01", end="2016-01-30"):
        for system_flags in aerosol.system_flags():
            try:
                cpc_data = cpc.get_input(system_flags, {"variable_id": "N1?"})
            except FileNotFoundError:
                continue
            apply_bits = np.full(cpc_data.values.shape, False, dtype=np.bool_)

            # High CPC contaminated
            apply_bits[cpc_data.values > 5000.0] = True

            if not np.any(apply_bits):
                continue
            bit = declare_flag(system_flags.variable, "data_contamination_cpc", 0x01)
            system_flags[apply_bits] = np.bitwise_or(system_flags[apply_bits], bit)

    # CPC spike removal
    for cpc in data.select_instrument((
            {"instrument_id": "N61"},
    ), start="2016-01-01", end="2016-01-30"):
        for cpc_data in cpc.select_variable({"variable_id": "N1?"}):
            remove_data = np.full(cpc_data.values.shape, False, dtype=np.bool_)

            # CPC spike (> 100 and > mean (6 hour) + stddev (2 hour) * 2.5)
            mean_cpc, stddev_cpc = _cpc_threshold_calculate(cpc_data)
            threshold_cpc_values = mean_cpc + stddev_cpc * 2.5
            remove_data[np.logical_and(
                cpc_data.values > 100.0,
                cpc_data.values > threshold_cpc_values
            )] = True

            cpc_data[remove_data, ...] = nan


def run(data: AvailableData) -> None:
    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    # PSAP-1W extrapolation
    for intensives, scattering, absorption, cpc in data.derive_output(
            "XI",
            {"tags": "scattering -secondary"},
            {"tags": "absorption -secondary -aethalometer -thermomaap"},
            {"tags": "cpc -secondary"},
            tags=("aerosol", "intensives"),
            end="2010-06-27",
    ):
        generate_intensives(intensives, cpc, scattering, absorption, wavelength_adjustment=AdjustWavelengthParameters(
            fallback_angstrom_exponent=1.0,
        ))
    standard_intensives(data, start="2010-06-27")

    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
