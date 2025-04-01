#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags
from forge.data.merge.extend import extend_selected


def rr_zero_correction(data: AvailableData) -> None:
    # RR neph zero subtraction.  This averages the value during the TSI zero, then subtracts that from the RR neph
    # data.  Then it invalidates all RR neph data while the TSI neph is not in normal operating mode (including
    # zero).

    from forge.data.merge.timealign import incoming_before
    from forge.processing.average.calculate import fixed_interval_weighted_average

    for rr_neph, tsi_neph in data.select_instrument((
            {"instrument": "rrm903nephelometer"},
            {"instrument_id": "S11"},
    ), start="2010-09-20", end="2012-05-09T14:12:00Z"):
        for scattering in rr_neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            try:
                source_flags = tsi_neph.get_input(scattering, {
                    "variable_name": "system_flags",
                })
            except FileNotFoundError:
                continue
            if not np.issubdtype(source_flags.values.dtype, np.integer):
                continue
            flags = parse_flags(source_flags.variable)
            in_zero_bits = 0
            invalidate_bits = 0
            for bits, name in flags.items():
                if name == "zero":
                    in_zero_bits |= bits
                if name in ("zero", "blank", "spancheck"):
                    invalidate_bits |= bits

            if in_zero_bits != 0:
                is_in_zero = np.bitwise_and(source_flags.values, in_zero_bits) != 0

                values_during_zero, times_during_zero = fixed_interval_weighted_average(
                    scattering.times[is_in_zero],
                    scattering.values[is_in_zero],
                    scattering.average_weights[is_in_zero],
                    60 * 60 * 1000,
                )
                values_during_zero[np.invert(np.isfinite(values_during_zero))] = 0

                subtraction = incoming_before(scattering.times, values_during_zero)
                scattering[...] = scattering[...] - values_during_zero[subtraction]

            if invalidate_bits != 0:
                is_invalid = np.bitwise_and(source_flags.values, invalidate_bits) != 0
                scattering[is_invalid, ...] = nan


def stp_corrections(data: AvailableData) -> None:
    for instrument in data.select_instrument((
            {"instrument": "rrm903nephelometer"},
    ), end="2012-05-09"):
        to_stp(instrument)

    standard_stp_corrections(data)


def absorption_corrections(data: AvailableData) -> None:
    # Extend the TSI neph zeros to get all the CLAP data
    for clap, neph in data.select_instrument((
            {"instrument": "clap"},
    ), {"instrument_id": "S11"}, start="2012-01-30"):
        for absorption in clap.select_variable((
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
        )):
            try:
                source_flags = neph.get_input(absorption, {
                    "variable_name": "system_flags",
                })
            except FileNotFoundError:
                continue
            if not np.issubdtype(source_flags.values.dtype, np.integer):
                continue
            flags = parse_flags(source_flags.variable)
            matched_bits = 0
            for bits, name in flags.items():
                if name not in ("zero", "blank"):
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            is_in_zero = np.bitwise_and(source_flags.values, matched_bits) != 0
            is_in_zero = extend_selected(is_in_zero, source_flags.times, 1 * 60 * 1000, 1 * 60 * 1000)
            absorption[is_in_zero, ...] = nan

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2017-06-05T18:03:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2017-06-05T18:03:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2017-06-05T18:03:00Z")


def scattering_corrections(data: AvailableData) -> None:
    # This is probably "wrong" (rr neph cell geometry is different from TSI nephs) but it's what's been done in the
    # past and I don't think there's a better correction available.
    for scattering in data.select_instrument({"instrument": "rrm903nephelometer"}, end="2012-05-09"):
        anderson_ogren_1998(scattering)

    standard_scattering_corrections(data)


def run(data: AvailableData) -> None:
    rr_zero_correction(data)

    stp_corrections(data)
    absorption_corrections(data)
    scattering_corrections(data)

    for S11, S12, A11, A12, pid, dilution_flow in data.select_multiple(
            {"instrument_id": "S11"},
            {"instrument_id": "S12"},
            {"instrument_id": "A11"},
            {"instrument_id": "A12"},
            {"instrument": "lovepid"},
            {"instrument_id": "Q13"},
            start="2014-05-28", end="2015-09-22T18:45:20Z"
    ):
        dilution(
            (S11, S12, A11, A12),
            (
                {"data": pid, "flow": {"variable_id": "Q_Q11"}},
                {"data": A11, "flow": {"variable_name": "sample_flow"}, "fallback": 0.0},
                {"data": A12, "flow": {"variable_name": "sample_flow"}, "fallback": 0.0},
            ), (
                {"data": dilution_flow, "flow": {"variable_name": "sample_flow"}},
            )
        )

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
