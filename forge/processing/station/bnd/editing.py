#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.filter_absorption import weiss_undo, spot_area_adjustment
from forge.processing.derived.average import hourly_median
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags, declare_flag
from forge.data.merge.extend import extend_selected


def stp_corrections(data: AvailableData) -> None:
    for instrument in data.select_instrument((
            {"instrument": "mrinephelometer"},
    ), end="1994-10-01"):
        to_stp(instrument)

    # a_e recovered data does not have a neph T/P, so use an assumed one (average of 1995Q4)
    for instrument in data.select_instrument((
            {"instrument": "mrinephelometer"},
    ), start="1994-10-01", end="1995-01-01"):
        to_stp(instrument, temperature=23.9, pressure=983.2)

    for instrument in data.select_instrument((
            {"instrument": "mrinephelometer"},
    ), start="1995-01-01"):
        to_stp(instrument)

    standard_stp_corrections(data)


def cpc_corrections(data: AvailableData) -> None:
    # Remove CPC data because 3-way valve was inadvertently set in neph position. - PJS
    for cpc, neph in data.select_instrument((
            {"instrument_id": "N61"},
    ), {"instrument_id": "S11"}, start="2015-11-01T15:00:03Z", end="2015-11-24T18:08:59Z"):
        for counts in cpc.select_variable((
                {"variable_name": "number_concentration"},
                {"standard_name": "number_concentration_of_ambient_aerosol_particles_in_air"},
        )):
            try:
                source_flags = neph.get_input(counts, {
                    "variable_name": "system_flags",
                })
            except FileNotFoundError:
                continue
            if not np.issubdtype(source_flags.values.dtype, np.integer):
                continue
            flags = parse_flags(source_flags.variable)
            matched_bits = 0
            for bits, name in flags.items():
                if name not in ("zero", "blank", "spancheck"):
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            is_in_zero = np.bitwise_and(source_flags.values, matched_bits) != 0
            counts[is_in_zero, ...] = nan


def absorption_corrections(data: AvailableData) -> None:
    # Extend the zero data removal so that the CLAP doesn't catch the zero filter still being
    # switched (since data will include the partial minute during the switch).
    for clap, neph in data.select_instrument((
            {"instrument": "clap"},
    ), {"instrument_id": "S11"}, start="2010-12-01"):
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
                if name not in ("zero", "blank", "spancheck"):
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            is_in_zero = np.bitwise_and(source_flags.values, matched_bits) != 0
            absorption[is_in_zero, ...] = nan

    # Fix incorrect spot size in CP configuration
    for absorption in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="1996-06-14", end="1997-10-30"):
        spot_area_adjustment(absorption, 18.597, 17.830)
    # 1997-10-30 to 1998-09-21T05:28:02Z ok (17.794 configured)
    for absorption in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="1998-09-21T05:28:02Z", end="2000-01-03T20:19:40Z"):
        spot_area_adjustment(absorption, 17.495, 17.794)

    # Incorrect Weiss constants initially
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="1996-05-06", end="2000-04-12"):
        remove_low_transmittance(absorption)
        weiss_undo(absorption, 0.710, 1.0796)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="2000-04-12", end="2012-03-25T13:46:48Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, start="2000-03-28", end="2012-03-25T13:46:48Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Fix CLAP spot sizes.  The defaults where still in the cpd.ini, so correct to the measured ones.
    # I think the O-rings where also still in place, so the effective size is somewhat approximate.
    for absorption in data.select_instrument((
            {"instrument_id": "A12"},
    ), start="2010-12-02", end="2010-12-03"):
        spot_area_adjustment(
            absorption,
            [17.814, 17.814, 17.814, 17.814, 17.814, 17.814, 17.814, 17.814],
            [23.64987, 24.23951, 24.46931, 24.10947, 26.17945, 27.51016, 27.1806, 26.7495],
        )

    # Normal corrections now
    standard_absorption_corrections(data, start="2012-03-25T13:46:48Z")


def aerosol_contamination(data: AvailableData) -> None:
    # CPC spike detection
    for aerosol, cpc in data.select_instrument((
            {"tags": "aerosol -met", "instrument_id": r"(?!F).+"},
    ), {"instrument_id": "N61"}, start="2016-05-29"):
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

    def remove_contamination(start, end, *remove_flags):
        for aerosol in data.select_instrument((
                {"tags": "aerosol"},
        ), start=start, end=end):
            for system_flags in aerosol.system_flags():
                flags = parse_flags(system_flags.variable)
                matched_bits = 0
                for bits, name in flags.items():
                    for flag in remove_flags:
                        if name.startswith(flag):
                            break
                    else:
                        continue
                    matched_bits |= bits
                if matched_bits == 0:
                    continue
                mask = np.array(matched_bits, dtype=np.uint64)
                mask = np.invert(mask)
                system_flags[:] = system_flags[:] & mask

    # Fix for CPC spike contamination not being correctly released after triggered.
    remove_contamination("2015-03-19T20:18:00Z", "2015-07-10T15:05:00Z", "data_contamination_cpc")


def run(data: AvailableData) -> None:
    aerosol_contamination(data)

    stp_corrections(data)
    cpc_corrections(data)
    absorption_corrections(data)
    # No truncation on MRI neph, but the default excludes it
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
