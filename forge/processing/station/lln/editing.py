#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.filter_absorption import spot_area_adjustment
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags
from forge.data.merge.extend import extend_selected


def cpc_corrections(data: AvailableData) -> None:
    # Remove neph zeros from CPC data
    for start, end in (
        ("2010-12-02T07:00:00Z", "2010-12-07T10:28:00Z"),
        ("2011-07-12T04:00:00Z", "2011-07-25T09:25:00Z"),
    ):
        for cpc, neph in data.select_instrument((
                {"instrument_id": "N71"},
        ), {"instrument_id": "S11"}, start=start, end=end):
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
                is_in_zero = extend_selected(is_in_zero, source_flags.times, 1 * 60 * 1000, 1 * 60 * 1000)
                counts[is_in_zero, ...] = nan


def absorption_corrections(data: AvailableData) -> None:
    for absorption in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="2008-10-10", end="2009-06-30"):
        spot_area_adjustment(absorption, 18.34, 17.31)
    for absorption in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="2009-06-30", end="2011-11-09T14:23:15Z"):
        spot_area_adjustment(absorption, 18.34, 18.17)
    for absorption in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="2011-11-09T14:23:15Z", end="2017-05-10T09:11:00Z"):
        spot_area_adjustment(absorption, 19.37, 18.17)

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2017-04-19T11:08:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2017-04-19T11:08:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # PSAP zero filter schedule removal.
    # This removes any PSAP absorptions when the zero flag is set, indicating that the valves where configured so
    # it was sampling filtered air.
    for psap in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="2017-07-25T13:00:00Z", end="2023-12-05T12:23:00Z"):
        for absorption, source_flags in psap.select_variable((
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
        ), {"variable_name": "system_flags"}):
            if not np.issubdtype(source_flags.values.dtype, np.integer):
                continue
            flags = parse_flags(source_flags.variable)
            matched_bits = 0
            for bits, name in flags.items():
                if name != "zero":
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            is_in_zero = np.bitwise_and(source_flags.values, matched_bits) != 0
            absorption[is_in_zero, ...] = nan

    # Normal corrections now
    standard_absorption_corrections(data, start="2017-04-19T11:08:00Z")


def run(data: AvailableData) -> None:
    standard_stp_corrections(data)
    cpc_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
