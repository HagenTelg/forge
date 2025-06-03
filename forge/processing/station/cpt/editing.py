#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags
from forge.data.merge.extend import extend_selected


def absorption_corrections(data: AvailableData) -> None:
    # Extend the zero data removal so that the CLAP doesn't catch the zero filter still being
    # switched (since data will include the partial minute during the switch).
    for clap, neph in data.select_instrument((
            {"instrument": "clap"},
    ), {"instrument_id": "S11"}, start="2011-07-20", end="2017-05-25"):
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
                if name not in ("zero", ):
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            is_in_zero = np.bitwise_and(source_flags.values, matched_bits) != 0
            is_in_zero = extend_selected(is_in_zero, source_flags.times, 2 * 60 * 1000, 2 * 60 * 1000)
            absorption[is_in_zero, ...] = nan

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2017-05-25"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2017-05-25"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2017-05-25")



def run(data: AvailableData) -> None:
    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
