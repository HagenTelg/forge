#!/usr/bin/env python3
import typing
import numpy as np
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags


def absorption_corrections(data: AvailableData) -> None:
    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2019-06-21T15:05:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2019-06-21T15:05:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2019-06-21T15:05:00Z")


def aerosol_contamination(data: AvailableData) -> None:
    def remove_contamination(start, end, flag="data_contamination_"):
        for aerosol in data.select_instrument((
                {"tags": "aerosol"},
        ), start=start, end=end):
            for system_flags in aerosol.system_flags():
                flags = parse_flags(system_flags.variable)
                matched_bits = 0
                for bits, name in flags.items():
                    if not name.startswith(flag):
                        continue
                    matched_bits |= bits
                if matched_bits == 0:
                    continue
                mask = np.array(matched_bits, dtype=np.uint64)
                mask = np.invert(mask)
                system_flags[:] = system_flags[:] & mask

    # Disable the CN auto contamination flag - sjy
    remove_contamination("2011-08-25T04:10:01Z", "2012-12-05T09:06:50Z")
    # disable CN auto contamination - sjy
    remove_contamination("2014-04-02T04:56:56Z", "2014-04-02T12:54:20Z")
    # disable CN contamination - sjy
    remove_contamination("2014-04-14T00:00:00Z", "2014-04-15T00:00:00Z")
    # CN8000 is too small for WLG, if there are NPF - sjy
    remove_contamination("2014-11-03T00:00:00Z", "2014-11-17T00:00:00Z")


def run(data: AvailableData) -> None:
    aerosol_contamination(data)

    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
