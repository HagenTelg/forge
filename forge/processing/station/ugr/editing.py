#!/usr/bin/env python3
import typing
import numpy as np
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections


def stp_corrections(data: AvailableData) -> None:
    # Legacy MAAP data wasn't STP corrected
    for instrument in data.select_instrument((
            {"instrument_id": "A31"},
    ), end="2013-08-27T16:00:00Z"):
        to_stp(instrument)

    standard_stp_corrections(data)


def absorption_corrections(data: AvailableData) -> None:
    # Legacy ingested PSAP data has no corrections applied (no Weiss)
    standard_absorption_corrections(data, end="2013-08-12T17:00:00Z")

    # CPD2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="2013-08-12T17:00:00Z", end="2014-02-27"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, start="2013-08-12T17:00:00Z", end="2014-02-27"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2014-02-27")


def run(data: AvailableData) -> None:
    stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
