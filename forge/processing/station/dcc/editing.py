#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.station.default.editing import standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.corrections import *


def absorption_corrections(data: AvailableData) -> None:
    # PSAP data ingested uses instrument output with Weiss already applied
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)


def run(data: AvailableData) -> None:
    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)
    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main

    processing_main(run)
