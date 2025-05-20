#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections


def absorption_corrections(data: AvailableData) -> None:
    standard_absorption_corrections(data, end="2019-01-01")

    # Neph invalid, use SMPS scattering
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"instrument_id": "N81"}, start="2019-01-01", end="2019-04-24"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    standard_absorption_corrections(data, start="2019-04-24")


def run(data: AvailableData) -> None:
    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
