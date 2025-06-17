#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.corrections.filter_absorption import azumi_filter


def absorption_corrections(data: AvailableData) -> None:
    for absorption in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), start="2017-04-23"):
        azumi_filter(absorption)

    standard_absorption_corrections(data)


def run(data: AvailableData) -> None:
    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
