#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.corrections.filter_absorption import spot_area_adjustment
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.corrections.filter_absorption import azumi_filter


def absorption_corrections(data: AvailableData) -> None:
    for absorption in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    )):
        azumi_filter(absorption)

    # Apply TAP spot sizes per Keefer email 2023-07-28
    for absorption in data.select_instrument((
            {"instrument_id": "A12"},
    ), start="2023-04-18", end="2023-07-28T16:25:00Z"):
        spot_area_adjustment(
            absorption,
            [25.28] * 8,
            [27.15, 26.97, 26.88, 26.6, 26.15, 25.06, 25.88, 27.29],
        )

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
