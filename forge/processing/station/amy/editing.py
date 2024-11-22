#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_corrections, standard_intensives, standard_meteorological


def run(data: AvailableData) -> None:
    standard_corrections(data)

    for S11, A11, pid, dilution_flow in data.select_multiple(
            {"instrument_id": "S11"},
            {"instrument_id": "A11"},
            {"instrument": "lovepid"},
            {"instrument_id": "Q12"},
            end="2022-03-04",  # Per email on 2022-06-27, dilution system removed until further notice
    ):
        dilution(
            (S11, A11),
            (
                {"data": pid, "flow": {"variable_id": "Q_Q11"}},
                {"data": A11, "flow": {"variable_name": "sample_flow"}, "fallback": 0.0},
            ), (
                {"data": dilution_flow, "flow": {"variable_name": "sample_flow"}},
            )
        )

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
