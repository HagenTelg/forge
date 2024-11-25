#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_intensives, standard_meteorological


def run(data: AvailableData) -> None:
    # Old data ingest is "already corrected"
    standard_corrections(data, start="2018-01-01")

    for S11, A11, A12, N71, N11, pid, dilution_flow in data.select_multiple(
            {"instrument_id": "S11"},
            {"instrument_id": "A11"},
            {"instrument_id": "A12"},
            {"instrument_id": "N71"},
            {"instrument_id": "N11"},
            {"instrument": "lovepid"},
            {"instrument_id": "Q12"},
            start="2019-07-31T21:00:00Z", end="2019-10-08"
    ):
        dilution(
            (S11, A11, A12, N11, N71),
            (
                {"data": pid, "flow": {"variable_id": "Q_Q11"}},
                {"data": A11, "flow": {"variable_name": "sample_flow"}},
                {"data": A12, "flow": {"variable_name": "sample_flow"}},
                {"data": N71, "flow": {"variable_name": "sample_flow"}, "fallback": 0.594},
                {"data": N11, "flow": {"variable_name": "sample_flow"}},
                0.4,  # SMPS
            ), (
                {"data": dilution_flow, "flow": {"variable_name": "sample_flow"}},
            )
        )

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
