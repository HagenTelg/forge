#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.corrections.filter_absorption import azumi_filter
from forge.processing.station.default.editing import run as default_run


def run(data: AvailableData) -> None:
    for absorption in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    )):
        azumi_filter(absorption)

    default_run(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main

    processing_main(run)
