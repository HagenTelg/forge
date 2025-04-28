#!/usr/bin/env python3
import typing
import numpy as np
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags, declare_flag


def aerosol_contamination(data: AvailableData) -> None:
    def remove_contamination(start, end, *remove_flags):
        for aerosol in data.select_instrument((
                {"tags": "aerosol"},
        ), start=start, end=end):
            for system_flags in aerosol.system_flags():
                flags = parse_flags(system_flags.variable)
                matched_bits = 0
                for bits, name in flags.items():
                    for flag in remove_flags:
                        if name.startswith(flag):
                            break
                    else:
                        continue
                    matched_bits |= bits
                if matched_bits == 0:
                    continue
                mask = np.array(matched_bits, dtype=np.uint64)
                mask = np.invert(mask)
                system_flags[:] = system_flags[:] & mask

    # getting rid of wind flagging - EJA
    remove_contamination("2002-03-31", "2003-10-12", "data_contamination_")


def run(data: AvailableData) -> None:
    for met in data.select_instrument({"instrument_id": "XM1"}, start="2007-01-11"):
        meteorological_climatology_limits(
            met,
            temperature_range=(-5, 30),
            dewpoint_range=(-10, 20),
            pressure_range=(960, 1025),
            precipitation_range=(0, 200),
            normalized_temperature_rate_of_change=(-0.05, 0.05),
            normalized_humidity_rate_of_change=(-0.016667, 0.016667),
            maximum_wind_speed=30.0,
        )

    aerosol_contamination(data)

    standard_stp_corrections(data)
    standard_absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
