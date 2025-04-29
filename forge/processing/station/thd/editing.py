#!/usr/bin/env python3
import typing
import numpy as np
import forge.data.structure.variable as netcdf_var
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags


def absorption_corrections(data: AvailableData) -> None:
    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2016-02-19T16:52:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2016-02-19T16:52:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2016-02-19T16:52:00Z")


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
    # Copy aerosol winds before OBOP system was installed
    for met, umac in data.derive_output(
            "XM1",
            {"instrument_id": "X1"},
            tags=("met", ),
            start="2002-04-10", end="2007-01-11",
    ):
        for WD, WS in umac.select_variable(
                {"variable_name": "WD_X1"},
                {"variable_name": "WS_X1"},
                commit_variable=False, commit_auxiliary=False):
            with met.get_output(WD, "wind_speed") as wind_speed:
                netcdf_var.variable_wind_speed(wind_speed.variable)
                wind_speed.variable.variable_id = "WS"
                wind_speed.variable.coverage_content_type = "physicalMeasurement"
                wind_speed.variable.cell_methods = f"time: mean wind_direction: vector_direction"
                wind_speed[:] = WS[:]
            with met.get_output(WD, "wind_direction") as wind_direction:
                netcdf_var.variable_wind_speed(wind_direction.variable)
                wind_direction.variable.variable_id = "WD"
                wind_direction.variable.coverage_content_type = "physicalMeasurement"
                wind_direction.variable.cell_methods = f"time: mean wind_speed: vector_magnitude"
                wind_direction[:] = WD[:]

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
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
