#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.corrections.filter_absorption import azumi_filter


def run(data: AvailableData) -> None:
    # Various configuration errors and testing have resulted in non-size selected neph data.  Just remove it
    # all to make EBAS happy.
    for aerosol in data.select_instrument((
            {"instrument_id": "S11"},
            {"instrument_id": "A11"},
    ), start="2017-01-01", end="2021-01-01"):
        for var, cut_size in aerosol.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"variable_name": "sample_temperature"},
                {"variable_name": "sample_pressure"},
                {"variable_name": "sample_humidity"},
                {"variable_name": "transmittance"},
        ), {"variable_name": "cut_size"}):
            to_remove = np.invert(np.isfinite(cut_size[...]))
            var[to_remove, ...] = nan

    for absorption in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), start="2025-02-07"):
        azumi_filter(absorption)

    standard_stp_corrections(data)
    standard_absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
