#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.station.lookup import station_data
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections


def stp_corrections(data: AvailableData) -> None:
    for instrument in data.select_instrument((
            {"tags": "aerosol size -grimm110xopc"},
    )):
        to_stp(instrument, temperature=12.0,
               pressure=station_data(instrument.station, 'climatology',
                                     'surface_pressure')(instrument.station))

    standard_stp_corrections(data)


def absorption_corrections(data: AvailableData) -> None:
    # CPD3 edit by S.S., no further information
    for instrument in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="2021-04-10T17:11:54Z", end="2021-05-10"):
        for absorption in instrument.select_variable((
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
        )):
            if len(absorption.shape) > 1 and absorption.has_multiple_wavelengths:
                do_remove = np.any(absorption[...] <= 0.0, axis=1)
            else:
                do_remove = absorption[...] <= 0.0
            absorption[do_remove] = nan

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, end="2015-03-25T15:11:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, end="2015-03-25T15:11:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    standard_absorption_corrections(data, start="2015-03-25T15:11:00Z")


def run(data: AvailableData) -> None:
    stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
