#!/usr/bin/env python3
import typing
from forge.processing.station.lookup import station_data
from forge.processing.derived.intensives import generate_intensives
from forge.processing.context import AvailableData
from forge.processing.corrections import *


def standard_stp_corrections(data: AvailableData) -> None:
    for instrument in data.select_instrument((
            {"instrument": "bmi1710cpc"},
            {"instrument": "tsi302xcpc"},
            {"instrument": "tsi375xcpc"},
            {"instrument": "tsi377xcpc"},
            {"instrument": "tsi3010cpc"},
            {"instrument": "tsi3760cpc"},
            {"instrument": "tsi3781cpc"},
    )):
        to_stp(instrument, temperature=12.0,
               pressure=station_data(instrument.station, 'climatology',
                                     'surface_pressure')(instrument.station))
    for instrument in data.select_instrument((
            {"instrument": "admagic200cpc"},
            {"instrument": "admagic250cpc"},
            {"instrument": "bmi1720cpc"},
            {"instrument": "tsi3783cpc"},
    )):
        to_stp(instrument, temperature={"variable_name": "optics_temperature"})
    for instrument in data.select_instrument((
            {"instrument": "teledynet640"},
            {"instrument": "tsi3563nephelometer"},
    )):
        to_stp(instrument)


def standard_absorption_corrections(data: AvailableData) -> None:
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)


def standard_scattering_corrections(data: AvailableData) -> None:
    for scattering in data.select_instrument({"instrument": "tsi3563nephelometer"}):
        anderson_ogren_1998(scattering)
    for scattering in data.select_instrument({"instrument": "ecotechnephelometer"}):
        mueller_2011(scattering)


def standard_corrections(data: AvailableData) -> None:
    standard_stp_corrections(data)
    standard_absorption_corrections(data)
    standard_scattering_corrections(data)


def standard_intensives(data: AvailableData) -> None:
    for intensives, scattering, absorption, cpc in data.derive_output(
            "XI",
            {"tags": "scattering -secondary"},
            {"tags": "absorption -secondary -aethalometer -thermomaap"},
            {"tags": "cpc -secondary"},
            tags=("aerosol", "intensives"),
    ):
        generate_intensives(intensives, cpc, scattering, absorption)


def run(data: AvailableData) -> None:
    standard_corrections(data)
    standard_intensives(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
