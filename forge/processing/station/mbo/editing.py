#!/usr/bin/env python3
import typing
import datetime
from forge.processing.station.lookup import station_data
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_corrections, standard_intensives


# Old data ingest is "already corrected"
_LEGACY_DATA_END = datetime.datetime(2018, 1, 1, tzinfo=datetime.timezone.utc)


def stp_corrections(data: AvailableData) -> None:
    for instrument in data.select_instrument((
            {"instrument": "bmi1710cpc"},
            {"instrument": "tsi302xcpc"},
            {"instrument": "tsi375xcpc"},
            {"instrument": "tsi377xcpc"},
            {"instrument": "tsi3010cpc"},
            {"instrument": "tsi3760cpc"},
            {"instrument": "tsi3781cpc"},
    ), start=_LEGACY_DATA_END):
        to_stp(instrument, temperature=12.0,
               pressure=station_data(instrument.station, 'climatology',
                                     'surface_pressure')(instrument.station))
    for instrument in data.select_instrument((
            {"instrument": "admagic200cpc"},
            {"instrument": "admagic250cpc"},
            {"instrument": "bmi1720cpc"},
            {"instrument": "tsi3783cpc"},
    ), start=_LEGACY_DATA_END):
        to_stp(instrument, temperature={"variable_name": "optics_temperature"})
    for instrument in data.select_instrument((
            {"instrument": "teledynet640"},
            {"instrument": "tsi3563nephelometer"},
    ), start=_LEGACY_DATA_END):
        to_stp(instrument)


def absorption_corrections(data: AvailableData) -> None:
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start=_LEGACY_DATA_END):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)


def scattering_corrections(data: AvailableData) -> None:
    for scattering in data.select_instrument({"instrument": "tsi3563nephelometer"}, start=_LEGACY_DATA_END):
        anderson_ogren_1998(scattering)
    for scattering in data.select_instrument({"instrument": "ecotechnephelometer"}, start=_LEGACY_DATA_END):
        mueller_2011(scattering)


def run(data: AvailableData) -> None:
    stp_corrections(data)
    absorption_corrections(data)
    scattering_corrections(data)

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


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
