 #!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.filter_absorption import weiss_undo
from forge.processing.corrections.climatology import vaisala_hmp_limits
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags


def absorption_corrections(data: AvailableData) -> None:
    # Incorrect Weiss constants initially
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="1997-10-06", end="2000-03-28"):
        remove_low_transmittance(absorption)
        weiss_undo(absorption, 0.710, 1.0796)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="2000-03-28", end="2016-08-18T17:52:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, start="2000-03-28", end="2016-08-18T17:52:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal CPD3 data up until building comparison
    standard_absorption_corrections(data, start="2016-08-18T17:52:00Z", end="2020-10-22")

    # New building comparison: make sure the scattering sources match
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap", "tags": "-secondary"},
            {"instrument": "clap", "tags": "-secondary"},
    ), {"tags": "scattering -secondary"}, start="2020-10-22", end="2022-01-20"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument_id": "A91"},
    ), {"instrument_id": "S91"}, start="2020-10-22", end="2022-01-20"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2022-01-20")

    # Extend the zero data removal so that the CLAP doesn't catch the zero filter still being
    # switched (since data will include the partial minute during the switch).
    for clap, neph in data.select_instrument((
            {"instrument_id": "A11"},
    ), {"instrument_id": "S11"}, start="2014-08-15T20:43:00Z"):
        for absorption in data.select_variable((
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
        )):
            source_flags = neph.get_input(absorption, {
                "variable_name": "system_flags",
            })
            if not np.issubdtype(source_flags.values.dtype, np.integer):
                continue
            flags = parse_flags(source_flags.variable)
            matched_bits = 0
            for bits, name in flags.items():
                if name not in ("zero", "blank", "spancheck"):
                    continue
                matched_bits |= bits
            if matched_bits == 0:
                continue
            is_in_zero = np.bitwise_and(source_flags.values, matched_bits) != 0
            absorption[is_in_zero, ...] = nan



def scattering_corrections(data: AvailableData) -> None:
    for scattering in data.select_instrument({"instrument": "mrinephelometer"}, end="1997-10-06"):
        anderson_ogren_1998(scattering)
    standard_scattering_corrections(data)


def aerosol_contamination(data: AvailableData) -> None:
    for aerosol, wind in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="1994-04-13", end="2011-09-23T14:20:00Z",
    ):
        wind_sector_contamination(
            aerosol, wind,
            contaminated_sector=((130, 360),),
            contaminated_minimum_speed=0.5,
        )

    for aerosol, wind_realtime, wind_met in data.select_instrument(
            {"tags": "aerosol -met"},
            {"instrument_id": "XM2"},
            {"instrument_id": "XM1"},
            always_tuple=True,
            start="2011-09-23T14:20:00Z",
    ):
        wind_sector_contamination(
            aerosol, wind_realtime, wind_met,
            contaminated_sector=((130, 360),),
            contaminated_minimum_speed=0.5,
            extend_before_ms=20 * 60 * 1000,
            extend_after_ms=20 * 60 * 1000,
        )


def run(data: AvailableData) -> None:
    for met in data.select_instrument({"instrument_id": "XM1"}, start="2007-01-01"):
        meteorological_climatology_limits(
            met,
            temperature_range=(-55, 25),
            dewpoint_range=(-65, 23),
            pressure_range=(965, 1060),
            normalized_temperature_rate_of_change=(-0.05, 0.05),
        )

    aerosol_contamination(data)

    # MRI neph data is already STP corrected, but the default excludes it anyway
    standard_stp_corrections(data)
    absorption_corrections(data)
    scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)

    for met in data.select_instrument({"instrument_id": "XM1"}, start="2017-07-03"):
        vaisala_hmp_limits(met)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
