#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.processing.station.lookup import station_data
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.selections import VOLUME_DEPENDENT_MEASUREMENTS
from forge.processing.corrections.stp import correct_optical, standard_temperature, standard_pressure
from forge.processing.corrections.filter_absorption import spot_area_adjustment, bond_1999_coarse
from forge.processing.station.default.editing import standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags


def stp_corrections(data: AvailableData) -> None:
    for instrument in data.select_instrument((
            {"instrument": "bmi1710cpc"},
            {"instrument": "tsi302xcpc"},
            {"instrument": "tsi375xcpc"},
            {"instrument": "tsi377xcpc"},
            {"instrument": "tsi3010cpc"},
            {"instrument": "tsi3760cpc"},
            {"instrument": "tsi3781cpc"},
            {"instrument": "gerichcpc"},
    ), end="2005-04-01"):
        to_stp(instrument, temperature=12.0,
               pressure=station_data(instrument.station, 'climatology',
                                     'surface_pressure')(instrument.station))
    for instrument in data.select_instrument((
            {"instrument": "admagic200cpc"},
            {"instrument": "admagic250cpc"},
            {"instrument": "bmi1720cpc"},
            {"instrument": "tsi3783cpc"},
    ), end="2005-04-01"):
        to_stp(instrument, temperature={"variable_name": "optics_temperature"})
    for instrument in data.select_instrument((
            {"instrument": "aerodynecaps"},
            {"instrument": "dmtpax"},
            {"instrument": "teledynet640"},
            {"instrument": "tsi3563nephelometer", "tags": "-secondary"},
    ), end="2005-04-01"):
        to_stp(instrument)

    # Use the dry neph pressure since the wet pressure is unreliable (messy om_ records only)
    for wet_neph, ref_neph in data.select_instrument((
            {"instrument": "tsi3563nephelometer", "tags": "secondary"},
    ), {"instrument": "tsi3563nephelometer", "tags": "-secondary"}, end="2005-04-01"):
        for optical, temperature in wet_neph.select_variable(
                VOLUME_DEPENDENT_MEASUREMENTS,
                {"standard_name": "air_temperature"},
                always_tuple=True
        ):
            pressure = ref_neph.get_input(optical, {"standard_name": "air_pressure"})
            optical.values = correct_optical(optical.values, temperature, pressure)

            standard_temperature(optical.parent)
            standard_pressure(optical.parent)
            ancillary_variables = set(getattr(optical.variable, "ancillary_variables", "").split())
            ancillary_variables.add("standard_pressure")
            ancillary_variables.add("standard_temperature")
            optical.variable.ancillary_variables = " ".join(ancillary_variables)

            if optical.standard_name == "number_concentration_of_ambient_aerosol_particles_in_air":
                optical.standard_name = "number_concentration_of_aerosol_particles_at_stp_in_air"
            elif optical.standard_name == "volume_scattering_coefficient_in_air_due_to_ambient_aerosol_particles":
                optical.standard_name = None
            elif optical.standard_name == "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles":
                optical.standard_name = None

    standard_stp_corrections(data, start="2005-04-01")


def absorption_corrections(data: AvailableData) -> None:
    for absorption in data.select_instrument((
            {"instrument_id": "A11"},
    ), end="2005-04-01"):
        spot_area_adjustment(absorption, 21.29, 21.29*1.2145)
    for absorption in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="2005-04-01", end="2007-05-23T02:12:00Z"):
        spot_area_adjustment(absorption, 1.0, 1.0692)

    # Extend the zero data removal so that the CLAP doesn't catch the zero filter still being
    # switched (since data will include the partial minute during the switch).
    for clap, neph in data.select_instrument((
            {"instrument": "clap"},
    ), {"instrument_id": "S11"}, start="2011-03-09"):
        for absorption in clap.select_variable((
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
        )):
            try:
                source_flags = neph.get_input(absorption, {
                    "variable_name": "system_flags",
                })
            except FileNotFoundError:
                continue
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

    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}):
        remove_low_transmittance(absorption)
        bond_1999_coarse(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999_coarse(absorption, scattering)


def scattering_corrections(data: AvailableData) -> None:
    # Note, the leakfix.01 and leakfix.02 are not applied (since they haven't been ported)

    def wet_neph_loss_correction(start, end, slope_coarse: float, slope_fine: float = None):
        if not slope_fine:
            slope_fine = slope_coarse
        for scattering in data.select_instrument((
                {"instrument_id": "S12"},
        ), start=start, end=end):
            for value in scattering.select_variable((
                    {"variable_name": "scattering_coefficient"},
                    {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
                    {"variable_name": "backscattering_coefficient"},
                    {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
            )):
                if slope_fine == slope_coarse:
                    value[:, ...] = value[:, ...] * slope_coarse
                else:
                    fine_data = value.get_cut_size_index(lambda x: x <= 2.5)
                    coarse_data = value.get_cut_size_index(lambda x: np.invert(x <= 2.5))
                    value[fine_data] = value[fine_data] * slope_fine
                    value[coarse_data] = value[coarse_data] * slope_coarse

    wet_neph_loss_correction("1998-12-18T16:48:00Z", "2000-01-18T04:48:00Z", 1.133)
    wet_neph_loss_correction("2000-01-18T04:48:00Z", "2000-01-29T07:12:00Z", 1.176)
    wet_neph_loss_correction("2000-01-29T07:12:00Z", "2000-07-03T19:12:00Z", 1.250)
    wet_neph_loss_correction("2000-07-03T19:12:00Z", "2001-02-16T12:00:00Z", 1.333, 1.613)
    wet_neph_loss_correction("2001-02-16T12:00:00Z", "2001-10-05T16:48:00Z", 1.14, 1.20)
    wet_neph_loss_correction("2001-10-05T16:48:00Z", "2001-10-27T14:24:00Z", 1.32, 1.36)
    wet_neph_loss_correction("2001-10-27T14:24:00Z", "2002-04-01T00:00:00Z", 1.15, 1.20)

    standard_scattering_corrections(data)


def recalculate_ccn(data: AvailableData) -> None:
    for ccn in data.select_instrument((
            {"instrument_id": "N11"},
    ), start="2007-05-22T22:00:00Z", end="2007-10-23T15:00:00Z"):
        for total_conc, bin_conc, flow in ccn.select_variable((
                {"variable_name": "number_concentration"},
        ),
                {"variable_name": "number_distribution"},
                {"variable_name": "sample_flow"},
                always_tuple=True
        ):
            if len(bin_conc.shape) != 2:
                total_conc[:] = nan
                continue
            total_conc[:] = np.sum(bin_conc[:], axis=1) * flow[:] * (60.0 / 1000.0)


def run(data: AvailableData) -> None:
    recalculate_ccn(data)

    stp_corrections(data)
    absorption_corrections(data)
    scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
