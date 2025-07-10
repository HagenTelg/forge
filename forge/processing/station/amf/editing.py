#!/usr/bin/env python3
import typing
import numpy as np
from math import nan
from forge.units import ZERO_C_IN_K
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.corrections.filter_absorption import bond_1999_coarse
from forge.processing.station.default.editing import standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.data.flags import parse_flags
from forge.dewpoint import extrapolate_rh


def absorption_corrections(data: AvailableData) -> None:
    # Extend the zero data removal so that the CLAP doesn't catch the zero filter still being
    # switched (since data will include the partial minute during the switch).
    for clap, neph in data.select_instrument((
            {"instrument": "clap"},
    ), {"instrument_id": "S11"}, start="2010-12-01T21:35:00Z"):
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
        bond_1999(absorption, scattering)


def neph_cal_corrections(data: AvailableData) -> None:
    # Imported from corr.amf.conf
    # This adjusts the calibration applied to the neph data by backing out the original Rayleigh, applying a factor
    # then re-applying Rayleigh subtraction

    forward_rayleigh: typing.Dict[float, float] = {
        450.0: 15.0383,
        550.0: 6.6106,
        700.0: 2.483,
    }
    back_rayleigh: typing.Dict[float, float] = {
        450.0: 15.0383 * 0.5,
        550.0: 6.6106 * 0.5,
        700.0: 2.483 * 0.5,
    }

    def apply(instrument_id: str, start, end, adj: typing.Dict[float, float]):
        def find_factor(wl, lookup) -> float:
            best = None
            best_wl = None
            for cwl, cv in lookup.items():
                if best is None:
                    best = cv
                    best_wl = cwl
                    continue
                if abs(cwl - wl) < abs(best_wl - wl):
                    best = cv
                    best_wl = cwl
            return best or 0

        def apply_scattering(scattering, temperature, pressure, rayleigh_factors):
            temperature = np.array(temperature[...], copy=True)
            pressure = np.array(pressure[...], copy=True)

            selected = temperature[...] < 150.0
            temperature[selected] = temperature[selected] + ZERO_C_IN_K
            temperature[temperature < 100.0] = nan
            temperature[temperature > 350.0] = nan

            pressure[pressure < 10.0] = nan
            pressure[pressure > 2000.0] = nan

            density_factor = pressure / temperature

            for wavelengths, value_select, _ in scattering.select_wavelengths():
                for widx in range(len(wavelengths)):
                    rayleigh = find_factor(wavelengths[widx], rayleigh_factors)
                    wavelength_adj = find_factor(wavelengths[widx], adj)

                    rayleigh = np.full((scattering.shape[0],), rayleigh, dtype=np.float64)
                    rayleigh = rayleigh * density_factor

                    wavelength_selector = value_select[widx]
                    scattering[wavelength_selector] = (
                            (scattering[wavelength_selector].T + rayleigh.T) * wavelength_adj -
                            rayleigh.T
                    ).T

        for instrument in data.select_instrument((
                {"instrument_id": instrument_id},
        ), start=start, end=end):
            for scattering, temperature, pressure in instrument.select_variable((
                    {"variable_name": "scattering_coefficient"},
                    {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
            ),
                    {"standard_name": "air_temperature"},
                    {"standard_name": "air_pressure"},
            ):
                apply_scattering(scattering, temperature, pressure, forward_rayleigh)
        for instrument in data.select_instrument((
                {"instrument_id": instrument_id},
        ), start=start, end=end):
            for scattering, temperature, pressure in instrument.select_variable((
                    {"variable_name": "backscattering_coefficient"},
                    {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
            ),
                    {"standard_name": "air_temperature"},
                    {"standard_name": "air_pressure"},
            ):
                apply_scattering(scattering, temperature, pressure, back_rayleigh)


    apply("S11", "2011-06-09", "2012-07-04", {
        450.0: 1.19,
        550.0: 1.273,
        700.0: 1.4366,
    })
    apply("S12", "2011-06-09", "2012-07-04", {
        450.0: 1.16,
        550.0: 1.139,
        700.0: 1.462,
    })


def absorption_humidity_correction(data: AvailableData) -> None:
    # Per AJ on 2014-09-02, all sub-1um data is bad due to humidity effects
    for instruument in data.select_instrument((
            {"instrument_id": "A11"},
    ), start="2014-07-21", end="2014-08-14"):
        for var, cut_size in instruument.select_variable((
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
        ), {"variable_name": "cut_size"}):
            to_remove = cut_size[...] <= 2.5
            var[to_remove, ...] = nan


def recalculate_neph_rh(data: AvailableData) -> None:
    for neph, umac, pid in data.select_instrument((
            {"instrument_id": "S11"},
    ),
            {"instrument_id": "X1"},
            {"instrument_id": "X2"},
            start="2012-07-17", end="2013-06-24",
    ):
        for rh_out, temperature_in in neph.select_variable((
                {"standard_name": "relative_humidity"},
        ),  {"standard_name": "air_temperature"}):
            sensor_temp = umac.get_input(rh_out, {"variable_id": "T_V11"})
            sensor_rh = pid.get_input(rh_out, {"variable_id": "U_V11"})

            rh_out[...] = extrapolate_rh(sensor_temp[...], sensor_rh[...], temperature_in[...])


def run(data: AvailableData) -> None:
    neph_cal_corrections(data)
    absorption_humidity_correction(data)
    recalculate_neph_rh(data)

    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
