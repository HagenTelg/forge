#!/usr/bin/env python3
import typing
import numpy as np
from forge.processing.context import AvailableData
from forge.processing.corrections import *
from forge.processing.station.default.editing import standard_absorption_corrections, standard_scattering_corrections, standard_intensives, standard_meteorological, standard_stp_corrections
from forge.processing.derived.average import hourly_average


def absorption_corrections(data: AvailableData) -> None:
    # CPD1/2 data: already has Weiss applied for PSAPs
    for absorption, scattering in data.select_instrument((
            {"instrument": "psap1w"},
            {"instrument": "psap3w"},
    ), {"tags": "scattering -secondary"}, start="2011-01-24", end="2016-10-05T02:23:00Z"):
        remove_low_transmittance(absorption)
        bond_1999(absorption, scattering)
    for absorption, scattering in data.select_instrument((
            {"instrument": "bmitap"},
            {"instrument": "clap"},
    ), {"tags": "scattering -secondary"}, start="2011-01-24", end="2016-10-05T02:23:00Z"):
        remove_low_transmittance(absorption)
        weiss(absorption)
        bond_1999(absorption, scattering)

    # Normal corrections now
    standard_absorption_corrections(data, start="2016-10-05T02:23:00Z")


def leaky_zero_correction(data: AvailableData) -> None:
    # Fix for leaky neph zeros.  This adds a fraction of the hourly average back into scattering.
    # The full history is described in an email from Mikhail on 2011-06-10.

    def apply(var, wavelength_fraction: typing.Dict[float, float]) -> None:
        smoothed = hourly_average(var)
        smoothed[np.invert(np.isfinite(smoothed))] = 0

        def find_best_fraction(wl) -> float:
            best = None
            best_wl = None
            for cwl, cv in wavelength_fraction.items():
                if best is None:
                    best = cv
                    best_wl = cwl
                    continue
                if abs(cwl - wl) < abs(best_wl - wl):
                    best = cv
                    best_wl = cwl
            return best or 0

        for wavelengths, value_select, _ in var.select_wavelengths():
            for widx in range(len(wavelengths)):
                wavelength_selector = value_select[widx]
                var[wavelength_selector] = var[wavelength_selector] + \
                                           smoothed[wavelength_selector] * find_best_fraction(wavelengths[widx])

    for neph in data.select_instrument((
            {"instrument_id": "S11"}
    ), start="2011-01-22T00:02:00Z", end="2011-03-12T16:02:03Z"):
        for total in neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            apply(total, {
                450.0: 0.06,
                550.0: 0.06,
                700.0: 0.07,
            })
        for back in neph.select_variable((
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            apply(back, {
                450.0: 0.06,
                550.0: 0.06,
                700.0: 0.07,
            })
    for neph in data.select_instrument((
            {"instrument_id": "S11"}
    ), start="2011-03-12T16:02:03Z", end="2011-04-14T13:02:03Z"):
        for total in neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            apply(total, {
                450.0: 0.07,
                550.0: 0.07,
                700.0: 0.07,
            })
        for back in neph.select_variable((
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            apply(back, {
                450.0: 0.07,
                550.0: 0.07,
                700.0: 0.07,
            })
    for neph in data.select_instrument((
            {"instrument_id": "S11"}
    ), start="2011-04-14T13:02:03Z", end="2011-08-08T16:45:21Z"):
        for total in neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            apply(total, {
                450.0: 0.12,
                550.0: 0.12,
                700.0: 0.12,
            })
        for back in neph.select_variable((
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            apply(back, {
                450.0: 0.12,
                550.0: 0.12,
                700.0: 0.12,
            })


def run(data: AvailableData) -> None:
    leaky_zero_correction(data)

    standard_stp_corrections(data)
    absorption_corrections(data)
    standard_scattering_corrections(data)

    standard_intensives(data)
    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
