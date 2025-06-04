#!/usr/bin/env python3
import typing
from forge.processing.context import AvailableData
from forge.processing.station.default.editing import standard_corrections, standard_meteorological
from forge.processing.derived.intensives import generate_intensives, AdjustWavelengthParameters
from forge.processing.derived.wavelength import align_wavelengths


def leaky_zero_correction(data: AvailableData) -> None:
    # bad valve - EJA

    def apply(scattering, wall, zero_replacement: typing.Dict[float, float]) -> None:
        def find_zero(wl) -> float:
            best = None
            best_wl = None
            for cwl, cv in zero_replacement.items():
                if best is None:
                    best = cv
                    best_wl = cwl
                    continue
                if abs(cwl - wl) < abs(best_wl - wl):
                    best = cv
                    best_wl = cwl
            return best or 0

        original_wall = align_wavelengths(wall, scattering)

        for wavelengths, value_select, _ in scattering.select_wavelengths():
            for widx in range(len(wavelengths)):
                wavelength_selector = value_select[widx]
                scattering[wavelength_selector] = scattering[wavelength_selector] + \
                                                  original_wall[wavelength_selector] - \
                                                  find_zero(wavelengths[widx])

    for neph in data.select_instrument((
            {"instrument_id": "S41"}
    ), start="2015-11-25T19:13:00Z", end="2016-01-01"):
        for total in neph.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            wall = neph.get_input(total, {"variable_name": "wall_scattering_coefficient"})
            apply(total, wall, {
                450.0: 5.44,
                550.0: 5.83,
                700.0: 8.1,
            })
        for back in neph.select_variable((
                {"variable_name": "backscattering_coefficient"},
                {"standard_name": "volume_backwards_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        )):
            wall = neph.get_input(back, {"variable_name": "wall_backscattering_coefficient"})
            apply(back, wall, {
                450.0: 3.27,
                550.0: 3.27,
                700.0: 5.89,
            })


def run(data: AvailableData) -> None:
    leaky_zero_correction(data)
    standard_corrections(data)

    for intensives, scattering, absorption, cpc in data.derive_output(
            "XI",
            {"tags": "scattering -secondary"},
            {"tags": "absorption -secondary"},
            {"tags": "cpc -secondary"},
            tags=("aerosol", "intensives"),
    ):
        generate_intensives(intensives, cpc, scattering, absorption, wavelength_adjustment=AdjustWavelengthParameters(
            fallback_angstrom_exponent=1.0,
        ))

    standard_meteorological(data)


if __name__ == '__main__':
    from forge.processing.context import processing_main
    processing_main(run)
