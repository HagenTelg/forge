import typing
import numpy as np
from math import nan
from forge.data.structure.stp import standard_temperature, standard_pressure
from ..context import SelectedData, SelectedVariable
from ..derived.angstrom import angstrom_exponent_at_wavelength
from .extensives import write_extensives
from .wavelength import AdjustWavelengthParameters


def _valid_div(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    assert a.shape == b.shape
    result = np.full_like(a, nan)
    valid = np.isfinite(a) & np.isfinite(b) & (b != 0)
    result[valid] = a[valid] / b[valid]
    return result


def calculate_ssa(
        scattering: np.ndarray,
        extinction: np.ndarray,
) -> np.ndarray:
    return _valid_div(scattering, extinction)


def calculate_backscatter_fraction(
        scattering: np.ndarray,
        backscatter: np.ndarray,
) -> np.ndarray:
    return _valid_div(backscatter, scattering)


_ASYMMETRY_POLY = np.polynomial.Polynomial((0.9893, -3.9636, 7.4644, -7.1439))


def calculate_asymmetry_parameter(
        backscatter_fraction: np.ndarray,
) -> np.ndarray:
    return _ASYMMETRY_POLY(backscatter_fraction)


_RFE_BETA = np.polynomial.Polynomial((0.0817, 1.8495, -2.9682))


def calculate_radiative_forcing_efficiency(
        backscatter_fraction: np.ndarray,
        ssa: np.ndarray,
        beta: np.polynomial.Polynomial = _RFE_BETA,
        daylight_fraction: float = 0.5,
        solar_constant: float = 1370.0,
        atmospheric_transmission: float = 0.76,
        cloud_fraction: float = 0.6,
        surface_albedo: float = 0.15,
) -> np.ndarray:
    beta = beta(backscatter_fraction)
    sas = 1.0 - surface_albedo
    sas = sas * sas

    valid =( np.abs(beta) > 1E-10) & (np.abs(ssa) > 1E-10)

    result = np.full_like(ssa, nan)
    result[valid] = (
            -1.0 *
            daylight_fraction *
            solar_constant *
            atmospheric_transmission**2 *
            (1.0 - cloud_fraction) *
            ssa[valid] *
            beta[valid] *
            (sas - (2.0 * surface_albedo / beta[valid] * ((1.0 / ssa[valid]) - 1.0)))
    )
    return result


def generate_intensives(
        intensives: SelectedData,
        cpc,
        scattering,
        absorption,
        extinction=None,
        wavelengths: "typing.Union[typing.List[float], typing.Tuple[float, ...]]" = (450.0, 550.0, 700.0),
        is_stp: bool = True,
        wavelength_adjustment: typing.Optional[AdjustWavelengthParameters] = None,
) -> None:
    intensives = SelectedData.ensure_data(intensives)
    intensives.append_history("forge.intensives")

    if cpc:
        cpc = SelectedData.ensure_data(cpc)
    if scattering:
        scattering = SelectedData.ensure_data(scattering)
    if absorption:
        absorption = SelectedData.ensure_data(absorption)
    if extinction:
        extinction = SelectedData.ensure_data(extinction)

    cpc_var, scattering_var, backscatter_var, absorption_var, extinction_var = write_extensives(
        intensives, cpc, scattering, absorption, extinction, wavelengths,
        is_stp=is_stp,
        wavelength_adjustment=wavelength_adjustment,
    )

    def setup_variable(destination: SelectedVariable) -> None:
        if is_stp:
            standard_temperature(destination.parent)
            standard_pressure(destination.parent)
            ancillary_variables = set(getattr(destination.variable, "ancillary_variables", "").split())
            ancillary_variables.add("standard_pressure")
            ancillary_variables.add("standard_temperature")
            destination.variable.ancillary_variables = " ".join(ancillary_variables)

        destination.variable.cell_methods = "time: mean"

    with intensives.get_output(scattering_var, "backscatter_fraction",
                               wavelength=True) as output_bfr:
        bfr = calculate_backscatter_fraction(
            scattering_var[:],
            intensives.get_input(output_bfr, {"variable_name": "backscattering_coefficient"},
                                 error_when_missing=False).values,
        )
        setup_variable(output_bfr)
        output_bfr.variable.long_name = "ratio of backwards hemispheric light scattering to total light scattering"
        output_bfr.variable.standard_name = "backscattering_ratio"
        output_bfr.variable.units = "1"
        output_bfr.variable.C_format = "%6.3f"
        output_bfr.variable.variable_id = "ZBfr"
        output_bfr[:] = bfr

    with intensives.get_output(scattering_var, "asymmetry_parameter",
                               wavelength=True) as output_g:
        setup_variable(output_g)
        output_g.variable.long_name = "scattering asymmetry parameter derived from the backscatter ratio"
        if not is_stp:
            output_g.variable.standard_name = "asymmetry_factor_of_ambient_aerosol_particles"
        output_g.variable.units = "1"
        output_g.variable.C_format = "%6.3f"
        output_g.variable.variable_id = "ZG"
        output_g[:] = calculate_asymmetry_parameter(bfr)

    with intensives.get_output(scattering_var, "single_scattering_albedo",
                               wavelength=True) as output_ssa:
        ssa = calculate_ssa(
            scattering_var[:],
            intensives.get_input(output_ssa, {"variable_name": "light_extinction"},
                                 error_when_missing=False).values
        )
        setup_variable(output_ssa)
        output_ssa.variable.long_name = "single scattering albedo (ratio of scattering to extinction)"
        if not is_stp:
            output_ssa.variable.standard_name = "single_scattering_albedo_in_air_due_to_ambient_aerosol_particles"
        output_ssa.variable.units = "1"
        output_ssa.variable.C_format = "%6.3f"
        output_ssa.variable.variable_id = "ZSSA"
        output_ssa[:] = ssa

    with intensives.get_output(scattering_var, "radiative_forcing_efficiency",
                               wavelength=True) as output_rfe:
        setup_variable(output_rfe)
        output_rfe.variable.long_name = "derived radiative forcing efficiency"
        output_rfe.variable.C_format = "%7.2f"
        output_rfe.variable.variable_id = "ZRFE"
        output_rfe[:] = calculate_radiative_forcing_efficiency(bfr, ssa)

    if scattering:
        for scattering_in in scattering.select_variable((
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
        ), commit_variable=False, commit_auxiliary=False):
            if not scattering_in.has_multiple_wavelengths:
                continue
            with intensives.get_output(scattering_in, "scattering_angstrom_exponent",
                                       wavelength=True) as output_sae:
                setup_variable(output_sae)
                output_sae.variable.long_name = "scattering Ångström exponent from adjacent wavelengths"
                if not is_stp:
                    output_sae.variable.standard_name = "angstrom_exponent_of_ambient_aerosol_in_air"
                output_sae.variable.units = "1"
                output_sae.variable.C_format = "%6.3f"
                output_sae.variable.variable_id = "ZAngBs"
                for i in range(len(output_sae.wavelengths)):
                    output_sae[output_sae.get_wavelength_index(output_sae.wavelengths[i])] = angstrom_exponent_at_wavelength(
                        scattering_in, output_sae.wavelengths[i],
                        always_adjacent=True,
                    )
            break

    if absorption:
        for absorption_in in absorption.select_variable((
                {"variable_name": "light_absorption"},
                {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
        ), commit_variable=False, commit_auxiliary=False):
            if not absorption_in.has_multiple_wavelengths:
                continue
            with intensives.get_output(absorption_in, "absorption_angstrom_exponent",
                                       wavelength=True) as output_aae:
                setup_variable(output_aae)
                output_aae.variable.long_name = "absorption Ångström exponent from adjacent wavelengths"
                if not is_stp:
                    output_aae.variable.standard_name = "angstrom_exponent_of_ambient_aerosol_in_air"
                output_aae.variable.units = "1"
                output_aae.variable.C_format = "%6.3f"
                output_aae.variable.variable_id = "ZAngBa"
                for i in range(len(output_aae.wavelengths)):
                    output_aae[output_aae.get_wavelength_index(output_aae.wavelengths[i])] = angstrom_exponent_at_wavelength(
                        absorption_in, output_aae.wavelengths[i],
                        always_adjacent=True,
                    )
            break
