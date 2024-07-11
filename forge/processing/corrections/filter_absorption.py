import typing
import logging
import numpy as np
from math import nan
from ..context import SelectedData, SelectedVariable
from ..derived.angstrom import angstrom_exponent_at_wavelength
from ..derived.wavelength import align_wavelengths, AdjustWavelengthParameters

_LOGGER = logging.getLogger(__name__)


def _weiss_factor(
        transmittance: np.ndarray,
        a: float = 0.814,
        b: float = 1.237,
) -> np.ndarray:
    return a + transmittance * b


def correct_weiss(
        absorption: np.ndarray,
        transmittance: np.ndarray,
        a: float = 0.814,
        b: float = 1.237,
) -> np.ndarray:
    return absorption / _weiss_factor(transmittance, a, b)


def weiss(
        data,
        a: float = 0.814,
        b: float = 1.237,
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.weiss")

    for absorption, transmittance in data.select_variable((
            {"variable_name": "light_absorption"},
            {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
            {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
    ), {"variable_name": "transmittance"}):
        absorption[...] /= _weiss_factor(transmittance[...], a, b)


def weiss_undo(
        data,
        a: float = 0.814,
        b: float = 1.237,
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.undoweiss")

    for absorption, transmittance in data.select_variable((
            {"variable_name": "light_absorption"},
            {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
            {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
    ), {"variable_name": "transmittance"}):
        absorption[...] *= _weiss_factor(transmittance[...], a, b)


# Wavelength adjustment factor embedded in the original formulation of the constants, which we need to undo
_BOND1999_WAVELENGTH_CORRECTION_FACTOR = 0.97


def correct_bond1999(
        absorption: np.ndarray,
        scattering: np.ndarray,
        k1: typing.Union[float, np.ndarray] = 0.02,
        k2: typing.Union[float, np.ndarray] = 1.22,
) -> np.ndarray:
    k1 = np.array(k1, copy=False)
    k2 = np.array(k2, copy=False)
    return ((absorption.T * _BOND1999_WAVELENGTH_CORRECTION_FACTOR - k1.T * scattering.T) / k2.T).T


def correct_bond1999_extinction(
        absorption: np.ndarray,
        extinction: np.ndarray,
        k1: typing.Union[float, np.ndarray] = 0.02,
        k2: typing.Union[float, np.ndarray] = 1.22,
) -> np.ndarray:
    k1 = np.array(k1, copy=False)
    k2 = np.array(k2, copy=False)
    return ((absorption.T * _BOND1999_WAVELENGTH_CORRECTION_FACTOR - k1.T * extinction.T) / (k2.T - k1.T)).T


def _apply_bond1999_inner(
        absorption: SelectedVariable,
        scattering: typing.Optional[SelectedVariable],
        extinction: typing.Optional[SelectedVariable],
        k1: typing.Union[float, np.ndarray] = 0.02,
        k2: typing.Union[float, np.ndarray] = 1.22,
        wavelength_adjustment: typing.Optional[AdjustWavelengthParameters] = None,
) -> None:
    if scattering and extinction:
        scattering_corrected = correct_bond1999(
            absorption.values,
            align_wavelengths(scattering, absorption, parameters=wavelength_adjustment),
            k1, k2
        )
        extinction_corrected = correct_bond1999_extinction(
            absorption.values,
            align_wavelengths(extinction, absorption, parameters=wavelength_adjustment),
            k1, k2
        )

        scattering_invalid = np.isnan(scattering_corrected)
        scattering_valid = np.invert(scattering_invalid)
        absorption[scattering_valid] = scattering_corrected[scattering_valid]
        extinction_invalid = np.isnan(extinction_corrected)
        extinction_valid = np.invert(extinction_invalid)
        extinction_valid = scattering_invalid & extinction_valid
        absorption[extinction_valid] = extinction_corrected[extinction_valid]
        absorption[np.invert(scattering_valid | extinction_valid)] = nan
    elif scattering:
        absorption.values = correct_bond1999(
            absorption.values,
            align_wavelengths(scattering, absorption, parameters=wavelength_adjustment),
            k1, k2
        )
    elif extinction:
        absorption.values = correct_bond1999_extinction(
            absorption.values,
            align_wavelengths(extinction, absorption, parameters=wavelength_adjustment),
            k1, k2
        )
    else:
        _LOGGER.debug("No scattering or extinction available for %s", absorption)
        absorption[:] = nan


def bond_1999(
        absorption,
        scattering_or_extinction,
        k1: typing.Union[float, np.ndarray] = 0.02,
        k2: typing.Union[float, np.ndarray] = 1.22,
        wavelength_adjustment: typing.Optional[AdjustWavelengthParameters] = None,
) -> None:
    absorption = SelectedData.ensure_data(absorption)
    scattering_or_extinction = SelectedData.ensure_data(scattering_or_extinction)
    absorption.append_history("forge.correction.bond1999")

    for ba in absorption.select_variable((
            {"variable_name": "light_absorption"},
            {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
            {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
    )):
        try:
            bs = scattering_or_extinction.get_input(ba, (
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
            ))
        except FileNotFoundError:
            bs = None
        try:
            be = scattering_or_extinction.get_input(ba, (
                {"variable_name": "light_extinction"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
            ))
        except FileNotFoundError:
            be = None

        _apply_bond1999_inner(ba, bs, be, k1, k2, wavelength_adjustment=wavelength_adjustment)


def bond_1999_coarse(
        absorption,
        scattering_or_extinction,
        k2: typing.Union[float, np.ndarray] = 1.22,
        wavelength_adjustment: typing.Optional[AdjustWavelengthParameters] = None,
) -> None:
    absorption = SelectedData.ensure_data(absorption)
    scattering_or_extinction = SelectedData.ensure_data(scattering_or_extinction)
    absorption.append_history("forge.correction.bond1999")

    for ba in absorption.select_variable((
            {"variable_name": "light_absorption"},
            {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
            {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
    )):
        angstrom = np.full_like(ba.values, nan)
        try:
            bs = scattering_or_extinction.get_input(ba, (
                {"variable_name": "scattering_coefficient"},
                {"standard_name": "volume_scattering_coefficient_in_air_due_to_dried_aerosol_particles"},
            ))
            for wavelengths, value_select, _ in ba.select_wavelengths():
                for widx in range(len(wavelengths)):
                    angstrom[value_select[widx]] = angstrom_exponent_at_wavelength(
                        bs, wavelengths[widx], always_adjacent=False)
        except FileNotFoundError:
            bs = None
        try:
            be = scattering_or_extinction.get_input(ba, (
                {"variable_name": "light_extinction"},
                {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
            ))
            for wavelengths, value_select, _ in ba.select_wavelengths():
                for widx in range(len(wavelengths)):
                    wavelength_selector = value_select[widx]
                    be_ang = angstrom_exponent_at_wavelength(be, wavelengths[widx], always_adjacent=False)
                    be_ang[np.isfinite(angstrom[wavelength_selector])] = angstrom[wavelength_selector]
                    angstrom[wavelength_selector] = be_ang
        except FileNotFoundError:
            be = None

        k1 = np.full_like(ba, 0.0)
        ang_fit = np.invert(np.isnan(angstrom))
        k1[ang_fit] = angstrom[ang_fit] * 0.0334
        k1[angstrom >= 0.6] = 0.02
        k1[angstrom <= 0.2] = 0.00668

        _apply_bond1999_inner(ba, bs, be, k1, k2, wavelength_adjustment=wavelength_adjustment)


def remove_low_transmittance(
        data,
        threshold: float = 0.5,
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.removelowtransmittance")

    def select_threshold_wavelength(wavelengths: typing.List[float]) -> int:
        best = 0
        for idx in range(1, len(wavelengths)):
            if abs(wavelengths[idx] - 528) > abs(wavelengths[best] - 528):
                continue
            best = idx
        return best

    for absorption, transmittance in data.select_variable((
            {"variable_name": "light_absorption"},
            {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
            {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
    ), {"variable_name": "transmittance"}):
        if transmittance.has_changing_wavelengths:
            for wavelengths, value_select, _ in transmittance.select_wavelengths():
                if len(wavelengths) <= 1:
                    trigger = transmittance[value_select[0]] < threshold
                else:
                    threshold_wavelength = select_threshold_wavelength(wavelengths)
                    trigger = transmittance[value_select[threshold_wavelength]] < threshold
                for widx in range(len(wavelengths)):
                    absorption[value_select[widx]] = np.where(trigger, nan, absorption[value_select[widx]])
        else:
            threshold_wavelength = select_threshold_wavelength(transmittance.wavelengths)
            trigger = transmittance[..., threshold_wavelength] < threshold
            absorption[trigger] = nan
