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
    return (absorption.T / _weiss_factor(transmittance, a, b).T).T


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
        absorption.values = correct_weiss(absorption.values, transmittance.values, a, b)


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
        absorption.values = (absorption.values.T * _weiss_factor(transmittance[...], a, b).T).T


# Wavelength adjustment factor embedded in the original formulation of the constants, which we need to undo
_BOND1999_WAVELENGTH_CORRECTION_FACTOR = 0.97


def correct_bond1999(
        absorption: np.ndarray,
        scattering: np.ndarray,
        k1: typing.Union[float, np.ndarray] = 0.02,
        k2: typing.Union[float, np.ndarray] = 1.22,
) -> np.ndarray:
    k1 = np.asarray(k1)
    k2 = np.asarray(k2)
    return ((absorption.T * _BOND1999_WAVELENGTH_CORRECTION_FACTOR - k1.T * scattering.T) / k2.T).T


def correct_bond1999_extinction(
        absorption: np.ndarray,
        extinction: np.ndarray,
        k1: typing.Union[float, np.ndarray] = 0.02,
        k2: typing.Union[float, np.ndarray] = 1.22,
) -> np.ndarray:
    k1 = np.asarray(k1)
    k2 = np.asarray(k2)
    return ((absorption.T * _BOND1999_WAVELENGTH_CORRECTION_FACTOR - k1.T * extinction.T) / (k2.T - k1.T)).T


def hourly_smoothing(scattering: SelectedVariable) -> np.ndarray:
    from ..derived.average import hourly_average
    return hourly_average(scattering)


def digital_filter_smoothing(scattering: SelectedVariable) -> np.ndarray:
    from ..derived.average import single_pole_low_pass_digital_filter
    return single_pole_low_pass_digital_filter(scattering)


def no_smoothing(scattering: SelectedVariable) -> np.ndarray:
    return scattering.values


def _apply_bond1999_inner(
        absorption: SelectedVariable,
        scattering: typing.Optional[SelectedVariable],
        extinction: typing.Optional[SelectedVariable],
        k1: typing.Union[float, np.ndarray] = 0.02,
        k2: typing.Union[float, np.ndarray] = 1.22,
        smoothing: typing.Callable[[SelectedVariable], np.ndarray] = hourly_smoothing,
        wavelength_adjustment: typing.Optional[AdjustWavelengthParameters] = None,
) -> None:
    if scattering and extinction:
        scattering_corrected = correct_bond1999(
            absorption.values,
            align_wavelengths(scattering, absorption, parameters=wavelength_adjustment,
                              source_values=smoothing(scattering)),
            k1, k2
        )
        extinction_corrected = correct_bond1999_extinction(
            absorption.values,
            align_wavelengths(extinction, absorption, parameters=wavelength_adjustment,
                              source_values=smoothing(extinction)),
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
            align_wavelengths(scattering, absorption, parameters=wavelength_adjustment,
                              source_values=smoothing(scattering)),
            k1, k2
        )
    elif extinction:
        absorption.values = correct_bond1999_extinction(
            absorption.values,
            align_wavelengths(extinction, absorption, parameters=wavelength_adjustment,
                              source_values=smoothing(extinction)),
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
        smoothing: typing.Callable[[SelectedVariable], np.ndarray] = hourly_smoothing,
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

        _apply_bond1999_inner(ba, bs, be, k1, k2, wavelength_adjustment=wavelength_adjustment,
                              smoothing=smoothing)


def bond_1999_coarse(
        absorption,
        scattering_or_extinction,
        k2: typing.Union[float, np.ndarray] = 1.22,
        wavelength_adjustment: typing.Optional[AdjustWavelengthParameters] = None,
        smoothing: typing.Callable[[SelectedVariable], np.ndarray] = hourly_smoothing,
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
                        bs, wavelengths[widx], always_adjacent=False,
                        values=smoothing(bs))
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
                    be_ang = angstrom_exponent_at_wavelength(be, wavelengths[widx], always_adjacent=False,
                                                             values=smoothing(be))
                    be_ang[np.isfinite(angstrom[wavelength_selector])] = angstrom[wavelength_selector]
                    angstrom[wavelength_selector] = be_ang
        except FileNotFoundError:
            be = None

        k1 = np.full_like(ba, 0.0)
        ang_fit = np.invert(np.isnan(angstrom))
        k1[ang_fit] = angstrom[ang_fit] * 0.0334
        k1[angstrom >= 0.6] = 0.02
        k1[angstrom <= 0.2] = 0.00668

        _apply_bond1999_inner(ba, bs, be, k1, k2, wavelength_adjustment=wavelength_adjustment,
                              smoothing=smoothing)


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


def correct_azumi_filter(
        absorption: np.ndarray,
        factor: float = 0.8,
) -> np.ndarray:
    return absorption * factor


def azumi_filter(
        data,
        factor: float = 0.8,
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.azumifilter")

    for absorption in data.select_variable((
            {"variable_name": "light_absorption"},
            {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
            {"standard_name": "volume_extinction_coefficient_in_air_due_to_ambient_aerosol_particles"},
    )):
        absorption[...] *= factor


def spot_area_adjustment(
        data,
        original: typing.Union[float, int, typing.Iterable[float]],
        corrected: typing.Union[float, typing.Iterable[float]],
) -> None:
    data = SelectedData.ensure_data(data)

    if not isinstance(original, float) and not isinstance(original, float):
        original = np.array(original, dtype=np.float64)
    else:
        original = np.array([original], dtype=np.float64)
    if not isinstance(corrected, float) and not isinstance(corrected, float):
        corrected = np.array(corrected, dtype=np.float64)
    else:
        corrected = np.array([corrected], dtype=np.float64)

    def calculate_correction_factor(spot_number) -> typing.Union[float, np.ndarray]:
        spot_number = spot_number[:]

        if corrected.shape != (1,):
            corrected_area = np.full(spot_number.shape, nan, dtype=np.float64)
            spot_in_range = np.logical_and(
                np.isfinite(spot_number),
                spot_number > 0,
                spot_number <= corrected.shape[0],
            )
            spot_index = spot_number[spot_in_range].astype(np.uint32, casting='unsafe', copy=False) - 1
            corrected_area[spot_in_range] = corrected[spot_index]
        else:
            corrected_area = corrected

        if original.shape != (1,):
            original_area = np.full(spot_number.shape, nan, dtype=np.float64)
            spot_in_range = np.logical_and(
                np.isfinite(spot_number),
                spot_number > 0,
                spot_number <= original.shape[0],
            )
            spot_index = spot_number[spot_in_range].astype(np.uint32, casting='unsafe', copy=False) - 1
            original_area[spot_in_range] = original[spot_index]
        else:
            original_area = original

        correction_factor = corrected_area / original_area
        correction_factor[np.invert(np.isfinite(correction_factor))] = 1.0

        return correction_factor

    for value, spot_number in data.select_variable((
            {"variable_name": "light_absorption"},
            {"standard_name": "volume_absorption_coefficient_in_air_due_to_dried_aerosol_particles"},
    ), {"variable_name": "spot_number"}):
        correction_factor = calculate_correction_factor(spot_number)
        value[:] = (value[:].T * correction_factor.T).T

    for value, spot_number in data.select_variable((
            {"variable_name": "path_length_change"},
    ), {"variable_name": "spot_number"}):
        correction_factor = calculate_correction_factor(spot_number)
        value[:] = (value[:].T / correction_factor.T).T
