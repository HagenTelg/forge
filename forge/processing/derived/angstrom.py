import typing
import numpy as np
from math import log, nan
from forge.processing.context import SelectedVariable
from .wavelength import bracket_wavelength


def calculate_angstrom_exponent(
        a_value: typing.Union[np.ndarray, float],
        a_wavelength: float,
        b_value: typing.Union[np.ndarray, float],
        b_wavelength: float,
) -> np.ndarray:
    a_value = np.array(a_value, copy=False)
    b_value = np.array(b_value, copy=False)

    result = np.full_like(a_value, nan)
    if a_wavelength <= 0 or b_wavelength <= 0 or a_wavelength == b_wavelength:
        return result

    valid = np.all((
        a_value > 0,
        b_value > 0,
    ), axis=0)
    result[valid] = np.log(a_value[valid] / b_value[valid]) / log(b_wavelength / a_wavelength)

    return result


def angstrom_exponent_adjacent(
        variable: SelectedVariable,
        values: typing.Optional[np.ndarray] = None,
) -> np.ndarray:
    if values is None:
        values = variable.values

    result = np.full(values.shape, nan)
    for wavelengths, value_select, _ in variable.select_wavelengths():
        if len(wavelengths) <= 1:
            continue
        for widx in range(len(wavelengths)):
            if widx == 0:
                lower = widx
                upper = widx+1
            elif widx == len(wavelengths)-1:
                lower = widx-1
                upper = widx
            else:
                lower = widx-1
                upper = widx+1

            result[value_select[widx]] = calculate_angstrom_exponent(
                values[value_select[lower]], wavelengths[lower],
                values[value_select[upper]], wavelengths[upper],
            )

    return result


def angstrom_exponent_at_wavelength(
        variable: SelectedVariable,
        target_wavelength: float,
        values: typing.Optional[np.ndarray] = None,
        always_adjacent: bool = True,
) -> np.ndarray:
    if values is None:
        values = variable.values

    result = np.full(variable.times.shape[0], nan)
    for wavelengths, value_select, time_select in variable.select_wavelengths():
        if len(wavelengths) <= 1:
            continue

        first_idx, second_idx = bracket_wavelength(wavelengths, target_wavelength, always_adjacent)
        result[time_select] = calculate_angstrom_exponent(
            values[value_select[first_idx]], wavelengths[first_idx],
            values[value_select[second_idx]], wavelengths[second_idx],
        )

    return result
