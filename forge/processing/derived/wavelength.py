import typing
import numpy as np
from math import nan, log
from forge.processing.context import SelectedVariable


def bracket_wavelength(
        wavelengths: typing.List[float],
        target_wavelength: float,
        always_adjacent: bool = True,
        allow_exact: bool = False,
) -> typing.Tuple[int, int]:
    # Only one possibility
    if len(wavelengths) <= 2:
        return 0, 1

    # Outside the covered wavelengths, so use the outermost
    if target_wavelength <= wavelengths[0]:
        return 0, 1
    if target_wavelength >= wavelengths[-1]:
        return len(wavelengths) - 2, len(wavelengths) - 1

    closest_index = 0
    for i in range(1, len(wavelengths)):
        if abs(wavelengths[i] - target_wavelength) >= abs(wavelengths[closest_index] - target_wavelength):
            continue
        closest_index = i

    # Closest to an endpoint, so use the outer wavelengths
    if closest_index == 0:
        return 0, 1
    if closest_index == len(wavelengths) - 1:
        return len(wavelengths) - 2, len(wavelengths) - 1
    if always_adjacent or (wavelengths[closest_index] == target_wavelength and not allow_exact):
        return closest_index - 1, closest_index + 1

    if target_wavelength >= wavelengths[closest_index]:
        return closest_index, closest_index + 1
    else:
        return closest_index - 1, closest_index


def _linear_interpolate(
        a_value: typing.Union[np.ndarray, float],
        a_wavelength: float,
        b_value: typing.Union[np.ndarray, float],
        b_wavelength: float,
        output_wavelength: float,
) -> np.ndarray:
    return a_value + (b_value - a_value) * (output_wavelength - a_wavelength) / (b_wavelength - a_wavelength)


def _log_log_interpolate(
        a_value: typing.Union[np.ndarray, float],
        a_wavelength: float,
        b_value: typing.Union[np.ndarray, float],
        b_wavelength: float,
        output_wavelength: float,
) -> np.ndarray:
    a_value = np.log(a_value)
    b_value = np.log(b_value)
    log_slope = (log(output_wavelength / a_wavelength) / log(b_wavelength / a_wavelength))
    return np.exp(a_value + (b_value - a_value) * log_slope)


def wavelength_interpolate(
        a_value: typing.Union[np.ndarray, float],
        a_wavelength: float,
        b_value: typing.Union[np.ndarray, float],
        b_wavelength: float,
        output_wavelength: float,
) -> np.ndarray:
    a_value = np.asarray(a_value)
    if a_wavelength == output_wavelength:
        return np.array(a_value)
    b_value = np.asarray(b_value)
    if b_wavelength == output_wavelength:
        return np.array(b_value)

    if a_wavelength <= 0 or b_wavelength <= 0 or output_wavelength <= 0:
        return np.full_like(a_value, nan)

    log_valid = (a_value > 0) & (b_value > 0)
    result = np.empty_like(a_value)
    result[log_valid] = _log_log_interpolate(
        a_value[log_valid], a_wavelength,
        b_value[log_valid], b_wavelength,
        output_wavelength
    )
    linear_valid = np.invert(log_valid)
    result[linear_valid] = _linear_interpolate(
        a_value[linear_valid], a_wavelength,
        b_value[linear_valid], b_wavelength,
        output_wavelength
    )
    return result


def wavelength_extrapolate(
        input_value: typing.Union[np.ndarray, float],
        input_wavelength: float,
        angstrom_exponent: float,
        output_wavelength: float,
) -> np.ndarray:
    input_value = np.asarray(input_value)
    if input_wavelength == output_wavelength:
        return np.array(input_value)
    if input_wavelength <= 0 or output_wavelength <= 0 or angstrom_exponent <= 0:
        return np.full_like(input_value, nan)

    return input_value * ((input_wavelength/output_wavelength) ** angstrom_exponent)


class AdjustWavelengthParameters:
    def __init__(
            self,
            fixed_angstrom_exponent: "typing.Optional[typing.Union[float, typing.List[float], typing.Tuple[float, ...]]]" = None,
            fallback_angstrom_exponent: "typing.Optional[typing.Union[float, typing.List[float], typing.Tuple[float, ...]]]" = None,
            maximum_wavelength_interpolation: typing.Optional[float] = None,
            maximum_angstrom_extrapolation: typing.Optional[float] = None,
    ):
        self.fixed_angstrom_exponent = fixed_angstrom_exponent
        self.fallback_angstrom_exponent = fallback_angstrom_exponent
        self.maximum_wavelength_interpolation = maximum_wavelength_interpolation
        self.maximum_angstrom_extrapolation = maximum_angstrom_extrapolation


def _adjust_single_wavelength(
        input_values: np.ndarray,
        input_wavelengths: "typing.Union[typing.List[float], typing.Tuple[float, ...]]",
        output_wavelength: float,
        parameters: typing.Optional[AdjustWavelengthParameters] = None,
) -> np.ndarray:
    assert len(input_wavelengths) > 0

    if len(input_wavelengths) == 1:
        if input_wavelengths[0] == output_wavelength:
            return input_values[..., 0]
        single_angstrom = None
        if parameters is not None:
            if parameters.fixed_angstrom_exponent and parameters.fixed_angstrom_exponent > 0.0:
                single_angstrom = parameters.fixed_angstrom_exponent
            elif parameters.fallback_angstrom_exponent and parameters.fallback_angstrom_exponent > 0.0:
                single_angstrom = parameters.fallback_angstrom_exponent
            if parameters.maximum_angstrom_extrapolation and abs(input_wavelengths[0] - output_wavelength) > parameters.maximum_angstrom_extrapolation:
                return np.full(input_values.shape[:-1], nan)
        if not single_angstrom:
            return np.full(input_values.shape[:-1], nan)
        return wavelength_extrapolate(input_values[..., 0], input_wavelengths[0], single_angstrom, output_wavelength)

    first, second = bracket_wavelength(input_wavelengths, output_wavelength, always_adjacent=False, allow_exact=True)

    fist_distance = abs(input_wavelengths[first] - output_wavelength)
    second_distance = abs(input_wavelengths[second] - output_wavelength)
    if fist_distance < second_distance:
        closest = first
        closest_distance = fist_distance
    else:
        closest = second
        closest_distance = fist_distance

    if closest_distance == 0.0:
        return input_values[..., closest]

    if parameters is not None and parameters.fixed_angstrom_exponent and parameters.fixed_angstrom_exponent > 0.0:
        single_angstrom = parameters.fixed_angstrom_exponent
        if parameters.maximum_angstrom_extrapolation and closest_distance > parameters.maximum_angstrom_extrapolation:
            return np.full(input_values.shape[:-1], nan)
        return wavelength_extrapolate(
            input_values[..., closest], input_wavelengths[closest],
            single_angstrom, output_wavelength
        )

    def can_interpolate() -> bool:
        if parameters is None:
            return True
        if parameters.maximum_wavelength_interpolation and closest_distance > parameters.maximum_wavelength_interpolation:
            return False
        return True

    if can_interpolate():
        result = wavelength_interpolate(
            input_values[..., first], input_wavelengths[first],
            input_values[..., second], input_wavelengths[second],
            output_wavelength
        )
    else:
        result = np.full(input_values.shape[:-1], nan)

    if parameters is not None and parameters.fallback_angstrom_exponent and parameters.fallback_angstrom_exponent > 0.0:
        single_angstrom = parameters.fallback_angstrom_exponent
        if parameters.maximum_angstrom_extrapolation and closest_distance > parameters.maximum_angstrom_extrapolation:
            return result
        fallback_apply = np.isnan(result)
        result[fallback_apply] = wavelength_extrapolate(
            input_values[fallback_apply, closest], input_wavelengths[closest],
            single_angstrom, output_wavelength
        )

    return result


def adjust_wavelengths(
        data: SelectedVariable,
        target_wavelengths: "typing.Union[typing.List[float], typing.Tuple[float, ...]]",
        parameters: typing.Optional[AdjustWavelengthParameters] = None,
        values: typing.Optional[np.ndarray] = None,
) -> np.ndarray:
    if not data.has_changing_wavelengths:
        if target_wavelengths == data.wavelengths:
            return np.array(data.values if values is None else values)
        if not data.wavelengths:
            return np.full((*data.shape[:-1], len(target_wavelengths)), nan)

        result = np.empty((*data.shape[:-1], len(target_wavelengths)), dtype=np.float64)
        for output_idx in range(len(target_wavelengths)):
            result[..., output_idx] = _adjust_single_wavelength(
                data.values if values is None else values,
                data.wavelengths, target_wavelengths[output_idx], parameters
            )
        return result

    result = np.full((*data.shape[:-1], len(target_wavelengths)), nan)
    for input_wavelengths, value_select, time_select in data.select_wavelengths():
        if values is not None:
            wavelength_selected = np.stack([values[vs] for vs in value_select], axis=-1)
        else:
            wavelength_selected = np.stack([data[vs] for vs in value_select], axis=-1)
        for output_idx in range(len(target_wavelengths)):
            result[tuple([*time_select] + [..., output_idx])] = _adjust_single_wavelength(
                wavelength_selected, input_wavelengths, target_wavelengths[output_idx], parameters
            )
    return result


def align_wavelengths(
        source: SelectedVariable,
        destination: SelectedVariable,
        parameters: typing.Optional[AdjustWavelengthParameters] = None,
        source_values: typing.Optional[np.ndarray] = None,
) -> np.ndarray:
    assert source.times.shape[0] == destination.times.shape[0]

    if not destination.has_changing_wavelengths:
        if not destination.wavelengths:
            return np.full(destination.shape, nan)
        return adjust_wavelengths(source, destination.wavelengths, parameters)

    result = np.full(destination.shape, nan)

    if not source.has_changing_wavelengths:
        if not source.wavelengths:
            return result
        for target_wavelengths, value_select, time_select in destination.select_wavelengths():
            for output_idx in range(len(target_wavelengths)):
                result[value_select[output_idx]] = _adjust_single_wavelength(
                    source.values[time_select], source.wavelengths, target_wavelengths[output_idx], parameters
                )
        return result

    for input_wavelengths, input_select, input_times in source.select_wavelengths(tail_index_only=True):
        if source_values is not None:
            input_data = np.stack([source_values[vs] for vs in input_select], axis=-1)
        else:
            input_data = np.stack([source[vs] for vs in input_select], axis=-1)
        input_begin, input_end = source.times[input_times][[0, -1]]
        for target_wavelengths, output_select, output_times in destination.select_wavelengths(tail_index_only=True):
            output_begin, output_end = destination.times[output_times][[0, -1]]

            apply_begin = max(input_begin, output_begin)
            apply_end = min(input_end, output_end)
            if apply_begin >= apply_end:
                continue

            begin_index = int(np.searchsorted(destination.times, apply_begin, side='left'))
            if begin_index >= destination.times.shape[0]:
                continue
            end_index = int(np.searchsorted(destination.times, apply_end, side='right'))
            if end_index <= 0:
                continue
            apply_times = slice(begin_index, end_index)

            for output_idx in range(len(target_wavelengths)):
                result[*((apply_times,) + output_select[output_idx])] = _adjust_single_wavelength(
                    input_data[apply_times], input_wavelengths, target_wavelengths[output_idx], parameters
                )

    return result
