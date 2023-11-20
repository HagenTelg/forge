import typing
import numpy as np
from numba import njit
from math import isfinite, exp


@njit(cache=True, nogil=True)
def _apply_single_pole_low_pass_1d(
        times: np.ndarray,
        values: np.ndarray,
        tc: float,
        gap: typing.Union[int, float],
        reset_on_invalid: bool,
) -> None:
    prior_value = values[0]
    prior_time = times[0]
    for i in range(1, times.shape[0]):
        if times[i] - times[i-1] > gap:
            prior_value = values[i]
            prior_time = times[i]
            continue
        if not isfinite(values[i]):
            if reset_on_invalid:
                prior_value = values[i]
                prior_time = times[i]
            continue
        if not isfinite(prior_value):
            prior_value = values[i]
            prior_time = times[i]
            continue

        b = exp(-1.0 / (tc / (times[i] - prior_time)))
        a = 1.0 - b
        prior_value = a * values[i] + b * prior_value
        values[i] = prior_value
        prior_time = times[i]


def single_pole_low_pass(
        times: np.ndarray,
        values: np.ndarray,
        tc: float,
        gap: typing.Union[int, float],
        reset_on_invalid: bool = False,
) -> np.ndarray:
    assert times.shape[0] == values.shape[0]
    if times.shape[0] == 0:
        return np.empty((0, *values.shape[1:]), dtype=np.float64)

    result = np.array(values, dtype=np.float64)
    if len(result.shape) > 1:
        for idx in np.ndindex(*result.shape[1:]):
            print(idx)
            _apply_single_pole_low_pass_1d(times, result[:, *idx], tc, gap, reset_on_invalid)
    else:
        _apply_single_pole_low_pass_1d(times, result, tc, gap, reset_on_invalid)
    return result
