import typing
from math import nan, inf
import numpy as np


def _bin_weighted_average(bin_start: np.ndarray, values: np.ndarray, weights: np.ndarray) -> np.ndarray:
    assert (values.shape[0], ) == (weights.shape[0], )

    weighted_values = (values.T * weights).T
    shaped_weights = np.full(values.T.shape, weights.T, dtype=weights.dtype).T
    invalid_values = np.invert(np.isfinite(weighted_values))
    weighted_values[invalid_values] = 0
    shaped_weights[invalid_values] = 0

    sum_values = np.add.reduceat(weighted_values, bin_start, dtype=np.float64)
    sum_weights = np.add.reduceat(shaped_weights, bin_start, dtype=np.float64)

    valid_averages = sum_weights != 0
    sum_values[valid_averages] /= sum_weights[valid_averages]
    sum_values[np.invert(valid_averages)] = nan

    empty_bins = np.where(bin_start[:-1] == bin_start[1:])[0]
    sum_values[empty_bins + 1] = nan

    return sum_values


def fixed_interval_weighted_average(
        times: np.ndarray,
        values: np.ndarray,
        weights: np.ndarray,
        interval: typing.Union[int, float],
) -> typing.Tuple[np.ndarray, np.ndarray]:
    """
    Calculate fixed time interval weighted averages.

    :param times: the times that the values and weights start at
    :param values: the values to average
    :param weights: the weights to apply
    :param interval: the interval to average at, with the same units as the time
    :returns: a tuple of the averaged values, and the average start times
    """

    bin_numbers = np.empty_like(times, dtype=np.int64)
    np.floor(times / interval, out=bin_numbers, casting='unsafe')
    bin_numbers, bin_start = np.unique(bin_numbers, return_index=True)
    average = _bin_weighted_average(bin_start, values, weights)
    bin_times = np.empty_like(bin_numbers, dtype=times.dtype)
    np.multiply(bin_numbers, interval, out=bin_times, casting='unsafe')
    return average, bin_times


def fixed_interval_coverage_weight(
        times: np.ndarray,
        averaged_time: np.ndarray,
        nominal_spacing: typing.Optional[typing.Union[int, float, np.ndarray]] = None,
) -> np.ndarray:
    """
    Calculate a coverage weight for average start times, a measured amount of time that went into the average,
    and an optional nominal spacing.

    :param times: the times that the values and averaged times start at
    :param averaged_time: the amount of time each value represents
    :param nominal_spacing: the nominal time between each value
    :returns: the coverage weight fractions
    """

    if times.shape[0] <= 1:
        if nominal_spacing is not None:
            return np.divide(averaged_time, nominal_spacing, dtype=np.float64)
        return np.full(times.shape, 1.0, dtype=np.float64)

    weights = np.empty(times.shape, dtype=np.float64)
    weights[:-1] = times[1:] - times[:-1]
    if nominal_spacing is not None:
        weights[-1] = inf
        np.minimum(weights, nominal_spacing, out=weights)
    else:
        weights[-1] = times[-1] - times[-2]
    np.divide(averaged_time, weights, out=weights)
    np.minimum(weights, 1.0, out=weights)
    return weights


def fixed_interval_cover_average(
        times: np.ndarray,
        values: np.ndarray,
        averaged_time: np.ndarray,
        interval: typing.Union[int, float],
        nominal_spacing: typing.Optional[typing.Union[int, float]] = None,
) -> typing.Tuple[np.ndarray, np.ndarray]:
    """
    Calculate a fixed interval average with weighting for values and the amount of time they represent.
    This uses the nominal spacing, if available, as an upper limit when calculating the relative coverage of
    a value.

    :param times: the times that the values and averaged times start at
    :param values: the values to average
    :param averaged_time: the amount of time each value represents
    :param nominal_spacing: the nominal time between each value
    :param interval: the interval to average at, with the same units as the time
    :returns: a tuple of the averaged values, and the average start times
    """

    return fixed_interval_weighted_average(
        times,
        values,
        fixed_interval_coverage_weight(times, averaged_time, nominal_spacing),
        interval
    )
