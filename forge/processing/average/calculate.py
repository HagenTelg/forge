import typing
from abc import ABC, abstractmethod
from math import nan, inf
import numpy as np
from forge.logicaltime import containing_epoch_month_range, start_of_epoch_month_ms


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


def _fixed_interval_bins(times: np.ndarray, interval: typing.Union[int, float]) -> typing.Tuple[np.ndarray, np.ndarray]:
    bin_numbers = np.empty_like(times, dtype=np.int64)
    np.floor(times / interval, out=bin_numbers, casting='unsafe')
    return np.unique(bin_numbers, return_index=True)


def _fixed_interval_times(bin_numbers: np.ndarray, dt, interval: typing.Union[int, float]) -> np.ndarray:
    bin_times = np.empty_like(bin_numbers, dtype=dt)
    return np.multiply(bin_numbers, interval, out=bin_times, casting='unsafe')


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

    bin_numbers, bin_start = _fixed_interval_bins(times, interval)
    average = _bin_weighted_average(bin_start, values, weights)
    bin_times = _fixed_interval_times(bin_numbers, times.dtype, interval)
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


def _month_bins(times_epoch_ms: np.ndarray):
    if times_epoch_ms.shape[0] < 1:
        return np.empty_like(times_epoch_ms, dtype=np.int64), np.empty_like(times_epoch_ms, dtype=np.int64)

    begin_index = 0
    bin_numbers: typing.List[int] = list()
    bin_start: typing.List[int] = list()
    for month_number in range(
            *containing_epoch_month_range(
                float(times_epoch_ms[0] / 1000.0),
                float(times_epoch_ms[-1] / 1000.0))
    ):
        end_time_ms = start_of_epoch_month_ms(month_number + 1)
        end_index = np.searchsorted(times_epoch_ms[begin_index:], end_time_ms, side='right') + begin_index
        if end_index == begin_index:
            continue
        bin_numbers.append(month_number)
        bin_start.append(begin_index)

    return np.array(bin_numbers), np.array(bin_start)


_month_times = np.vectorize(start_of_epoch_month_ms)


def month_weighted_average(
        times_epoch_ms: np.ndarray,
        values: np.ndarray,
        weights: np.ndarray,
) -> typing.Tuple[np.ndarray, np.ndarray]:
    """
    Calculate one month weighted averages.

    :param times_epoch_ms: the times in ms since the epoch that the values and weights start at
    :param values: the values to average
    :param weights: the weights to apply
    :returns: a tuple of the averaged values, and the average start times
    """

    bin_numbers, bin_start = _month_bins(times_epoch_ms)
    average = _bin_weighted_average(bin_start, values, weights)
    bin_times = _month_times(bin_numbers)
    return average, bin_times


def month_cover_average(
        times_epoch_ms: np.ndarray,
        values: np.ndarray,
        averaged_time_ms: np.ndarray,
        nominal_spacing_ms: typing.Optional[typing.Union[int, float]] = None,
) -> typing.Tuple[np.ndarray, np.ndarray]:
    """
    Calculate a monthly average with weighting for values and the amount of time they represent.

    :param times_epoch_ms: the times in ms since the epoch that the values and averaged times start at
    :param values: the values to average
    :param averaged_time_ms: the amount of time each value represents in ms
    :param nominal_spacing_ms: the nominal time between each value in ms
    :returns: a tuple of the averaged values, and the average start times
    """

    return month_weighted_average(
        times_epoch_ms,
        values,
        fixed_interval_coverage_weight(times_epoch_ms, averaged_time_ms, nominal_spacing_ms),
    )


class FileAverager(ABC):
    """
    A class that handles averaging for the general file format.
    """
    def __init__(
            self,
            times_epoch_ms: np.ndarray,
            averaged_time_ms: typing.Optional[np.ndarray] = None,
            nominal_spacing_ms: typing.Optional[typing.Union[int, float]] = None,
    ):
        """
        Construct the averager.

        :param times_epoch_ms: the times in ms since the epoch that the values and averaged times start at
        :param averaged_time: the amount of time each value represents
        :param nominal_spacing_ms: the nominal time between each value in ms
        """
        self._original_times = times_epoch_ms
        if averaged_time_ms:
            self._weights = fixed_interval_coverage_weight(times_epoch_ms, averaged_time_ms, nominal_spacing_ms)
        else:
            self._weights = np.full(times_epoch_ms.shape, 1.0, dtype=np.float64)

        bin_numbers, bin_start = self.calculate_bins()
        self._bin_numbers = bin_numbers
        self._bin_start = bin_start

    @abstractmethod
    def calculate_bins(self) -> typing.Tuple[np.ndarray, np.ndarray]:
        pass

    def __call__(self, values: np.ndarray) -> np.ndarray:
        return _bin_weighted_average(self._bin_start, values, self._weights)
