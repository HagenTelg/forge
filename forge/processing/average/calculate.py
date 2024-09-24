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
        begin_index = end_index

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
        :param averaged_time_ms: the amount of time each value represents in ms
        :param nominal_spacing_ms: the nominal time between each value in ms
        """

        assert len(times_epoch_ms.shape) == 1
        assert times_epoch_ms.shape[0] > 0

        self._original_times = times_epoch_ms
        self._original_averaged_time = averaged_time_ms
        if averaged_time_ms is not None:
            assert self._original_times.shape == averaged_time_ms.shape
            self._weights = fixed_interval_coverage_weight(times_epoch_ms, averaged_time_ms, nominal_spacing_ms)
        else:
            self._weights = np.full(times_epoch_ms.shape, 1.0, dtype=np.float64)

        bin_numbers, bin_start = self.calculate_bins()
        assert len(bin_start) > 0
        self._bin_numbers = bin_numbers
        self._bin_start = bin_start

    @abstractmethod
    def calculate_bins(self) -> typing.Tuple[np.ndarray, np.ndarray]:
        """
        Generate the bins for the file.  This is called during construction only.

        :returns: a tuple of the bin numbers (or identifiers) and the first indices belonging to the bin
        """
        pass

    @property
    def times(self) -> np.ndarray:
        """
        The bin times in epoch milliseconds.
        """
        raise NotImplementedError

    def __call__(self, values: np.ndarray, mask: np.ndarray = None) -> np.ndarray:
        """
        Calculate the weighted average of an input.

        :param values: the input values
        :param mask: the optional mask of values to exclude
        :returns: the weighted average
        """
        weights = self._weights
        if mask is not None:
            weights = np.array(weights)
            weights[mask] = 0
        return _bin_weighted_average(self._bin_start, values, weights)

    def sum(self, values: np.ndarray, mask: np.ndarray = None) -> np.ndarray:
        """
        Calculate the sum of values in each bin.

        :param values: the input values
        :param mask: the optional mask of values to exclude
        :returns: the per-bin sum
        """
        if np.issubdtype(values.dtype, np.floating):
            values = np.array(values)
            values[np.invert(np.isfinite(values))] = 0
            if mask is not None:
                values[mask] = 0
        elif mask is not None:
            values = np.array(values)
            values[mask] = 0
        return np.add.reduceat(values, self._bin_start, dtype=values.dtype)

    def bitwise_or(self, values: np.ndarray) -> np.ndarray:
        """
        Calculate a bitwise OR of the input values in each bin.

        :param values: the input values
        :returns: the per-bin bitwise OR of the inputs in that bin
        """
        return np.bitwise_or.reduceat(values, self._bin_start, dtype=values.dtype)

    def _ufunc_index(self, values: np.ndarray, ufunc, invalid_value, mask: np.ndarray = None) -> np.ndarray:
        flat_values = values.flatten()
        indices = np.arange(flat_values.shape[0], dtype=np.int64)
        indices[np.invert(np.isfinite(flat_values))] = invalid_value
        if mask is not None:
            indices[mask] = invalid_value
        indices = indices.reshape(values.shape)

        indices = ufunc.reduceat(indices, self._bin_start, dtype=np.int64)
        valid_indices = indices != invalid_value

        result = np.empty_like(indices, dtype=values.dtype)
        result[valid_indices] = flat_values[indices[valid_indices]]
        if np.issubdtype(values.dtype, np.floating):
            result[np.invert(valid_indices)] = nan
        else:
            result[np.invert(valid_indices)] = 0

        return result

    def first_valid(self, values: np.ndarray, mask: np.ndarray = None) -> np.ndarray:
        """
        Return the first valid (finite/non-NaN) value in each bin.

        :param values: the input values
        :param mask: the optional mask of values to exclude
        :returns: the first valid value in each bin
        """
        return self._ufunc_index(values, np.minimum, np.iinfo(np.int64).max, mask=mask)

    def last_valid(self, values: np.ndarray, mask: np.ndarray = None) -> np.ndarray:
        """
        Return the last valid (finite/non-NaN) value in each bin.

        :param values: the input values
        :param mask: the optional mask of values to exclude
        :returns: the last valid value in each bin
        """
        return self._ufunc_index(values, np.maximum, np.iinfo(np.int64).min, mask=mask)

    def vector(self, magnitude: np.ndarray, direction: np.ndarray,
               mask: np.ndarray = None) -> typing.Tuple[np.ndarray, np.ndarray]:
        """
        Calculate a weighted vector average.  The weighting and averaging is done in the X-Y components, then
        the magnitude and direction are calculated from those components.

        :param magnitude: the input magnitude
        :param direction: the input direction (degrees)
        :param mask: the optional mask of values to exclude
        :returns: a tuple of the averaged magnitude and the averaged direction
        """
        assert magnitude.shape == direction.shape
        valid_values = np.all((
            np.isfinite(magnitude),
            np.isfinite(direction),
        ), axis=0)
        direction = np.radians(direction - 180)
        x = np.full(magnitude.shape, nan, dtype=np.float64)
        x[valid_values] = np.cos(direction[valid_values]) * magnitude[valid_values]
        y = np.full(magnitude.shape, nan, dtype=np.float64)
        y[valid_values] = np.sin(direction[valid_values]) * magnitude[valid_values]

        x = self(x, mask=mask)
        y = self(y, mask=mask)

        magnitude = np.sqrt(x**2 + y**2)
        direction = np.degrees(np.arctan2(y, x)) + 180.0
        direction[np.fabs(direction - 360) < 1E-10] = 0
        return magnitude, direction

    def valid_count(self, values: np.ndarray, mask: np.ndarray = None) -> np.ndarray:
        """
        Return the number of valid (finite/non-NaN) values in each bin.

        :param values: the input values
        :param mask: the optional mask of values to exclude
        :returns: the number of valid points in each bin
        """
        ones = np.full(values.shape, 1, dtype=np.uint32)
        ones[np.invert(np.isfinite(values))] = 0
        if mask is not None:
            ones[mask] = 0
        return np.add.reduceat(ones, self._bin_start, dtype=np.uint32)

    def unweighted_mean(self, values: np.ndarray, mask: np.ndarray = None) -> np.ndarray:
        """
        Calculate the unweighted mean.

        :param values: the input values
        :param mask: the optional mask of values to exclude
        :returns: the mean values
        """
        weights = np.full((values.shape[0], ), 1, dtype=values.dtype)
        if mask is not None:
            weights[mask] = 0
        return _bin_weighted_average(self._bin_start, values, weights)

    def stddev(self, values: np.ndarray,
               unweighted_mean: typing.Optional[np.ndarray] = None, mask: np.ndarray = None) -> np.ndarray:
        """
        Calculate the per-bin standard deviation.

        :param values: the input values
        :param unweighted_mean: the unweighted mean value (automatically calculated if not provided)
        :param mask: the optional mask of values to exclude
        :returns: the per-bin standard deviations
        """
        sq = values ** 2
        sq[np.invert(np.isfinite(sq))] = 0
        if mask is not None:
            sq[mask] = 0
        sq = np.add.reduceat(sq, self._bin_start, dtype=np.float64)

        count = np.full(values.shape, 1, dtype=np.float64)
        count[np.invert(np.isfinite(values))] = 0
        if mask is not None:
            count[mask] = 0
        count = np.add.reduceat(count, self._bin_start, dtype=np.float64)

        if unweighted_mean is None:
            unweighted_mean = self.unweighted_mean(values)

        result = np.full(unweighted_mean.shape, nan, dtype=np.float64)
        valid_values = count >= 2
        result[valid_values] = sq[valid_values] / count[valid_values] - unweighted_mean[valid_values]**2
        result[result < 0.0] = nan
        valid_values = np.isfinite(result)

        result[valid_values] = np.sqrt(result[valid_values] * (count[valid_values] / (count[valid_values] - 1)))
        result[np.invert(valid_values)] = nan
        return result

    def quantiles(
            self,
            values: np.ndarray,
            quantiles: typing.Union[np.ndarray, float, typing.Iterable[float]]
    ) -> np.ndarray:
        """
        Calculate quantiles for the input.

        :param values: the input values
        :param quantiles: the quantiles [0-1]
        :returns: the binned quantiles, with the quantile dimension added to the end
        """
        from forge.processing.average.statistics import bin_quantiles
        return bin_quantiles(self._bin_start, values, quantiles)

    @property
    def averaged_count(self) -> np.ndarray:
        """
        The number of records present in each bin.
        """
        if self._original_times.shape[0] == 0:
            return np.empty((0,), dtype=np.uint32)
        elif self._original_times.shape[0] == 1:
            return np.array([1], dtype=np.uint32)
        count = np.empty(self._bin_start.shape, dtype=np.uint32)
        if self._bin_start.shape[0] > 1:
            count[:-1] = self._bin_start[1:] - self._bin_start[:-1]
        count[-1] = self._original_times.shape[0] - self._bin_start[-1]
        return count

    @property
    def averaged_time_ms(self) -> typing.Optional[np.ndarray]:
        """
        The total number of milliseconds averaged for each bin.
        """
        if self._original_averaged_time is None:
            return None
        return np.add.reduceat(self._original_averaged_time, self._bin_start, dtype=np.uint64)


class FixedIntervalFileAverager(FileAverager):
    """
    A class that handles fixed interval (e.x. hourly) averaging files.
    """
    def __init__(
            self,
            interval_ms: int,
            times_epoch_ms: np.ndarray,
            averaged_time_ms: typing.Optional[np.ndarray] = None,
            nominal_spacing_ms: typing.Optional[typing.Union[int, float]] = None,
    ):
        """
        Construct the averager.

        :param interval_ms: the interval to average at in ms
        :param times_epoch_ms: the times in ms since the epoch that the values and averaged times start at
        :param averaged_time_ms: the amount of time each value represents in ms
        :param nominal_spacing_ms: the nominal time between each value in ms
        """
        self._interval = interval_ms
        super().__init__(times_epoch_ms, averaged_time_ms, nominal_spacing_ms)

    def calculate_bins(self) -> typing.Tuple[np.ndarray, np.ndarray]:
        return _fixed_interval_bins(self._original_times, self._interval)

    @property
    def times(self) -> np.ndarray:
        return _fixed_interval_times(self._bin_numbers, np.int64, self._interval)


class MonthFileAverager(FileAverager):
    """
    A class that handles monthly averaging files.
    """

    def calculate_bins(self) -> typing.Tuple[np.ndarray, np.ndarray]:
        return _month_bins(self._original_times)

    @property
    def times(self) -> np.ndarray:
        return _month_times(self._bin_numbers)
