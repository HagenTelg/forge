import logging
import typing
import numpy as np
from math import nan
from forge.data.merge.extend import extend_selected
from ..context import SelectedData

_LOGGER = logging.getLogger(__name__)


def _outside_range(
        values: np.ndarray, times: np.ndarray,
        remove_below: float = None, below_inclusive: bool = False,
        remove_above: float = None, above_inclusive: bool = False,
        extend_before_ms: int = 0,
        extend_after_ms: int = 0,
) -> np.ndarray:
    hit = np.full(values.shape, False, dtype=np.bool_)
    if remove_below is not None:
        if below_inclusive:
            hit[values <= remove_below] = True
        else:
            hit[values < remove_below] = True
    if remove_above is not None:
        if above_inclusive:
            hit[values >= remove_above] = True
        else:
            hit[values > remove_above] = True

    hit = extend_selected(hit, times, extend_before_ms, extend_after_ms)
    return hit


def apply_limit(
        values: np.ndarray, times: np.ndarray,
        remove_below: float = None, below_inclusive: bool = False,
        remove_above: float = None, above_inclusive: bool = False,
        extend_before_ms: int = 0,
        extend_after_ms: int = 0,
) -> None:
    assert len(values.shape) == 1

    do_remove = _outside_range(
        values, times,
        remove_below=remove_below, below_inclusive=below_inclusive,
        remove_above=remove_above, above_inclusive=above_inclusive,
        extend_before_ms=extend_before_ms, extend_after_ms=extend_after_ms,
    )
    values[do_remove] = nan


def _windowed_slope_and_mean(
        x: np.ndarray,
        y: np.ndarray,
        window: int = 2,
) -> typing.Tuple[np.ndarray, np.ndarray]:
    x = np.concatenate(([0], x))
    y = np.concatenate(([0], y))

    sx = np.cumsum(x)
    sy = np.cumsum(y)
    sxx = np.cumsum(x * x)
    syy = np.cumsum(x * y)

    sx = sx[window:] - sx[:-window]
    sy = sy[window:] - sy[:-window]
    sxx = sxx[window:] - sxx[:-window]
    syy = syy[window:] - syy[:-window]

    num = window * syy - sx * sy
    div = window * sxx - sx * sx
    valid = div != 0.0
    result = np.full(div.shape, nan, dtype=np.float64)
    result[valid] = num[valid] / div[valid]
    return result, sy / window


def apply_normalized_rate_of_change_limit(
        values: np.ndarray, times: np.ndarray,
        remove_below: float = None, below_inclusive: bool = False,
        remove_above: float = None, above_inclusive: bool = False,
        minimum_normalization: float = 1E-6,
        extend_before_ms: int = 0,
        extend_after_ms: int = 0,
        window_size: int = 3,
) -> None:
    assert len(values.shape) == 1
    if values.shape[0] < (window_size+1):
        return

    forward_slopes, forward_means = _windowed_slope_and_mean(times / (60.0 * 1000.0), values, window=window_size)
    center_offset = window_size // 2
    center_end = -(window_size - center_offset - 1)

    centered_slopes = np.empty(values.shape, dtype=np.float64)
    centered_slopes[center_offset:center_end] = forward_slopes
    centered_slopes[:center_offset] = forward_slopes[0]
    centered_slopes[center_end:] = forward_slopes[-1]

    centered_means = np.empty(values.shape, dtype=np.float64)
    centered_means[center_offset:center_end] = forward_means
    centered_means[:center_offset] = forward_means[0]
    centered_means[center_end:] = forward_means[-1]
    centered_means = np.abs(centered_means)

    replace_with_minimum = np.any([
        np.invert(np.isfinite(centered_means)),
        centered_means < minimum_normalization
    ], axis=0)
    centered_means[replace_with_minimum] = minimum_normalization
    centered_slopes /= centered_means

    do_remove = _outside_range(
        centered_slopes, times,
        remove_below=remove_below, below_inclusive=below_inclusive,
        remove_above=remove_above, above_inclusive=above_inclusive,
        extend_before_ms=extend_before_ms, extend_after_ms=extend_after_ms,
    )
    values[do_remove] = nan


def meteorological_climatology_limits(
        data,
        temperature_range: typing.Tuple[float, float] = None,
        dewpoint_range: typing.Tuple[float, float] = None,
        pressure_range: typing.Tuple[float, float] = None,
        normalized_temperature_rate_of_change: typing.Tuple[float, float] = None,
        normalized_humidity_rate_of_change: typing.Tuple[float, float] = None,
        extend_before_ms: int = 0,
        extend_after_ms: int = 0,
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.climatologylimits")

    if temperature_range is not None:
        for temperature in data.select_variable({"variable_id": r"T\d*"}):
            apply_limit(
                temperature.values,
                temperature.times,
                remove_below=temperature_range[0], remove_above=temperature_range[1],
                extend_before_ms=extend_before_ms, extend_after_ms=extend_after_ms,
            )

    if dewpoint_range is not None:
        for dewpoint in data.select_variable({"variable_id": r"TD\d*"}):
            apply_limit(
                dewpoint.values,
                dewpoint.times,
                remove_below=dewpoint_range[0], remove_above=dewpoint_range[1],
                extend_before_ms=extend_before_ms, extend_after_ms=extend_after_ms,
            )

    if normalized_temperature_rate_of_change is not None:
        for temperature in data.select_variable((
                {"variable_id": r"T\d*"},
                {"variable_id": r"TD\d*"},
        )):
            apply_normalized_rate_of_change_limit(
                temperature.values,
                temperature.times,
                remove_below=normalized_temperature_rate_of_change[0],
                remove_above=normalized_temperature_rate_of_change[1],
                minimum_normalization=2.5,
                extend_before_ms=extend_before_ms, extend_after_ms=extend_after_ms,
            )

    if normalized_humidity_rate_of_change is not None:
        for humidity in data.select_variable((
                {"variable_id": r"U\d*"},
        )):
            apply_normalized_rate_of_change_limit(
                humidity.values,
                humidity.times,
                remove_below=normalized_humidity_rate_of_change[0],
                remove_above=normalized_humidity_rate_of_change[1],
                extend_before_ms=extend_before_ms, extend_after_ms=extend_after_ms,
            )

    if pressure_range is not None:
        for pressure in data.select_variable({"variable_id": r"P\d*"}):
            apply_limit(
                pressure.values,
                pressure.times,
                remove_below=pressure_range[0], remove_above=pressure_range[1],
                extend_before_ms=extend_before_ms, extend_after_ms=extend_after_ms,
            )

    # Sanity limits
    for suffix in ("", "1", "2", "3"):
        for wind_speed, wind_direction in data.select_variable(
                {"variable_id": "WS" + suffix},
                {"variable_id": "WD" + suffix},
                always_tuple=True, commit_auxiliary=True,
        ):
            do_remove = np.full(wind_speed.shape, False, dtype=np.bool_)
            do_remove[wind_speed.values < 0] = True
            do_remove[wind_speed.values > 30] = True
            do_remove[wind_direction.values < 0] = True
            do_remove[wind_direction.values > 360] = True
            wind_speed[do_remove] = nan
            wind_direction[do_remove] = nan


def vaisala_hmp_limits(
    data,
) -> None:
    data = SelectedData.ensure_data(data)
    data.append_history("forge.correction.vaisalahmplimits")

    for dewpoint, humidity in data.select_variable(
            {"variable_id": r"TD1?"},
            {"variable_id": r"U1?"},
            always_tuple=True, commit_auxiliary=True,
    ):
        do_remove = np.full(dewpoint.shape, False, dtype=np.bool_)
        do_remove[dewpoint.values < -59.85] = True
        do_remove[humidity.values > 100.0] = True
        dewpoint[do_remove] = nan
        humidity[do_remove] = nan
