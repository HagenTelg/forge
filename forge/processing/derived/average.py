import typing
import numpy as np
from math import nan
from forge.processing.context import SelectedVariable


def hourly_average(source: SelectedVariable) -> np.ndarray:
    from forge.data.merge.timealign import incoming_before
    from ..average.calculate import fixed_interval_weighted_average

    smoothed_output = np.full_like(source.values, nan)
    for _, value_select, time_select in source.select_cut_size():
        selected_times = source.times[time_select]
        selected_values = source.values[value_select]
        smoothed_values, smoothed_start = fixed_interval_weighted_average(
            selected_times,
            selected_values,
            source.average_weights[time_select],
            60 * 60 * 1000,
        )

        smoothed_targets = incoming_before(selected_times, smoothed_start)
        smoothed_output[value_select] = smoothed_values[smoothed_targets]
    return smoothed_output


def single_pole_low_pass_digital_filter(
        source: SelectedVariable,
        tc: float = 3 * 60 * 1000,
        gap: typing.Union[int, float] = 35 * 60 * 1000,
) -> np.ndarray:
    from ..average.digitalfilter import single_pole_low_pass

    smoothed_output = np.full_like(source.values, nan)
    for _, value_select, time_select in source.select_cut_size():
        smoothed_output[value_select] = single_pole_low_pass(
            source.times[value_select],
            source.values[time_select],
            tc,
            gap,
        )
    return smoothed_output
