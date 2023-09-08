import typing
import numpy as np


def record_times(start_times: np.ndarray,
                 expected_record_interval: typing.Optional[typing.Union[float, int]] = None,
                 file_start_time: typing.Optional[typing.Union[float, int]] = None,
                 file_end_time: typing.Optional[typing.Union[float, int]] = None) -> np.ndarray:
    if start_times.shape[0] == 0:
        return np.array([])
    final_end_time = start_times[-1]
    if expected_record_interval:
        final_end_time = start_times[-1] + expected_record_interval
    elif file_end_time:
        final_end_time = file_end_time

    start_end_times = np.stack((
        start_times,
        np.hstack((start_times[1:], final_end_time))
    ))

    if expected_record_interval:
        possible_end_times = start_times[:-1] + expected_record_interval
        np.minimum(start_end_times[1, :-1], possible_end_times, out=start_end_times[1, :-1])

    if file_start_time:
        start_end_times[0, start_end_times[0] < file_start_time] = file_start_time
    if file_end_time:
        start_end_times[1, start_end_times[1] > file_end_time] = file_end_time

    return start_end_times


def true_ranges(truth_values: np.ndarray) -> np.ndarray:
    """
    End indices are exclusive (meaning the index after the last true value)
    """
    padded = np.concatenate(([False], truth_values, [False]))
    return np.column_stack((
        np.flatnonzero(~padded[:-1] & padded[1:]),
        np.flatnonzero(padded[:-1] & ~padded[1:])
    ))
