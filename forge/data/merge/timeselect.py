import typing
import numpy as np


def selected_time_range(times: np.ndarray, start: int, end: int,
                        is_state: typing.Optional[bool] = None) -> typing.Optional[typing.Tuple[int, int]]:
    source_start = np.searchsorted(times, start)
    source_end = np.searchsorted(times, end)
    if source_end < times.shape[0] and times[source_end] < end:
        source_end += 1
    if is_state:
        # State incorporates the time before the interval, since that value overlaps the interval
        if source_start >= times.shape[0]:
            source_start = times.shape[0] - 1
        elif source_start > 0 and times[source_start] > start:
            source_start -= 1
    if source_start >= times.shape[0]:
        return None
    if source_start == source_end:
        return None

    return source_start, source_end
