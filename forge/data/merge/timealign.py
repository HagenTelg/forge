import typing
import numpy as np
from math import log2, ceil
from numba import njit


def _unsorted_before(
        destination: np.ndarray,
        incoming: np.ndarray,
) -> np.ndarray:
    # Sort is N*log(N), while this is N*log(M), so just avoid sorting destination (assuming N ~= M)
    incoming_indices = np.searchsorted(incoming, destination, side='right') - 1
    incoming_indices[incoming_indices < 0] = 0
    return incoming_indices


@njit(cache=True, nogil=True)
def _sorted_before(
        destination: np.ndarray,
        incoming: np.ndarray,
        result: np.ndarray
) -> None:
    incoming_idx = 0
    for destination_idx in range(destination.shape[0]):
        while incoming_idx + 1 < incoming.shape[0] and incoming[incoming_idx + 1] <= destination[destination_idx]:
            incoming_idx += 1
        result[destination_idx] = incoming_idx


def incoming_before(
        destination: np.ndarray,
        incoming: np.ndarray,
        sort_destination: bool = False,
        sort_incoming: bool = False,
) -> np.ndarray:
    """
    Get the indices into incoming such that the times are the value before or equal to the destination.
    This generally means the indices can be used for data so that the incoming indexed values are the latest
    value effective for the destination time.  In other words, destination >= incoming[result], with the difference
    minimized.

    :param destination: the target times
    :param incoming: the incoming times to merge into the destination
    :param sort_destination: if true then the destination requires sorting
    :param sort_incoming: if true then the incoming requires sorting
    :returns: indices in incoming that target the destination, with the same size as destination
    """

    assert incoming.shape[0] > 0
    assert destination.shape[0] > 0

    incoming_much_larger = incoming.shape[0] > max(int(ceil(log2(incoming.shape[0])))*destination.shape[0], 2048)

    if not sort_destination and not sort_incoming and not incoming_much_larger:
        result = np.empty((destination.shape[0], ), dtype=np.uintp)
        _sorted_before(destination, incoming, result)
        return result
    elif not sort_incoming:
        return _unsorted_before(destination, incoming)

    incoming_idx = np.argsort(incoming)
    incoming_sorted = incoming[incoming_idx]
    if not sort_destination and not incoming_much_larger:
        result = np.empty((destination.shape[0],), dtype=np.uintp)
        _sorted_before(destination, incoming_sorted, result)
    else:
        result = _unsorted_before(destination, incoming_sorted)
    return incoming_idx[result]


def align_latest(
        output_times: np.ndarray,
        input_times: np.ndarray,
        input_values: np.ndarray,
) -> np.ndarray:
    """
    Perform simple alignment of input values to output times, taking the latest available input for the output
    time.

    :param output_times: the output times to align at
    :param input_times: the times the input values are at
    :param input_values: the values to align
    :returns: an array with the time (first) dimension at the output times, with the other values at the latest input
    """
    indices = incoming_before(output_times, input_times)
    return input_values[indices]


def peer_output_time(
        *peer_times: typing.Union[np.ndarray, typing.Sequence],
        apply_rounding: typing.Union[bool, int, float] = True,
) -> np.ndarray:
    """
    Generate an output time series for a number of peer input times.  This tries to create a series of times that
    are well suited to match all the input times.

    :param peer_times: the input time values
    :param apply_rounding: apply best guess interval rounding or set the interval to round at
    :returns: the combined time points
    """

    if len(peer_times) == 0:
        return np.empty((0,), dtype=np.int64)
    if len(peer_times) == 1:
        return np.array(peer_times[0], copy=False)

    combined_time = np.unique(np.concatenate(peer_times))
    if not apply_rounding or combined_time.shape[0] <= 2:
        return combined_time

    if np.issubdtype(type(apply_rounding), np.floating) or np.issubdtype(type(apply_rounding), np.integer):
        time_step = float(apply_rounding)
    else:
        time_difference = combined_time[1:] - combined_time[:-1]
        time_step_values, time_step_count = np.unique(time_difference, return_counts=True)
        time_step = time_step_values[np.argmax(time_step_count)]
        if time_step == 0:
            return np.empty((0,), dtype=peer_times[0].dtype)

    original_type = combined_time.dtype
    combined_time = np.round(combined_time / time_step) * time_step
    combined_time = combined_time.astype(original_type, casting='unsafe', copy=False)
    combined_time = np.unique(combined_time)
    return combined_time
