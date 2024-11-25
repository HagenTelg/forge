import typing
import numpy as np


def extend_selected(
        hit: np.ndarray,
        times: np.ndarray,
        extend_before: int,
        extend_after: int,
) -> np.ndarray:
    assert times.shape == hit.shape

    if extend_before <= 0 and extend_after <= 0:
        return hit.astype(np.bool_, casting='unsafe', copy=False)

    origin_times = times[hit]
    begin_times = origin_times - extend_before
    end_times = origin_times + extend_after

    compare = times[:, None]
    return np.any((compare >= begin_times) & (compare <= end_times), axis=1)