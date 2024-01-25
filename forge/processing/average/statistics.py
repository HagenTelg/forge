import typing
import numpy as np
from numba import njit, prange


@njit(cache=True, nogil=True, parallel=True)
def _bin_quantiles_inner(values: np.ndarray, bin_start: np.ndarray, quantiles: np.ndarray, result: np.ndarray) -> None:
    last_bin = bin_start.shape[0] - 1
    for value_index in np.ndindex(*values.shape[1:]):
        for bin_number in prange(last_bin):
            result[(bin_number, *value_index)] = np.nanquantile(
                values[(
                    slice(bin_start[bin_number], bin_start[bin_number + 1]),
                    *value_index)
                ], quantiles
            )
        result[(last_bin, *value_index)] = np.nanquantile(
            values[(
                slice(bin_start[-1], None),
                *value_index)
            ], quantiles
        )


def bin_quantiles(
        bin_start: np.ndarray,
        values: np.ndarray,
        quantiles: typing.Union[np.ndarray, float, typing.Iterable[float]]
) -> np.ndarray:
    assert len(bin_start.shape) == 1
    assert bin_start.shape[0] > 0
    assert len(values.shape) > 0
    assert values.shape[0] > 0

    quantiles = np.array(quantiles, copy=False).flatten()
    assert len(quantiles.shape) == 1
    assert quantiles.shape[0] > 0

    result = np.empty((bin_start.shape[0], *values.shape[1:], quantiles.shape[0]), dtype=values.dtype)
    _bin_quantiles_inner(values, bin_start, quantiles, result)
    return result
