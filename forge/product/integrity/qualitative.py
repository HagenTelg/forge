import typing
import numpy as np
from math import ceil, nan


_DECOMPOSITION_LEVELS = [1, 3, 7, 11, 24]


def _decompose(values: np.ndarray) -> np.ndarray:
    result = np.empty(sum(_DECOMPOSITION_LEVELS), np.int16)
    level_begin = 0
    for level_split in _DECOMPOSITION_LEVELS:
        level_pieces = np.array_split(values, level_split)
        pad_size = int(ceil(values.shape[0] / level_split))
        for i in range(len(level_pieces)):
            level_pieces[i] = np.pad(level_pieces[i], (0, pad_size - level_pieces[i].shape[0]),
                                     'constant', constant_values=nan)

        level_data = np.abs(np.nanmean(level_pieces, axis=1))
        valid_level_data = np.logical_and(
            np.isfinite(level_data),
            level_data > 0
        )
        level_data[valid_level_data] = np.round(np.log10(level_data[valid_level_data]) * 10)
        level_data[np.invert(valid_level_data)] = -32767
        level_data[level_data < -32767] = -32767
        level_data[level_data > 32767] = 32767
        result[level_begin:level_begin + level_data.shape[0]] = level_data.astype(np.int16)
        level_begin += level_data.shape[0]
    return result


def qualitative_digest(data: np.ndarray, digest) -> None:
    assert len(data.shape) == 1

    data = data[np.isfinite(data)]
    if data.shape[0] == 0:
        digest.update(bytes([0]))
        return

    if data.shape[0] <= _DECOMPOSITION_LEVELS[-1]:
        data = np.abs(data)
        valid_data = np.logical_and(
            np.isfinite(data),
            data > 0
        )
        data[valid_data] = np.round(np.log10(data[valid_data]) * 10)
        data[np.invert(valid_data)] = -32767
        data[data < -32767] = -32767
        data[data > 32767] = 32767
        digest.update(data.astype('<i2', casting='unsafe', order='C').tobytes(order='C'))
        return

    decomposed = _decompose(data)

    digest.update(decomposed.astype('<i2', casting='unsafe', order='C').tobytes(order='C'))
