import pytest
import numpy as np
from math import nan
from forge.processing.average.calculate import fixed_interval_weighted_average, coverage_weight


def test_fixed_interval_weighted():
    average, times = fixed_interval_weighted_average(
        np.array([100,  120,    130,    210,    300,    310], dtype=np.float64),
        np.array([1,    2,      3,      4,      1,      1], dtype=np.float64),
        np.array([1,    4,      2,      0,      1,      1], dtype=np.float64),
        100.0,
    )
    assert times.tolist() == pytest.approx([100, 200, 300], nan_ok=True)
    assert average.tolist() == pytest.approx([2.142857142857143, nan, 1], nan_ok=True)

    average, times = fixed_interval_weighted_average(
        np.array([100, 120, 130, 200], dtype=np.float64),
        np.array([
            [1, 2],
            [3, nan],
            [5, 6],
            [7, nan],
        ], dtype=np.float64),
        np.array([1, 4, 2, 1], dtype=np.float64),
        100.0,
    )
    assert times.tolist() == pytest.approx([100, 200], nan_ok=True)
    assert average[:, 0].tolist() == pytest.approx([3.2857142857142856, 7], nan_ok=True)
    assert average[:, 1].tolist() == pytest.approx([4.666666666666667, nan], nan_ok=True)


def test_coverage_weight():
    assert coverage_weight(
        np.array([100, 200, 300, 310, 400, 600], dtype=np.int64),
        np.array([100, 50,  9,   90,  90,  100], dtype=np.int64),
        100,
    ).tolist() == pytest.approx([1.0, 0.5, 0.9, 1.0, 0.9, 1.0])

    assert coverage_weight(
        np.array([100, 200, 300], dtype=np.int64),
        np.array([100, 90,  80], dtype=np.int64),
    ).tolist() == pytest.approx([1.0, 0.9, 0.8])

    assert coverage_weight(
        np.array([100, 200,  400], dtype=np.int64),
        np.array([100, 90,   80], dtype=np.int64),
        np.array([100, 200,  100], dtype=np.int64),
    ).tolist() == pytest.approx([1.0, 0.45, 0.8])
