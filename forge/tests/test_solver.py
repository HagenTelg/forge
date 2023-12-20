import typing
import pytest
import numpy as np
from math import nan
from forge.solver import newton_raphson, polynomial


def test_newton_raphson():
    assert newton_raphson(3.5, lambda x: x * 0.5 + 1.0) == 5.0


def test_polynomial():
    assert polynomial([0.0, 2.0], 60.0) == [30.0]
    assert polynomial([5.0, 2.0], 65.0) == [30.0]
    assert polynomial([-2, -1, 1], -1.25) == [-0.5, 1.5]
    assert polynomial([-2, -1, 1], -2) == [0.0, 1.0]
    assert polynomial([-2, -1, 1], -4) == []
    assert polynomial([2, -3, -3, 2], 20) == [pytest.approx(3.0)]
    assert polynomial([2, -3, -3, 2]) == [pytest.approx(-1.0), pytest.approx(0.5), pytest.approx(2.0)]
    assert polynomial([2, -3, -3, 2, -1], -16.0, guess=1.0) == [pytest.approx(2.0)]

    assert polynomial(np.array([0.0, 2.0]), np.array([60.0, 20.0])).tolist() == [[30.0], [10.0]]
    assert polynomial(np.array([5.0, 2.0]), np.array([65.0, 25.0])).tolist() == [[30.0], [10.0]]
    assert polynomial(np.array([-2, -1, 1]), np.array([-1.25, -2.0, -4.0])).tolist() == [
        [-0.5, 1.5],
        [0.0, 1.0],
        pytest.approx([nan, nan], nan_ok=True),
    ]
    assert polynomial(np.array([2, -3, -3, 2]), np.array([20.0, 0.0])).tolist() == [
        pytest.approx([nan, nan, 3.0], nan_ok=True),
        pytest.approx([-1.0, 0.5, 2.0], nan_ok=True),
    ]
    assert polynomial(np.array([2, -3, -3, 2, -1]), np.array([-16.0]), guess=np.array([1.0])).tolist() == [
        pytest.approx([2.0]),
    ]

