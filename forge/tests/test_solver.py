import typing
import pytest
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

