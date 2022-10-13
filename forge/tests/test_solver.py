import typing
import pytest
from forge.solver import newton_raphson


def test_newton_raphson():
    assert newton_raphson(3.5, lambda x: x * 0.5 + 1.0) == 5.0

