import pytest
import numpy as np
from math import nan
from forge.processing.derived.angstrom import calculate_angstrom_exponent


def test_angstrom_calculate():
    assert calculate_angstrom_exponent(
        np.array([22.0, 21.0, 20.0, -1.0]),
        450.0,
        np.array([12.0, 11.0, 10.0, 9.0]),
        550.0,
    ).tolist() == pytest.approx([3.020549673057, 3.222329814704, 3.454152480827, nan], nan_ok=True)
