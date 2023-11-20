import pytest
import numpy as np
from math import nan
from forge.processing.corrections.stp import correct_optical, correct_volume


def test_optical():
    values = np.array([15.0, 12.123])
    temperatures = np.array([273.15, 23.0])
    pressures = np.array([1013.25, 1000.0])

    result = correct_optical(values, temperatures, pressures).tolist()
    assert result[0] == pytest.approx(15.0)
    assert result[1] == pytest.approx(13.3179460020593)


def test_volume():
    values = np.array([1.0, 3.0])
    temperatures = np.array([273.15, 282.0])
    pressures = np.array([1013.25, 1008.0])

    result = correct_volume(values, temperatures, pressures).tolist()
    assert result[0] == pytest.approx(1.0)
    assert result[1] == pytest.approx(2.89079484070114)


def test_multidimensional():
    values = np.array([
        [15.0, 16.0, 17.0],
        [1.0, 2.0, nan],
    ])
    temperatures = np.array([273.15, 280.0])
    pressures = np.array([1013.25, 1010.0])

    result = correct_optical(values, temperatures, pressures)
    assert result[0].tolist() == [15.0, 16.0, 17.0]
    assert result[1].tolist() == pytest.approx([1.02837631374340, 2.05675262748680, nan], nan_ok=True)


def test_constants():
    values = np.array([15.0, 12.123])

    result = correct_optical(values, 273.15, 1013.25).tolist()
    assert result[0] == pytest.approx(15.0)
    assert result[1] == pytest.approx(12.123)

    result = correct_optical(values, 23.0, 1000.0).tolist()
    assert result[0] == pytest.approx(16.47852759472817)
    assert result[1] == pytest.approx(13.3179460020593)
