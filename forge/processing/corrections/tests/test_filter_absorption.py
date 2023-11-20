import pytest
import numpy as np
from math import nan
from forge.processing.corrections.filter_absorption import correct_weiss, correct_bond1999


def test_weiss():
    absorption = np.array([
        [2.0, 3.0, 3.5],
        [5.0, nan, 4.0],
    ])
    transmittance = np.array([
        [1.0, 0.8, 0.95],
        [0.5, 0.6, nan],
    ])

    result = correct_weiss(absorption, transmittance)
    assert result[0].tolist() == pytest.approx([0.975134080936, 1.663339986693, 1.759545534525], nan_ok=True)
    assert result[1].tolist() == pytest.approx([3.490401396161, nan, nan], nan_ok=True)


def test_bond1999():
    absorption = np.array([
        [2.0, 3.0, 3.5],
        [5.0, nan, 4.0],
    ])
    scattering = np.array([
        [10.0, 11.0, 12.0],
        [13.0, 14.0, nan],
    ])

    result = correct_bond1999(absorption, scattering)
    assert result[0].tolist() == pytest.approx([1.426229508197, 2.204918032787, 2.586065573770], nan_ok=True)
    assert result[1].tolist() == pytest.approx([3.762295081967, nan, nan], nan_ok=True)
