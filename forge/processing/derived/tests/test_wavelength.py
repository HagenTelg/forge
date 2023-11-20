import pytest
import numpy as np
from math import nan
from forge.processing.derived.wavelength import bracket_wavelength, wavelength_interpolate, wavelength_extrapolate


def test_bracket_wavelength():
    assert sorted(bracket_wavelength([100, 200], 100)) == [0, 1]

    assert sorted(bracket_wavelength([100, 200, 300], 100)) == [0, 1]
    assert sorted(bracket_wavelength([100, 200, 300], 90)) == [0, 1]
    assert sorted(bracket_wavelength([100, 200, 300], 300)) == [1, 2]
    assert sorted(bracket_wavelength([100, 200, 300], 310)) == [1, 2]
    assert sorted(bracket_wavelength([100, 200, 300], 110)) == [0, 1]
    assert sorted(bracket_wavelength([100, 200, 300], 290)) == [1, 2]
    assert sorted(bracket_wavelength([100, 200, 300], 200)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 300], 200, always_adjacent=False)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 300], 200, always_adjacent=False, allow_exact=True))[0] == 1
    assert sorted(bracket_wavelength([100, 200, 300], 210)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 300], 210, always_adjacent=False)) == [1, 2]
    assert sorted(bracket_wavelength([100, 200, 300], 190)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 300], 190, always_adjacent=False)) == [0, 1]

    assert sorted(bracket_wavelength([100, 200, 300, 400], 100)) == [0, 1]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 90)) == [0, 1]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 400)) == [2, 3]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 410)) == [2, 3]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 110)) == [0, 1]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 390)) == [2, 3]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 210)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 210, always_adjacent=False)) == [1, 2]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 200, always_adjacent=False, allow_exact=True))[0] == 1
    assert sorted(bracket_wavelength([100, 200, 300, 400], 190)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 190, always_adjacent=False)) == [0, 1]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 310)) == [1, 3]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 310, always_adjacent=False)) == [2, 3]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 300, always_adjacent=False, allow_exact=True))[0] == 2
    assert sorted(bracket_wavelength([100, 200, 300, 400], 290)) == [1, 3]
    assert sorted(bracket_wavelength([100, 200, 300, 400], 290, always_adjacent=False)) == [1, 2]

    assert sorted(bracket_wavelength([100, 200, 210, 300], 200)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 201)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 201, always_adjacent=False)) == [1, 2]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 210)) == [1, 3]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 209)) == [1, 3]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 209, always_adjacent=False)) == [1, 2]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 199)) == [0, 2]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 199, always_adjacent=False)) == [0, 1]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 211)) == [1, 3]
    assert sorted(bracket_wavelength([100, 200, 210, 300], 211, always_adjacent=False)) == [2, 3]


def test_wavelength_interpolate():
    assert wavelength_interpolate(
        np.array([12.0, 22.0, -10.0]), 450.0,
        np.array([11.0, 21.0, -11.0]), 550.0,
        467.0
    ).tolist() == pytest.approx([11.808598250642, 21.811690349314, -10.17], nan_ok=True)
    assert wavelength_interpolate(
        np.array([12.0, 22.0, -10.0]), 450.0,
        np.array([11.0, 21.0, -11.0]), 550.0,
        528.0
    ).tolist() == pytest.approx([11.196439256671, 21.199676059089, -10.78], nan_ok=True)
    assert wavelength_interpolate(
        np.array([11.0, 21.0, nan]), 550.0,
        np.array([10.0, nan, -12.0]), 700.0,
        660.0
    ).tolist() == pytest.approx([10.235269752923, nan, nan], nan_ok=True)

    result = wavelength_interpolate(
        np.array([
            [12.0, 22.0, -10.0],
            [12.0, 22.0, -11.0],
        ]), 450.0,
        np.array([
            [11.0, 21.0, -11.0],
            [11.0, 21.0, -12.0],
        ]), 550.0,
        467.0
    )
    assert result[0].tolist() == pytest.approx([11.808598250642, 21.811690349314, -10.17], nan_ok=True)
    assert result[1].tolist() == pytest.approx([11.808598250642, 21.811690349314, -11.17], nan_ok=True)


def test_wavelength_extrapolate():
    assert wavelength_extrapolate(
        np.array([8.0, 12.0, nan]), 550.0, 1.5, 450.0
    ).tolist() == pytest.approx([10.8097400574546, 16.214610086181956, nan], nan_ok=True)

    result = wavelength_extrapolate(
        np.array([
            [8.0, 12.0, nan],
            [10.0, nan, 5.0],
        ]), 500.0, 1.75, 600.0
    )
    assert result[0].tolist() == pytest.approx([5.8146396632894755, 8.721959494934213, nan], nan_ok=True)
    assert result[1].tolist() == pytest.approx([7.2682995791118445, nan, 3.6341497895559223], nan_ok=True)
