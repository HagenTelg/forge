import pytest
import numpy as np
from math import nan
from forge.processing.corrections.dilution import correct_diluted


def test_dilution():
    assert correct_diluted(
        np.array([12.0, 15.0, nan]),
        [30.0], [15.0],
    ).tolist() == pytest.approx([24.0, 30.0, nan], nan_ok=True)

    result = correct_diluted(
        np.array([
            [10.0, 11.0],
            [12.0, 13.0],
            [14.0, 15.0],
        ]),
        [np.array([20.0, 21.0, 22.0]), np.array([10.0, 9.0, 8.0])],
        np.array([15.0, 10.0, nan]),
    )
    assert result[0].tolist() == pytest.approx([20.0, 22.0], nan_ok=True)
    assert result[1].tolist() == pytest.approx([18.0, 19.5], nan_ok=True)
    assert result[2].tolist() == pytest.approx([nan, nan], nan_ok=True)

