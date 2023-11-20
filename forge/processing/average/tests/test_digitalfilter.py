import pytest
import numpy as np
from math import nan
from forge.processing.average.digitalfilter import single_pole_low_pass


def test_single_pole_low_pass():
    assert single_pole_low_pass(
        np.array([100,  110,    120,    140,    250,    260,    270,    280,    290], dtype=np.float64),
        np.array([1,    1,      2,      2,      1,      2,      nan,    1,      2], dtype=np.float64),
        10.0, 100.0,
        reset_on_invalid=True,
    ).tolist() == pytest.approx([1.0, 1.0, 1.63212055882856, 1.95021293163214, 1.0, 1.63212055882856, nan, 1.0, 1.63212055882856], nan_ok=True)

    result = single_pole_low_pass(
        np.array([100, 110, 120, 130], dtype=np.float64),
        np.array([
            [1.0, 1.0, 1.0],
            [1.0, 2.0, nan],
            [2.0, 2.0, 2.0],
            [2.0, nan, 2.0],
        ], dtype=np.float64),
        10.0, 100.0,
    )
    assert result[:, 0].tolist() == pytest.approx([1.0, 1.0, 1.63212055882856, 1.8646647167633872], nan_ok=True)
    assert result[:, 1].tolist() == pytest.approx([1.0, 1.63212055882856, 1.8646647167633872, nan], nan_ok=True)
    assert result[:, 2].tolist() == pytest.approx([1.0, nan, 1.8646647167633872, 1.950212931632136], nan_ok=True)
