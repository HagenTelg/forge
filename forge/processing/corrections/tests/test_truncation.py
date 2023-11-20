import pytest
import numpy as np
from math import nan
from forge.processing.corrections.truncation import ANDERSON_OGREN_1998_COEFFICIENTS, MUELLER_2011_TSI_COEFFICIENTS, MUELLER_2011_ECOTECH_COEFFICIENTS


def test_anderson_ogren_1998():
    scattering = np.array([
        [15.0, 14.0, 13.0],
        [15.0, -14.0, -13.0],
        [15.0, nan, nan],
    ])
    scattering = ANDERSON_OGREN_1998_COEFFICIENTS.apply_total_coarse(
        scattering, np.array([
            [0.343811393727, 0.323880117313, 0.307295323037],
            [nan, nan, nan],
            [nan, nan, nan],
        ]),
        [450.0, 550.0, 700.0]
    )
    assert scattering[0].tolist() == pytest.approx([19.6704813386795, 18.0922636133521, 16.4095831704592], nan_ok=True)
    assert scattering[1].tolist() == pytest.approx([19.35, -18.06, -16.38], nan_ok=True)
    assert scattering[2].tolist() == pytest.approx([19.35, nan, nan], nan_ok=True)

    scattering = np.array([
        [15.0, 14.0],
        [-15.0, 14.0],
        [nan, 14.0],
    ])
    scattering = ANDERSON_OGREN_1998_COEFFICIENTS.apply_total_fine(
        scattering, np.array([
            [0.343811393727, 0.343811393727],
            [nan, nan],
            [nan, nan],
        ]),
        [450.0, 550.0]
    )
    assert scattering[0].tolist() == pytest.approx([17.2377701383286, 15.9162121814643], nan_ok=True)
    assert scattering[1].tolist() == pytest.approx([-16.41, 15.022], nan_ok=True)
    assert scattering[2].tolist() == pytest.approx([nan, 15.022], nan_ok=True)

    backscatter = np.array([
        [5.0],
        [nan],
    ])
    backscatter = ANDERSON_OGREN_1998_COEFFICIENTS.apply_back_coarse(
        backscatter,
        [700.0]
    )
    assert backscatter[0].tolist() == pytest.approx([4.925], nan_ok=True)

    backscatter = np.array([
        [5.0],
        [nan],
    ])
    backscatter = ANDERSON_OGREN_1998_COEFFICIENTS.apply_back_fine(
        backscatter,
        [450.0]
    )
    assert backscatter[0].tolist() == pytest.approx([4.755], nan_ok=True)


def test_mueller_2011_tsi():
    scattering = np.array([
        [15.0, 14.0, 13.0],
        [15.0, -14.0, -13.0],
        [15.0, nan, nan],
    ])
    scattering = MUELLER_2011_TSI_COEFFICIENTS.apply_total_coarse(
        scattering, np.array([
            [0.343811393727, 0.323880117313, 0.307295323037],
            [nan, nan, nan],
            [nan, nan, nan],
        ]),
        [450.0, 550.0, 700.0]
    )
    assert scattering[0].tolist() == pytest.approx([19.4220530477385, 17.8810725081335, 16.2075418840550], nan_ok=True)
    assert scattering[1].tolist() == pytest.approx([19.5, -18.06, -16.38], nan_ok=True)
    assert scattering[2].tolist() == pytest.approx([19.5, nan, nan], nan_ok=True)

    scattering = np.array([
        [15.0, 14.0],
        [-15.0, 14.0],
        [nan, 14.0],
    ])
    scattering = MUELLER_2011_TSI_COEFFICIENTS.apply_total_fine(
        scattering, np.array([
            [0.343811393727, 0.343811393727],
            [nan, nan],
            [nan, nan],
        ]),
        [450.0, 550.0]
    )
    assert scattering[0].tolist() == pytest.approx([17.0085559928581, 15.7254656195130], nan_ok=True)
    assert scattering[1].tolist() == pytest.approx([-16.29, 14.924], nan_ok=True)
    assert scattering[2].tolist() == pytest.approx([nan, 14.924], nan_ok=True)

    backscatter = np.array([
        [5.0],
        [nan],
    ])
    backscatter = MUELLER_2011_TSI_COEFFICIENTS.apply_back_coarse(
        backscatter,
        [700.0]
    )
    assert backscatter[0].tolist() == pytest.approx([4.94], nan_ok=True)

    backscatter = np.array([
        [5.0],
        [nan],
    ])
    backscatter = MUELLER_2011_TSI_COEFFICIENTS.apply_back_fine(
        backscatter,
        [450.0]
    )
    assert backscatter[0].tolist() == pytest.approx([4.75], nan_ok=True)


def test_mueller_2011_ecotech():
    scattering = np.array([
        [15.0, 14.0, 13.0],
    ])
    scattering = MUELLER_2011_ECOTECH_COEFFICIENTS.apply_total_coarse(
        scattering, np.array([
            [0.447567740631, 0.415534924588, 0.389577057330],
        ]),
        [450.0, 525.0, 635.0]
    )
    assert scattering[0].tolist() == pytest.approx([20.5561454553124, 19.0521219458153, 17.4489377277341], nan_ok=True)

    backscatter = np.array([
        [5.0],
    ])
    backscatter = MUELLER_2011_ECOTECH_COEFFICIENTS.apply_back_coarse(
        backscatter,
        [635.0]
    )
    assert backscatter[0].tolist() == pytest.approx([4.84], nan_ok=True)
