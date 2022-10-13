import typing
import pytest
from forge.rayleigh import rayleigh_scattering


def test_rayleigh():
    # Some selections Table 2
    assert rayleigh_scattering(230) == pytest.approx(469.1 * (288.15 / 273.15), rel=0.02)
    assert rayleigh_scattering(350) == pytest.approx(74.50 * (288.15 / 273.15), rel=0.02)
    assert rayleigh_scattering(450) == pytest.approx(26.16 * (288.15 / 273.15), rel=0.02)
    assert rayleigh_scattering(510) == pytest.approx(15.64 * (288.15 / 273.15), rel=0.02)
    assert rayleigh_scattering(550) == pytest.approx(11.49 * (288.15 / 273.15), rel=0.02)
    assert rayleigh_scattering(600) == pytest.approx(8.053 * (288.15 / 273.15), rel=0.02)
    assert rayleigh_scattering(700) == pytest.approx(4.310 * (288.15 / 273.15), rel=0.02)
    assert rayleigh_scattering(780) == pytest.approx(2.781 * (288.15 / 273.15), rel=0.02)

    # Standard TSI neph values
    assert rayleigh_scattering(450) == pytest.approx(27.89, rel=0.02)
    assert rayleigh_scattering(550) == pytest.approx(12.26, rel=0.02)
    assert rayleigh_scattering(700) == pytest.approx(4.605, rel=0.02)
    assert rayleigh_scattering(450, start_angle=90.0) == pytest.approx(27.89 * 0.5, rel=0.02)
    assert rayleigh_scattering(550, start_angle=90.0) == pytest.approx(12.26 * 0.5, rel=0.02)
    assert rayleigh_scattering(700, start_angle=90.0) == pytest.approx(4.605 * 0.5, rel=0.02)
