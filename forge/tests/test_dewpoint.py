import typing
import pytest
from forge.dewpoint import dewpoint, rh, temperature, extrapolate_rh


def test_dewpoint():
    assert dewpoint(20.0, 10.0) == pytest.approx(-11.1798699958809)
    assert dewpoint(30.0, 10.0) == pytest.approx(-4.34958706769009)
    assert dewpoint(30.0, 80.0) == pytest.approx(26.1684418167009)
    assert dewpoint(30.0, 80.0, over_water=True) == pytest.approx(26.1684418167009)
    assert dewpoint(30.0, 10.0, over_water=True) == pytest.approx(-4.90916662962104)
    assert dewpoint(-10.0, 10.0) == pytest.approx(-33.597234223073)
    assert dewpoint(-10.0, 10.0, over_water=True) == pytest.approx(-35.9333319940692)


def test_rh():
    assert rh(30.0, 28.0) == pytest.approx(89.0767330629172)
    assert rh(30.0, 30.0) == pytest.approx(100.0)
    assert rh(30.0, -10.0) == pytest.approx(6.11960074076457)
    assert rh(30.0, -10.0, over_water=True) == pytest.approx(6.74489883084801)
    assert rh(-5.0, -10.0) == pytest.approx(64.686228666108)
    assert rh(-5.0, -10.0, over_water=True) == pytest.approx(67.9145715862388)


def test_temperature():
    assert temperature(89.0767330629172, 28.0) == pytest.approx(30.0)
    assert temperature(100.0, 30.0) == pytest.approx(30.0)
    assert temperature(6.11960074076457, -10.0) == pytest.approx(30.0)
    assert temperature(6.74489883084801, -10.0, over_water=True) == pytest.approx(30.0)
    assert temperature(64.686228666108, -10.0) == pytest.approx(-5.0)
    assert temperature(67.9145715862388, -10.0, over_water=True) == pytest.approx(-5.0)


def test_extrapolate_rh():
    assert extrapolate_rh(10.0, 20.0, 15.0) == pytest.approx(14.4005416242256)
    assert extrapolate_rh(-10.0, 20.0, -15.0) == pytest.approx(31.448302796248)
    assert extrapolate_rh(-10.0, 20.0, -15.0, over_water=True) == pytest.approx(29.9503799100084)
