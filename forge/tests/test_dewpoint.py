import typing
import pytest
from forge.dewpoint import dewpoint, rh, temperature, extrapolate_rh


def test_dewpoint():
    assert dewpoint(20.0, 10.0) == pytest.approx(-11.179408058084732)
    assert dewpoint(30.0, 10.0) == pytest.approx(-4.349123215490977)
    assert dewpoint(30.0, 80.0) == pytest.approx(26.1684418167009)
    assert dewpoint(30.0, 80.0, over_water=True) == pytest.approx(26.1684418167009)
    assert dewpoint(30.0, 10.0, over_water=True) == pytest.approx(-4.9092699810922795)
    assert dewpoint(-10.0, 10.0) == pytest.approx(-33.597234223073)
    assert dewpoint(-10.0, 10.0, over_water=True) == pytest.approx(-35.933420551139704)


def test_rh():
    assert rh(30.0, 28.0) == pytest.approx(89.0767330629172)
    assert rh(30.0, 30.0) == pytest.approx(100.0)
    assert rh(30.0, -10.0) == pytest.approx(6.119359265186419)
    assert rh(30.0, -10.0, over_water=True) == pytest.approx(6.744961275673876)
    assert rh(-5.0, -10.0) == pytest.approx(64.686228666108)
    assert rh(-5.0, -10.0, over_water=True) == pytest.approx(67.91466729065742)


def test_temperature():
    assert temperature(89.0767330629172, 28.0) == pytest.approx(30.0)
    assert temperature(100.0, 30.0) == pytest.approx(30.0)
    assert temperature(6.119359265186419, -10.0) == pytest.approx(30.0)
    assert temperature(6.744961275673876, -10.0, over_water=True) == pytest.approx(30.0)
    assert temperature(64.686228666108, -10.0) == pytest.approx(-5.0)
    assert temperature(67.91466729065742, -10.0, over_water=True) == pytest.approx(-5.0)


def test_extrapolate_rh():
    assert extrapolate_rh(10.0, 20.0, 15.0) == pytest.approx(14.400557642334947)
    assert extrapolate_rh(-10.0, 20.0, -15.0) == pytest.approx(31.448302796248)
    assert extrapolate_rh(-10.0, 20.0, -15.0, over_water=True) == pytest.approx(29.9503350757957)
