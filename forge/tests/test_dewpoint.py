import typing
import pytest
import numpy as np
from math import nan
from forge.dewpoint import dewpoint, rh, temperature, extrapolate_rh


def test_dewpoint():
    assert dewpoint(20.0, 10.0) == pytest.approx(-11.179408058084732)
    assert dewpoint(30.0, 10.0) == pytest.approx(-4.349123215490977)
    assert dewpoint(30.0, 80.0) == pytest.approx(26.1684418167009)
    assert dewpoint(30.0, 80.0, over_water=True) == pytest.approx(26.1684418167009)
    assert dewpoint(30.0, 10.0, over_water=True) == pytest.approx(-4.9092699810922795)
    assert dewpoint(-10.0, 10.0) == pytest.approx(-33.597234223073)
    assert dewpoint(-10.0, 10.0, over_water=True) == pytest.approx(-35.933420551139704)

    assert dewpoint(np.array([20.0, 30.0, 30.0, -10.0, nan]), np.array([10.0, 10.0, 80.0, 10.0, 15.0])).tolist() == pytest.approx([
        -11.179408058084732, -4.349123215490977, 26.1684418167009, -33.597234223073, nan
    ], nan_ok=True)
    assert dewpoint(np.array([30.0, 30.0, -10.0, 15.0]), np.array([80.0, 10.0, 10.0, nan]), over_water=True).tolist() == pytest.approx([
        26.1684418167009, -4.9092699810922795, -35.933420551139704, nan
    ], nan_ok=True)


def test_rh():
    assert rh(30.0, 28.0) == pytest.approx(89.0767330629172)
    assert rh(30.0, 30.0) == pytest.approx(100.0)
    assert rh(30.0, -10.0) == pytest.approx(6.119359265186419)
    assert rh(30.0, -10.0, over_water=True) == pytest.approx(6.744961275673876)
    assert rh(-5.0, -10.0) == pytest.approx(64.686228666108)
    assert rh(-5.0, -10.0, over_water=True) == pytest.approx(67.91466729065742)

    assert rh(np.array([30.0, 30.0, -5.0, nan]), np.array([28.0, 30.0, -10.0, 5.0])).tolist() == pytest.approx([
        89.0767330629172, 100.0, 64.686228666108, nan
    ], nan_ok=True)
    assert rh(np.array([30.0, -5.0, 5.0]), np.array([-10.0, -10.0, nan]), over_water=True).tolist() == pytest.approx([
        6.744961275673876, 67.91466729065742, nan
    ], nan_ok=True)


def test_temperature():
    assert temperature(89.0767330629172, 28.0) == pytest.approx(30.0)
    assert temperature(100.0, 30.0) == pytest.approx(30.0)
    assert temperature(6.119359265186419, -10.0) == pytest.approx(30.0)
    assert temperature(6.744961275673876, -10.0, over_water=True) == pytest.approx(30.0)
    assert temperature(64.686228666108, -10.0) == pytest.approx(-5.0)
    assert temperature(67.91466729065742, -10.0, over_water=True) == pytest.approx(-5.0)

    assert temperature(np.array([89.0767330629172, 100.0, 6.119359265186419, 64.686228666108, nan]),
                       np.array([28.0, 30.0, -10.0, -10.0, 5.0])).tolist() == pytest.approx([
        30.0, 30.0, 30.0, -5.0, nan
    ], nan_ok=True)
    assert temperature(np.array([6.744961275673876, 67.91466729065742, 5.0]),
                       np.array([-10.0, -10.0, nan]), over_water=True).tolist() == pytest.approx([
        30.0, -5.0, nan
    ], nan_ok=True)


def test_extrapolate_rh():
    assert extrapolate_rh(10.0, 20.0, 15.0) == pytest.approx(14.400557642334947)
    assert extrapolate_rh(-10.0, 20.0, -15.0) == pytest.approx(31.448302796248)
    assert extrapolate_rh(-10.0, 20.0, -15.0, over_water=True) == pytest.approx(29.9503350757957)

    assert extrapolate_rh(np.array([10.0, -10.0, nan]),
                          np.array([20.0, 20.0, 5.0]),
                          np.array([15.0, -15.0, nan])).tolist() == pytest.approx([
        14.400557642334947, 31.448302796248, nan
    ], nan_ok=True)
    assert extrapolate_rh(np.array([nan, -10.0]),
                          np.array([nan, 20.0]),
                          np.array([nan, -15.0]), over_water=True).tolist() == pytest.approx([
        nan, 29.9503350757957
    ], nan_ok=True)
