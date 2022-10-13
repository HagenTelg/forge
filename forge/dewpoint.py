import typing
from math import isfinite, nan, exp, log, sqrt
from forge.solver import newton_raphson


def svp_over_ice(t_kelvin: float) -> float:
    if not isfinite(t_kelvin) or t_kelvin <= 0.0:
        return nan

    """
    See http://www.decatur.de/javascript/dew/dew.js
    Saturation Vapor Pressure formula for range -100..0 Deg. C.
    This is taken from
        ITS-90 Formulations for Vapor Pressure, Frostpoint Temperature,
        Dewpoint Temperature, and Enhancement Factors in the Range 100 to +100 C
    by Bob Hardy
    as published in "The Proceedings of the Third International Symposium on Humidity & Moisture",
    Teddington, London, England, April 1998
    """

    return exp(-5.8666426e3 / t_kelvin +
               2.232870244e1 +
               (1.39387003e-2 + (-3.4262402e-5 + (2.7040955e-8 * t_kelvin)) * t_kelvin) * t_kelvin +
               6.7063522e-1 * log(t_kelvin))


def svp_over_water(t_kelvin: float) -> float:
    if not isfinite(t_kelvin) or t_kelvin <= 0.0:
        return nan

    """
    See http://www.decatur.de/javascript/dew/dew.js
    Saturation Vapor Pressure formula for range 273..678 Deg. K.
    This is taken from the
        Release on the IAPWS Industrial Formulation 1997
        for the Thermodynamic Properties of Water and Steam
    by IAPWS (International Association for the Properties of Water and Steam),
    Erlangen, Germany, September 1997.

    This is Equation (30) in Section 8.1 "The Saturation-Pressure Equation (Basic Equation)"
    """

    th = t_kelvin - 0.23855557567849 / (t_kelvin - 0.11670521452767e40)
    A = (th + 0.11670521452767e4) * th - 0.72421316703206e6
    B = (-0.17073846940092e2 * th + 0.12020824702470e5) * th - 0.32325550322333e7
    C = (0.14915108613530e2 * th - 0.48232657361591e4) * th + 0.40511340542057e6

    p = 2.0 * C / (-B + sqrt(B * B - 4 * A * C))
    p *= p
    p *= p
    return p * 1e6


def svp(t_kelvin: float, over_water: bool = False):
    if not over_water and t_kelvin < 273.15:
        return svp_over_ice(t_kelvin)
    return svp_over_water(t_kelvin)


def _svp_solve(svp_target: float, t_initial: float = None, over_water: bool = False) -> float:
    x1 = None
    if t_initial is not None:
        x1 = t_initial + 1.0
    try:
        return newton_raphson(svp_target, lambda t: svp(t, over_water=over_water), x0=t_initial, x1=x1)
    except (ValueError, OverflowError):
        return nan


def dewpoint(t: float, rh: float, over_water: bool = False) -> float:
    if not isfinite(t) or not isfinite(rh):
        return nan
    if rh <= 0.0 or rh > 100.0 or t < -100.0 or t > 400.0:
        return nan
    t += 273.15

    svp_target = svp(t, over_water=over_water)
    if not isfinite(svp_target):
        return nan
    svp_target *= (rh / 100.0)

    return _svp_solve(svp_target, t_initial=t, over_water=over_water) - 273.15


def rh(t: float, dewpoint: float, over_water: bool = False) -> float:
    if not isfinite(t) or not isfinite(dewpoint):
        return nan
    if t < -100.0 or t > 400.0 or dewpoint < -100.0 or dewpoint > 400.0:
        return nan
    t += 273.15
    dewpoint += 273.15

    svp_t = svp(t, over_water=over_water)
    if not isfinite(svp_t) or svp_t == 0.0:
        return nan
    svp_dewpoint = svp(dewpoint, over_water=over_water)
    if not isfinite(svp_dewpoint):
        return nan
    return (svp_dewpoint / svp_t) * 100.0


def temperature(rh: float, dewpoint: float, over_water: bool = False) -> float:
    if not isfinite(dewpoint) or not isfinite(rh):
        return nan
    if rh <= 0.0 or rh > 100.0 or dewpoint < -100.0 or dewpoint > 400.0:
        return nan
    dewpoint += 273.15

    svp_target = svp(dewpoint, over_water=over_water)
    if not isfinite(svp_target):
        return nan
    svp_target /= (rh / 100.0)

    return _svp_solve(svp_target, t_initial=dewpoint, over_water=over_water) - 273.15


def extrapolate_rh(t_of_rh: float, rh_measured: float, t_target: float, over_water: bool = False) -> float:
    return rh(t_target, dewpoint(t_of_rh, rh_measured, over_water=over_water), over_water=over_water)
