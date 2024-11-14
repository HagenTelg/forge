import typing
import numpy as np
from math import isfinite, nan, exp, log, sqrt
from forge.solver import newton_raphson
from forge.units import ZERO_C_IN_K


def svp_over_ice(t_kelvin: typing.Union[float, np.ndarray]) -> typing.Union[float, np.ndarray]:
    """
    See http://www.decatur.de/javascript/dew/dew.js
    Saturation Vapor Pressure formula for range -100..0 Deg. C.
    This is taken from
        ITS-90 Formulations for Vapor Pressure, Frostpoint Temperature,
        Dewpoint Temperature, and Enhancement Factors in the Range 100 to +100 C
    by Bob Hardy
    as published in "The Proceedings of the Third International Symposium on Humidity & Moisture",
    Teddington, London, England, April 1998

    :param t_kelvin: the temperature in kelvin
    :returns: the saturation vapor pressure
    """

    if not isinstance(t_kelvin, (int, float)):
        result = np.full_like(t_kelvin, np.nan, dtype=np.float64)
        valid = t_kelvin > 0.0
        t_kelvin = t_kelvin[valid]
        result[valid] = np.exp(-5.8666426e3 / t_kelvin + 2.232870244e1 +
                               (1.39387003e-2 + (-3.4262402e-5 + (2.7040955e-8 * t_kelvin)) * t_kelvin) * t_kelvin +
                               6.7063522e-1 * np.log(t_kelvin))
        return result
    else:
        if not isfinite(t_kelvin) or t_kelvin <= 0.0:
            return nan

        return exp(-5.8666426e3 / t_kelvin +
                   2.232870244e1 +
                   (1.39387003e-2 + (-3.4262402e-5 + (2.7040955e-8 * t_kelvin)) * t_kelvin) * t_kelvin +
                   6.7063522e-1 * log(t_kelvin))


def svp_over_water(t_kelvin: typing.Union[float, np.ndarray]) -> typing.Union[float, np.ndarray]:
    """
    See http://www.decatur.de/javascript/dew/dew.js
    Saturation Vapor Pressure formula for range 273..678 Deg. K.
    This is taken from the
        Release on the IAPWS Industrial Formulation 1997
        for the Thermodynamic Properties of Water and Steam
    by IAPWS (International Association for the Properties of Water and Steam),
    Erlangen, Germany, September 1997.

    This is Equation (30) in Section 8.1 "The Saturation-Pressure Equation (Basic Equation)"

    :param t_kelvin: the temperature in kelvin
    :returns: the saturation vapor pressure
    """

    if not isinstance(t_kelvin, (int, float)):
        result = np.full_like(t_kelvin, np.nan, dtype=np.float64)
        valid = t_kelvin > 0.0
        t_kelvin = t_kelvin[valid]

        th = t_kelvin - 0.23855557567849 / (t_kelvin - 0.65017534844798e3)
        A = (th + 0.11670521452767e4) * th - 0.72421316703206e6
        B = (-0.17073846940092e2 * th + 0.12020824702470e5) * th - 0.32325550322333e7
        C = (0.14915108613530e2 * th - 0.48232657361591e4) * th + 0.40511340542057e6

        p = 2.0 * C / (-B + np.sqrt(B * B - 4 * A * C))
        p *= p
        p *= p
        result[valid] = p * 1e6
        return result
    else:
        if not isfinite(t_kelvin) or t_kelvin <= 0.0:
            return nan

        th = t_kelvin - 0.23855557567849 / (t_kelvin - 0.65017534844798e3)
        A = (th + 0.11670521452767e4) * th - 0.72421316703206e6
        B = (-0.17073846940092e2 * th + 0.12020824702470e5) * th - 0.32325550322333e7
        C = (0.14915108613530e2 * th - 0.48232657361591e4) * th + 0.40511340542057e6

        p = 2.0 * C / (-B + sqrt(B * B - 4 * A * C))
        p *= p
        p *= p
        return p * 1e6


def svp(t_kelvin: typing.Union[float, np.ndarray], over_water: bool = False) -> typing.Union[float, np.ndarray]:
    if not isinstance(t_kelvin, (int, float)):
        result_svp = svp_over_water(t_kelvin)
        if over_water:
            return result_svp
        over_ice = t_kelvin < ZERO_C_IN_K
        if not np.any(over_ice):
            return result_svp
        result_svp[over_ice] = svp_over_ice(t_kelvin[over_ice])
        return result_svp
    else:
        if not over_water and t_kelvin < ZERO_C_IN_K:
            return svp_over_ice(t_kelvin)
        return svp_over_water(t_kelvin)


def _svp_solve(svp_target: typing.Union[float, np.ndarray],
               t_initial: typing.Union[float, np.ndarray] = None,
               over_water: bool = False) -> typing.Union[float, np.ndarray]:
    x1 = None
    if t_initial is not None:
        x1 = t_initial + 1.0
    try:
        return newton_raphson(svp_target, lambda t: svp(t, over_water=over_water), x0=t_initial, x1=x1)
    except (ValueError, OverflowError):
        return nan


def dewpoint(t: typing.Union[float, np.ndarray], rh: typing.Union[float, np.ndarray],
             over_water: bool = False) -> typing.Union[float, np.ndarray]:
    if not isinstance(t, (int, float)) or not isinstance(rh, (int, float)):
        if isinstance(t, (int, float)):
            rh = np.array(rh)
            t = np.full_like(rh, t)
        else:
            t = np.array(t)
            if isinstance(rh, (int, float)):
                rh = np.full_like(t, rh)
            else:
                rh = np.array(rh)

        rh[rh < 0.0] = nan
        rh[rh > 100.0] = nan
        t[t < -100] = nan
        t[t > 400] = nan
        t += ZERO_C_IN_K

        svp_target = svp(t, over_water=over_water)
        svp_target *= (rh / 100.0)
        return _svp_solve(svp_target, t_initial=t, over_water=over_water) - ZERO_C_IN_K
    else:
        if not isfinite(t) or not isfinite(rh):
            return nan
        if rh <= 0.0 or rh > 100.0 or t < -100.0 or t > 400.0:
            return nan
        t += ZERO_C_IN_K

        svp_target = svp(t, over_water=over_water)
        if not isfinite(svp_target):
            return nan
        svp_target *= (rh / 100.0)

        return _svp_solve(svp_target, t_initial=t, over_water=over_water) - ZERO_C_IN_K


def rh(t: typing.Union[float, np.ndarray], dewpoint: typing.Union[float, np.ndarray],
       over_water: bool = False) -> typing.Union[float, np.ndarray]:
    if not isinstance(t, (int, float)) or not isinstance(dewpoint, (int, float)):
        if isinstance(t, (int, float)):
            dewpoint = np.array(dewpoint)
            t = np.full_like(dewpoint, t)
        else:
            t = np.array(t)
            if isinstance(dewpoint, (int, float)):
                dewpoint = np.full_like(t, dewpoint)
            else:
                dewpoint = np.array(dewpoint)

        t[t < -100] = nan
        t[t > 400] = nan
        dewpoint[dewpoint < -100] = nan
        dewpoint[dewpoint > 400] = nan
        t += ZERO_C_IN_K
        dewpoint += ZERO_C_IN_K

        svp_t = svp(t, over_water=over_water)
        svp_dewpoint = svp(dewpoint, over_water=over_water)
        result = np.full_like(t, np.nan, dtype=np.float64)
        valid = np.logical_and(svp_t != 0.0, np.isfinite(svp_t))
        result[valid] = (svp_dewpoint[valid] / svp_t[valid]) * 100.0
        return result
    else:
        if not isfinite(t) or not isfinite(dewpoint):
            return nan
        if t < -100.0 or t > 400.0 or dewpoint < -100.0 or dewpoint > 400.0:
            return nan
        t += ZERO_C_IN_K
        dewpoint += ZERO_C_IN_K

        svp_t = svp(t, over_water=over_water)
        if not isfinite(svp_t) or svp_t == 0.0:
            return nan
        svp_dewpoint = svp(dewpoint, over_water=over_water)
        if not isfinite(svp_dewpoint):
            return nan
        return (svp_dewpoint / svp_t) * 100.0


def temperature(rh: typing.Union[float, np.ndarray], dewpoint: typing.Union[float, np.ndarray],
                over_water: bool = False) -> typing.Union[float, np.ndarray]:
    if not isinstance(rh, (int, float)) or not isinstance(dewpoint, (int, float)):
        if isinstance(rh, (int, float)):
            dewpoint = np.array(dewpoint)
            rh = np.full_like(dewpoint, rh)
        else:
            rh = np.array(rh)
            if isinstance(dewpoint, (int, float)):
                dewpoint = np.full_like(rh, dewpoint)
            else:
                dewpoint = np.array(dewpoint)

        rh[rh < 0.0] = nan
        rh[rh > 100.0] = nan
        dewpoint[dewpoint < -100] = nan
        dewpoint[dewpoint > 400] = nan
        dewpoint += ZERO_C_IN_K

        svp_target = svp(dewpoint, over_water=over_water)
        svp_target /= (rh / 100.0)
        return _svp_solve(svp_target, t_initial=dewpoint, over_water=over_water) - ZERO_C_IN_K
    else:
        if not isfinite(dewpoint) or not isfinite(rh):
            return nan
        if rh <= 0.0 or rh > 100.0 or dewpoint < -100.0 or dewpoint > 400.0:
            return nan
        dewpoint += ZERO_C_IN_K

        svp_target = svp(dewpoint, over_water=over_water)
        if not isfinite(svp_target):
            return nan
        svp_target /= (rh / 100.0)

        return _svp_solve(svp_target, t_initial=dewpoint, over_water=over_water) - ZERO_C_IN_K


def extrapolate_rh(
        t_of_rh: typing.Union[float, np.ndarray],
        rh_measured: typing.Union[float, np.ndarray],
        t_target: typing.Union[float, np.ndarray],
        over_water: bool = False
) -> typing.Union[float, np.ndarray]:
    return rh(t_target, dewpoint(t_of_rh, rh_measured, over_water=over_water), over_water=over_water)
