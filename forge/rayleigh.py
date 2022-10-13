import typing
import bisect
from math import cos, sin, isfinite, nan, radians, ceil


# Calculations from http://www.opticsinfobase.org/ao/abstract.cfm?uri=ao-34-15-2765
# Applied Optics, Vol. 34, Issue 15, pp. 2765-2773 (1995)


# Constants are from Table 3 in Bucholtz 1995 with conversion to Mm-1 and
# changed to 0C, used with eq (8)
def _air_rayleigh(wavelength: float) -> float:
    wavelength *= 1E-3
    if wavelength < 0.5:
        A = 7.68246E-4 * 1E3 * (288.15 / 273.15)
        B = 3.55212
        C = 1.35579
        D = 0.11563
        return A * pow(wavelength, -1.0 * (B + C * wavelength + D / wavelength))
    else:
        A = 10.21675E-4 * 1E3 * (288.15 / 273.15)
        B = 3.99668
        C = 1.10298E-3
        D = 2.71393E-2
        return A * pow(wavelength, -1.0 * (B + C * wavelength + D / wavelength))


# Constants from Table 1 in Bucholtz 1995.  No analytic form for gamma
# given, so just do linear interpolation on them
_gamma_wavelength = [200, 205, 210, 215, 220, 225, 230, 240, 250, 260, 270, 280, 290, 300, 310, 320, 330, 340, 350,
                     360, 370, 380, 390, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000]
_gamma_values = [2.326E-2, 2.241E-2, 2.156E-2, 2.100E-2, 2.043E-2, 1.986E-2, 1.930E-2, 1.872E-2, 1.815E-2, 1.758E-2,
                 1.729E-2, 1.672E-2, 1.643E-2, 1.614E-2, 1.614E-2, 1.586E-2, 1.557E-2, 1.557E-2, 1.528E-2, 1.528E-2,
                 1.528E-2, 1.499E-2, 1.499E-2, 1.499E-2, 1.471E-2, 1.442E-2, 1.442E-2, 1.413E-2, 1.413E-2, 1.413E-2,
                 1.413E-2, 1.384E-2, 1.384E-2, 1.384E-2, 1.384E-2, 1.384E-2]


def _wavelength_gamma(wavelength: float) -> float:
    upper = bisect.bisect(_gamma_wavelength, wavelength)
    if upper == 0:
        upper = 1
        lower = 0
    elif upper >= len(_gamma_wavelength):
        upper = len(_gamma_wavelength) - 1
        lower = upper - 1
    else:
        lower = upper - 1
    slope = (_gamma_values[upper] - _gamma_values[lower]) / (_gamma_wavelength[upper] - _gamma_wavelength[lower])
    return _gamma_values[lower] + slope * (wavelength - _gamma_wavelength[lower])


# This is eq (12) in Bucholtz 1995
def _phase_function(gamma: float, angle: float) -> float:
    den = 4.0 * (1.0 + 2.0 * gamma)
    cs = cos(angle)
    return (3.0 / den) * ((1.0 + 3.0 * gamma) + (1.0 - gamma) * cs * cs)


# I'm not actually sure of the origin of these, but I got them from the Ecotech Neph manual
CO2 = 2.61
FM200 = 15.3
SF6 = 6.74
R12 = 15.31
R22 = 7.53
R134 = 7.35

RAYLEIGH_FACTOR: typing.Dict[str, float] = {
    "CO2": CO2,
    "CO₂": CO2,
    "FM200": FM200,
    "FM-200": FM200,
    "SF6": SF6,
    "SF₆": SF6,
    "R12": R12,
    "R-12": R12,
    "R22": R22,
    "R-22": R22,
    "R134": R134,
    "R-134": R134,
}


def rayleigh_scattering(wavelength: float, start_angle: float = 0.0, end_angle: float = 180.0) -> float:
    if not isfinite(wavelength):
        return nan
    if not isfinite(start_angle) or not isfinite( end_angle):
        return nan

    bs = _air_rayleigh(wavelength)
    n_steps = int(ceil(abs(end_angle - start_angle)))
    if n_steps < 100:
        n_steps = 100

    gamma = _wavelength_gamma(wavelength)
    start_angle = radians(start_angle)
    end_angle = radians(end_angle)
    delta_angle = end_angle - start_angle
    angle_step = delta_angle / float(n_steps)

    # Trapezoidal integration
    s = _phase_function(gamma, start_angle) + _phase_function(gamma, end_angle)
    for i in range(1, n_steps):
        angle = start_angle + angle_step * i
        s += 2.0 * _phase_function(gamma, angle) * sin(angle)
    s *= delta_angle / float(2 * n_steps)

    # Eq (14) canceled with the 2*pi from the phase integration
    s *= bs / 2.0
    return s

