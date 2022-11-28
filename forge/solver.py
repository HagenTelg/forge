import typing
from math import isfinite, sqrt
from cmath import sqrt as csqrt


def newton_raphson(y_target: float, evaluate: typing.Callable[[float], float], x0: float = 0.0, x1: float = None,
                   x_epsilon: float = 1E-6, max_iterations: int = 20) -> float:
    if not isfinite(y_target) or not isfinite(x0):
        raise ValueError
    if x1 is None:
        if x0 == 0.0:
            x1 = 0.1
        else:
            x1 = x0 * 0.9
    elif not isfinite(x1):
        raise ValueError

    y0 = evaluate(x0)
    if not isfinite(y0):
        raise ValueError

    for i in range(max_iterations):
        y1 = evaluate(x1)
        if not isfinite(y1):
            raise ValueError
        if y1 == y0:
            return x1
        dX = ((x1 - x0) / (y1 - y0)) * (y_target - y1)
        if abs(dX) < x_epsilon:
            return x1 + dX
        x0 = x1
        y0 = y1
        x1 += dX

    raise OverflowError


def _poly_2nd_order(coefficients: typing.List[float], value: float) -> typing.List[float]:
    d = coefficients[1] ** 2 - 4.0 * coefficients[2] * (coefficients[0] - value)
    if d < 0.0:
        return []
    d = sqrt(d)
    negative = (-coefficients[1] - d) / (2.0 * coefficients[2])
    positive = (-coefficients[1] + d) / (2.0 * coefficients[2])
    return [negative, positive]


def _poly_3nd_order(coefficients: typing.List[float], value: float) -> typing.List[float]:
    a = complex(coefficients[3], 0.0)
    b = complex(coefficients[2], 0.0)
    c = complex(coefficients[1], 0.0)
    d = complex(coefficients[0] - value, 0.0)
    i1 = 2.0 * b * b * b - 9.0 * a * b * c + 27.0 * a * a * d
    i2 = b * b - 3.0 * a * c
    Q = csqrt(i1 * i1 - 4.0 * i2 * i2 * i2)
    C = (0.5 * (Q + 2.0 * b * b * b - 9.0 * a * b * c + 27.0 * a * a * d)) ** (1.0 / 3.0)
    if abs(C) == 0.0:
        return []
    x1 = -b / (3.0 * a) - C / (3.0 * a) - i2 / (3.0 * a * C)
    i3 = complex(1.0, sqrt(3.0))
    i4 = complex(1.0, -sqrt(3.0))
    x2 = -b / (3.0 * a) + (C * i3) / (6.0 * a) + (i4 * i2) / (6.0 * a * C)
    x3 = -b / (3.0 * a) + (C * i4) / (6.0 * a) + (i3 * i2) / (6.0 * a * C)

    epsilon0 = 1E-8
    if abs(x1.imag) > epsilon0:
        if abs(x2.imag) > epsilon0:
            if abs(x3.imag) > epsilon0:
                return []
            return [x3.real]
        elif abs(x3.imag) > epsilon0:
            return [x2.real]

        return [x2.real, x3.real]
    elif abs(x2.imag) > epsilon0:
        if abs(x3.imag) > epsilon0:
            return [x1.real]
        return [x1.real, x3.real]
    elif abs(x3.imag) > epsilon0:
        return [x1.real, x2.real]
    else:
        return [x1.real, x2.real, x3.real]


def _poly_Nth_order(coefficients: typing.List[float], value: float, guess: float) -> typing.List[float]:
    def apply(v: float) -> float:
        result = 0.0
        accumulator = 1.0
        for c in coefficients:
            result += c * accumulator
            accumulator *= v
        return result

    epsilon0 = 1E-9
    x0 = guess
    for _ in range(500):
        dYdX = coefficients[1]
        x = 1.0
        for i in range(1, len(coefficients)):
            dYdX += i * x * coefficients[i]
            x *= x0
        if dYdX == 0.0 or abs(dYdX) < epsilon0:
            break

        y = apply(x0)
        eeps = abs(y) * epsilon0
        if eeps < epsilon0:
            eeps = epsilon0
        dy = value - y
        if abs(dy) < eeps:
            break

        x0 += dy / dYdX

    result = apply(x0)
    eeps = (abs(result) + abs(value)) * epsilon0 * 2.0
    if eeps < epsilon0:
        eeps = epsilon0
    if abs(value - result) > eeps:
        return []
    return [x0]


def polynomial(poly: typing.Iterable[float], value: float = 0.0, guess: float = 0.0) -> typing.List[float]:
    value = float(value)
    if not isfinite(value):
        raise ValueError
    coefficients: typing.List[float] = list()
    for v in poly:
        v = float(v)
        if not isfinite(v):
            raise ValueError
        coefficients.append(v)
    while len(coefficients) > 0 and coefficients[-1] == 0.0:
        del coefficients[-1]
    if len(coefficients) < 2:
        return []
    elif len(coefficients) == 2:
        return [(value - coefficients[0]) / coefficients[1]]
    elif len(coefficients) == 3:
        return _poly_2nd_order(coefficients, value)
    elif len(coefficients) == 4:
        return _poly_3nd_order(coefficients, value)
    else:
        return _poly_Nth_order(coefficients, value, guess)
