import typing
import numpy as np
from math import isfinite, sqrt, nan
from cmath import sqrt as csqrt


def _newton_raphson_array(
        y_target: np.ndarray,
        evaluate: typing.Callable[[np.ndarray], np.ndarray],
        x0: np.ndarray = None, x1: np.ndarray = None,
        x_epsilon: float = 1E-6, max_iterations: int = 20,
) -> np.ndarray:
    if x0 is None:
        x0 = np.full_like(y_target, 0)
    elif isinstance(x0, (int, float)):
        x0 = np.full_like(y_target, x0)
    else:
        x0 = np.array(x0)

    if x1 is None:
        x1 = x0 * 0.9
        replace = x1 == x0
        x1[replace] = x0[replace] + 0.1
    elif isinstance(x1, (int, float)):
        x1 = np.full_like(y_target, x1)
    else:
        x1 = np.array(x1)

    iterating = np.full(y_target.shape, True, dtype=bool)
    y0 = evaluate(x0)
    for _ in range(max_iterations):
        y1 = evaluate(x1[iterating])
        dX = ((x1[iterating] - x0[iterating]) / (y1 - y0[iterating])) * (y_target[iterating] - y1)

        y0[iterating] = y1
        x0[iterating] = x1[iterating]
        x1[iterating] += dX

        continue_iteration = np.abs(dX) > x_epsilon
        if not np.any(continue_iteration):
            break
        iterating[iterating] = continue_iteration

    return x1


def newton_raphson(
        y_target: typing.Union[float, np.ndarray],
        evaluate: typing.Union[typing.Callable[[float], float], typing.Callable[[np.ndarray], np.ndarray]],
        x0: typing.Union[float, np.ndarray] = 0.0, x1: typing.Union[float, np.ndarray] = None,
        x_epsilon: float = 1E-6, max_iterations: int = 20
) -> typing.Union[float, np.ndarray]:
    if not isinstance(y_target, (int, float)):
        return _newton_raphson_array(y_target, evaluate, x0, x1, x_epsilon, max_iterations)

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


def _array_poly_2nd_order(coefficients: np.ndarray, value: np.ndarray) -> np.ndarray:
    d = coefficients[1] ** 2 - 4.0 * coefficients[2] * (coefficients[0] - value)
    valid = d >= 0
    d[valid] = np.sqrt(d[valid])
    d[np.invert(valid)] = nan
    negative = (-coefficients[1] - d) / (2.0 * coefficients[2])
    positive = (-coefficients[1] + d) / (2.0 * coefficients[2])
    return np.stack((negative, positive), axis=-1)


def _poly_3rd_order(coefficients: typing.List[float], value: float) -> typing.List[float]:
    a = complex(coefficients[3], 0.0)
    b = complex(coefficients[2], 0.0)
    c = complex(coefficients[1], 0.0)
    d = complex(coefficients[0] - value, 0.0)
    i1 = 2.0 * b * b * b - 9.0 * a * b * c + 27.0 * a * a * d
    i2 = b * b - 3.0 * a * c
    Q = csqrt(i1 * i1 - 4.0 * i2 * i2 * i2)
    C = (0.5 * (Q + 2.0 * b * b * b - 9.0 * a * b * c + 27.0 * a * a * d)) ** (1.0 / 3.0)
    if abs(a * C) < 1E-8:
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


def _array_poly_3rd_order(coefficients: np.ndarray, value: np.ndarray) -> np.ndarray:
    a = complex(coefficients[3], 0.0)
    b = complex(coefficients[2], 0.0)
    c = complex(coefficients[1], 0.0)
    d = np.array(coefficients[0] - value, dtype=np.complex128)

    with np.errstate(divide='ignore', invalid='ignore'):
        i1 = 2.0 * b * b * b - 9.0 * a * b * c + 27.0 * a * a * d
        i2 = b * b - 3.0 * a * c
        Q = np.sqrt(i1 * i1 - 4.0 * i2 * i2 * i2)
        C = (0.5 * (Q + 2.0 * b * b * b - 9.0 * a * b * c + 27.0 * a * a * d)) ** (1.0 / 3.0)

        C[np.abs(a * C) <= 1E-8] = complex(nan, nan)
        x1 = -b / (3.0 * a) - C / (3.0 * a) - i2 / (3.0 * a * C)
        i3 = complex(1.0, sqrt(3.0))
        i4 = complex(1.0, -sqrt(3.0))
        x2 = -b / (3.0 * a) + (C * i3) / (6.0 * a) + (i4 * i2) / (6.0 * a * C)
        x3 = -b / (3.0 * a) + (C * i4) / (6.0 * a) + (i3 * i2) / (6.0 * a * C)

    result = np.full((*value.shape, 3), nan, dtype=np.float64)

    def set_result(values: np.ndarray, index: int):
        assign = np.abs(np.imag(values)) < 1E-8
        result[assign, index] = np.real(values[assign])

    set_result(x1, 0)
    set_result(x2, 1)
    set_result(x3, 2)

    return result


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


def _array_poly_Nth_order(coefficients: np.ndarray, value: np.ndarray, guess: np.ndarray) -> np.ndarray:
    poly = np.polynomial.Polynomial(coefficients)
    first_der = poly.deriv()

    epsilon0 = 1E-9
    x0 = np.full(value.shape, guess, dtype=np.float64)
    iterating = np.full(value.shape, True, dtype=bool)
    for _ in range(500):
        dYdX = first_der(x0[iterating])
        y = poly(x0[iterating])

        eeps = np.abs(y) * epsilon0
        eeps[eeps < epsilon0] = epsilon0
        dy = value - y
        continue_iteration = np.abs(dy) > eeps

        if not np.any(continue_iteration):
            break
        iterating[iterating] = continue_iteration

        x0[iterating] += dy[continue_iteration] / dYdX[continue_iteration]

    result = poly(x0)
    eeps = (np.abs(result) + np.abs(value)) * epsilon0 * 2.0
    eeps[eeps < epsilon0] = epsilon0
    x0[np.abs(value - result) > eeps] = nan
    return x0.reshape((*x0.shape, 1))


def _array_polynomial(poly: np.ndarray, value: np.ndarray, guess: np.ndarray = None) -> np.ndarray:
    value = np.asarray(value, dtype=np.float64)

    poly = np.asarray(poly, dtype=np.float64)
    if len(poly.shape) != 1:
        raise ValueError
    poly = np.trim_zeros(poly, "b")
    if poly.shape[0] < 2:
        return np.full(tuple([1, *value.shape]), nan, dtype=np.float64)

    elif poly.shape[0] == 2:
        root = (value - poly[0]) / poly[1]
        return root.reshape((*root.shape, 1))
    elif poly.shape[0] == 3:
        return _array_poly_2nd_order(poly, value)
    elif poly.shape[0] == 4:
        return _array_poly_3rd_order(poly, value)
    else:
        if guess is None:
            guess = np.zeros_like(value, dtype=np.float64)
        return _array_poly_Nth_order(poly, value, guess)


def polynomial(
        poly: typing.Union[typing.Iterable[float], np.ndarray],
        value: typing.Union[float, np.ndarray] = 0.0,
        guess: typing.Union[float, np.ndarray] = None,
) -> typing.Union[typing.List[float], np.ndarray]:
    if not isinstance(value, (int, float)):
        return _array_polynomial(poly, value, guess)

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

    if guess is not None:
        guess = 0.0

    if len(coefficients) < 2:
        return []
    elif len(coefficients) == 2:
        return [(value - coefficients[0]) / coefficients[1]]
    elif len(coefficients) == 3:
        return _poly_2nd_order(coefficients, value)
    elif len(coefficients) == 4:
        return _poly_3rd_order(coefficients, value)
    else:
        return _poly_Nth_order(coefficients, value, guess)
