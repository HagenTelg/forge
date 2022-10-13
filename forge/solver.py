import typing
from math import isfinite, nan


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
