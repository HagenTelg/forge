import typing
from math import isfinite


def format_bytes(n: typing.Optional[typing.Union[int, float]]) -> str:
    if n is None:
        return "--- B"
    n = float(n)
    if not isfinite(n):
        return "--- B"
    divisor = 1
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        divided = n / divisor
        if divided > 999.0:
            divisor *= 1024
            continue

        if divisor == 1:
            return f"{divided:.0f} {u}"
        elif divided <= 9.99:
            return f"{divided:.2f} {u}"
        elif divided <= 99.9:
            return f"{divided:.1f} {u}"
        else:
            return f"{divided:.0f} {u}"
