import typing
from math import isfinite


class DataDisplay:
    def __init__(self, name: typing.Optional[str] = None, value_format: typing.Optional[str] = None):
        self.name = name
        self.value_format = value_format

    @staticmethod
    def apply_default_format(value: float) -> str:
        if value is None:
            return ""
        value = float(value)
        if not isfinite(value):
            return ""

        if abs(value) < 99.99:
            return f"{value:.2f}"
        elif abs(value) < 999.9:
            return f"{value:.1f}"
        elif abs(value) < 99999:
            return f"{value:.0f}"
        return f"{value:.2E}"

    def apply_format(self, value: float) -> str:
        if self.value_format is None:
            return self.apply_default_format(value)

        if value is None:
            return ""
        value = float(value)
        if not isfinite(value):
            return ""
        return self.value_format.format(value)


display: typing.Dict[str, typing.Optional[DataDisplay]] = dict()
