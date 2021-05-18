import typing
from collections import OrderedDict
from ..met.temperature import Temperature as BaseTemperature


class Temperature(BaseTemperature):
    def __init__(self, mode: str, measurements: typing.Optional[typing.Dict[str, str]] = None):
        if measurements is None:
            measurements = OrderedDict([
                ('{code}inlet', '{code}_V51 (inlet)'),
                ('{code}sample', '{code}_V11 (sample)'),
                ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
                ('{code}neph', '{code}_S11 (neph sample)'),
                ('{code}aux', 'Auxiliary {type}'),
                ('{code}ambient', 'Ambient {type}'),
            ])
        super().__init__(f'{mode}-temperature', measurements)
        self.title = "System Conditions"
