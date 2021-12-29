import typing
from collections import OrderedDict
from ..met.temperature import Temperature as BaseTemperature


class Temperature(BaseTemperature):
    def __init__(self, mode: str, measurements: typing.Optional[typing.Dict[str, str]] = None,
                 omit_traces: typing.Optional[typing.Set[str]] = None, **kwargs):
        if measurements is None:
            measurements = OrderedDict([
                ('{code}inlet', '{code}_V51 (inlet)'),
                ('{code}sample', '{code}_V11 (sample)'),
                ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
                ('{code}neph', '{code}_S11 (neph sample)'),
                ('{code}aux', 'Auxiliary {type}'),
                ('{code}ambient', 'Ambient {type}'),
            ])
        if omit_traces is None:
            omit_traces = {'TDnephinlet'}
        super().__init__(f'{mode}-temperature', measurements, omit_traces, **kwargs)
        self.title = "System Conditions"
        for graph in self.graphs:
            graph.contamination = f'{mode}-contamination'


