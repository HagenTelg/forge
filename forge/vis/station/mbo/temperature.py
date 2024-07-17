import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries
from ..default.met.temperature import Temperature as BaseTemperature


class Temperature(BaseTemperature):
    def __init__(self, mode: str, **kwargs):
        super().__init__(f'{mode}-temperature', OrderedDict([
            ('{code}sample', '{code}_V11 (sample)'),
            ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
            ('{code}neph', '{code}_S11 (neph sample)'),
            ('{code}ambient', 'Ambient {type}'),
            ('{code}room', 'Room {type}'),
            ('{code}room2', 'Room 2 {type}'),
            ('{code}cr1000', 'CR1000 Panel {type}'),
        ]), {'TDnephinlet', 'TDroom', 'Uroom', 'TDroom2', 'Uroom2', 'TDcr1000', 'Ucr1000'}, **kwargs)
        self.title = "System Conditions"
        for graph in self.graphs:
            graph.contamination = f'{mode}-contamination'


class Ambient(BaseTemperature):
    def __init__(self, mode: str, **kwargs):
        super().__init__(f'{mode}-ambient', OrderedDict([
            ('{code}ambient', 'Ambient {type}'),
            ('{code}sheltered', 'Sheltered {type}'),
        ]), **kwargs)
        self.title = "Ambient Conditions"
        for graph in self.graphs:
            graph.contamination = f'{mode}-contamination'
