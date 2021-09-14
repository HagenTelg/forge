import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries
from ..default.met.temperature import Temperature as BaseTemperature


class Temperature(BaseTemperature):
    def __init__(self, mode: str):
        super().__init__(f'{mode}-temperature', OrderedDict([
            ('{code}sample', '{code}_V11 (sample)'),
            ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
            ('{code}neph', '{code}_S11 (neph sample)'),
            ('{code}ambient', 'Ambient {type}'),
            ('{code}room', 'Room {type}'),
            ('{code}cr1000', 'CR1000 Panel {type}'),
        ]), {'TDnephinlet', 'TDroom', 'Uroom', 'TDcr1000', 'Ucr1000'})
        self.title = "System Conditions"
        for graph in self.graphs:
            graph.contamination = f'{mode}-contamination'

        thermodenuder = TimeSeries.Graph()
        thermodenuder.title = "Thermodenuder"
        thermodenuder.contamination = f'{mode}-contamination'
        self.graphs.append(thermodenuder)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        thermodenuder.axes.append(T_C)

        section1 = TimeSeries.Trace(T_C)
        section1.legend = "Thermodenuder Section 1"
        section1.data_record = f'{mode}-temperature'
        section1.data_field = 'Tthermodenuder1'
        thermodenuder.traces.append(section1)

        section2 = TimeSeries.Trace(T_C)
        section2.legend = "Thermodenuder Section 2"
        section2.data_record = f'{mode}-temperature'
        section2.data_field = 'Tthermodenuder2'
        thermodenuder.traces.append(section2)


class Ambient(BaseTemperature):
    def __init__(self, mode: str):
        super().__init__(f'{mode}-ambient', OrderedDict([
            ('{code}ambient', 'Ambient {type}'),
            ('{code}sheltered', 'Sheltered {type}'),
        ]))
        self.title = "Ambient Conditions"
        for graph in self.graphs:
            graph.contamination = f'{mode}-contamination'
