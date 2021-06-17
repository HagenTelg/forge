import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries


class Temperature(TimeSeries):
    class CalculateMissing(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('numeric_solve')
            self.components.append('dewpoint')
            self.script = r"""(function(dataName) { return new Dewpoint.CalculateDispatch(dataName); })"""

    def __init__(self, record: str, measurements: typing.Optional[typing.Dict[str, str]] = None,
                 omit_traces: typing.Optional[typing.Set[str]] = None):
        super().__init__()
        self.title = "Ambient Conditions"

        self.processing[record] = self.CalculateMissing()

        if measurements is None:
            measurements = OrderedDict([
                ('{code}ambient', '{type}'),
            ])

        rh = TimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)
        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)
        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperature.axes.append(T_C)

        dewpoint = TimeSeries.Graph()
        dewpoint.title = "Dewpoint"
        self.graphs.append(dewpoint)
        TD_C = TimeSeries.Axis()
        TD_C.title = "°C"
        TD_C.format_code = '.1f'
        dewpoint.axes.append(TD_C)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(rh_percent)
            trace.legend = legend.format(type='RH', code='U')
            trace.data_record = record
            trace.data_field = field.format(code='U')
            if not omit_traces or trace.data_field not in omit_traces:
                rh.traces.append(trace)

            trace = TimeSeries.Trace(T_C)
            trace.legend = legend.format(type='Temperature', code='T')
            trace.data_record = record
            trace.data_field = field.format(code='T')
            if not omit_traces or trace.data_field not in omit_traces:
                temperature.traces.append(trace)

            trace = TimeSeries.Trace(TD_C)
            trace.legend = legend.format(type='Dewpoint', code='TD')
            trace.data_record = record
            trace.data_field = field.format(code='TD')
            if not omit_traces or trace.data_field not in omit_traces:
                dewpoint.traces.append(trace)
