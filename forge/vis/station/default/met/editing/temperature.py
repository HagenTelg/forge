import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries


class EditingTemperature(TimeSeries):
    class CalculateMissing(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('numeric_solve')
            self.components.append('dewpoint')
            self.script = r"""(function(dataName) { return new Dewpoint.CalculateDispatch(dataName); })"""

    def __init__(self, profile: str = 'met', measurements: typing.Optional[typing.Dict[str, str]] = None):
        super().__init__()
        self.title = "Temperature"

        if measurements is None:
            measurements = OrderedDict([
                ('{code}ambient', '{mode} at 2m'),
            ])

        self.processing[f'{profile}-raw-temperature'] = self.CalculateMissing()

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        raw.axes.append(T_C)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(T_C)
            trace.legend = legend.format(type='Temperature', code='T', mode='Edited')
            trace.data_record = f'{profile}-raw-temperature'
            trace.data_field = field.format(code='T')
            raw.traces.append(trace)


        self.processing[f'{profile}-editing-temperature'] = self.CalculateMissing()

        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        edited.axes.append(T_C)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(T_C)
            trace.legend = legend.format(type='Temperature', code='T', mode='Edited')
            trace.data_record = f'{profile}-editing-temperature'
            trace.data_field = field.format(code='T')
            edited.traces.append(trace)


class EditingDewpoint(TimeSeries):
    class CalculateMissing(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('numeric_solve')
            self.components.append('dewpoint')
            self.script = r"""(function(dataName) { return new Dewpoint.CalculateDispatch(dataName); })"""

    def __init__(self, profile: str = 'met', measurements: typing.Optional[typing.Dict[str, str]] = None):
        super().__init__()
        self.title = "Dewpoint"

        if measurements is None:
            measurements = OrderedDict([
                ('{code}ambient', '{mode} at 2m'),
            ])

        self.processing[f'{profile}-raw-temperature'] = self.CalculateMissing()

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        raw.axes.append(T_C)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(T_C)
            trace.legend = legend.format(type='Dewpoint', code='TD', mode='Raw')
            trace.data_record = f'{profile}-raw-temperature'
            trace.data_field = field.format(code='TD')
            raw.traces.append(trace)


        self.processing[f'{profile}-editing-temperature'] = self.CalculateMissing()

        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        edited.axes.append(T_C)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(T_C)
            trace.legend = legend.format(type='Dewpoint', code='TD', mode='Edited')
            trace.data_record = f'{profile}-editing-temperature'
            trace.data_field = field.format(code='TD')
            edited.traces.append(trace)


class EditingRH(TimeSeries):
    class CalculateMissing(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('numeric_solve')
            self.components.append('dewpoint')
            self.script = r"""(function(dataName) { return new Dewpoint.CalculateDispatch(dataName); })"""

    def __init__(self, profile: str = 'met', measurements: typing.Optional[typing.Dict[str, str]] = None):
        super().__init__()
        self.title = "Relative Humidity"

        if measurements is None:
            measurements = OrderedDict([
                ('{code}ambient', '{mode} at 2m'),
            ])

        self.processing[f'{profile}-raw-temperature'] = self.CalculateMissing()

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        raw.axes.append(rh_percent)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(rh_percent)
            trace.legend = legend.format(type='RH', code='U', mode='Raw')
            trace.data_record = f'{profile}-raw-temperature'
            trace.data_field = field.format(code='U')
            raw.traces.append(trace)


        self.processing[f'{profile}-editing-temperature'] = self.CalculateMissing()

        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        edited.axes.append(rh_percent)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(rh_percent)
            trace.legend = legend.format(type='RH', code='U', mode='Edited')
            trace.data_record = f'{profile}-editing-temperature'
            trace.data_field = field.format(code='U')
            edited.traces.append(trace)
