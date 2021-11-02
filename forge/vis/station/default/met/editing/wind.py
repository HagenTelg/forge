import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries


class EditingWindSpeed(TimeSeries):
    def __init__(self, profile: str = 'met', measurements: typing.Optional[typing.Dict[str, str]] = None):
        super().__init__()
        self.title = "Wind Speed"

        if measurements is None:
            measurements = OrderedDict([
                ('{code}ambient', '{mode}'),
            ])

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        mps = TimeSeries.Axis()
        mps.title = "m/s"
        mps.range = 0
        mps.format_code = '.1f'
        raw.axes.append(mps)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(mps)
            trace.legend = legend.format(type='Speed', mode='Raw')
            trace.data_record = f'{profile}-raw-wind'
            trace.data_field = field.format(code='WS')
            raw.traces.append(trace)

        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        mps = TimeSeries.Axis()
        mps.title = "m/s"
        mps.range = 0
        mps.format_code = '.1f'
        edited.axes.append(mps)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(mps)
            trace.legend = legend.format(type='Speed', mode='Edited')
            trace.data_record = f'{profile}-editing-wind'
            trace.data_field = field.format(code='WS')
            edited.traces.append(trace)


class EditingWindDirection(TimeSeries):
    def __init__(self, profile: str = 'met', measurements: typing.Optional[typing.Dict[str, str]] = None):
        super().__init__()
        self.title = "Wind Direction"

        if measurements is None:
            measurements = OrderedDict([
                ('{code}ambient', '{mode}'),
            ])

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        degrees = TimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        raw.axes.append(degrees)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(degrees)
            trace.legend = legend.format(type='Direction', mode='Raw')
            trace.data_record = f'{profile}-raw-wind'
            trace.data_field = field.format(code='WD')
            trace.script_incoming_data = r"""(function() {
const plotIncomingData = incomingData;
const wrapper = new Winds.DirectionWrapper();
incomingData = (plotTime, values, epoch) => {
    const r = wrapper.apply(values, plotTime, epoch);
    plotIncomingData(r.times, r.direction, r.epoch);
};
})();"""
            raw.traces.append(trace)

        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        degrees = TimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        edited.axes.append(degrees)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(degrees)
            trace.legend = legend.format(type='Direction', mode='Edited')
            trace.data_record = f'{profile}-editing-wind'
            trace.data_field = field.format(code='WD')
            trace.script_incoming_data = r"""(function() {
const plotIncomingData = incomingData;
const wrapper = new Winds.DirectionWrapper();
incomingData = (plotTime, values, epoch) => {
    const r = wrapper.apply(values, plotTime, epoch);
    plotIncomingData(r.times, r.direction, r.epoch);
};
})();"""
            edited.traces.append(trace)

    def required_components(self) -> typing.List[str]:
        return super().required_components() + ['winds']
