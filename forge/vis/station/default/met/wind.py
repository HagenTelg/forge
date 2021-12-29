import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries


class Wind(TimeSeries):
    def __init__(self, record: str, measurements: typing.Optional[typing.Dict[str, str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.title = "Winds"

        if measurements is None:
            measurements = OrderedDict([
                ('{code}ambient', '{type}'),
            ])

        speed = TimeSeries.Graph()
        self.graphs.append(speed)
        mps = TimeSeries.Axis()
        mps.title = "m/s"
        mps.range = 0
        mps.format_code = '.1f'
        speed.axes.append(mps)

        direction = TimeSeries.Graph()
        self.graphs.append(direction)
        degrees = TimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        direction.axes.append(degrees)

        for field, legend in measurements.items():
            ws = TimeSeries.Trace(mps)
            ws.legend = legend.format(type='Speed', code='WS')
            ws.data_record = record
            ws.data_field = field.format(code='WS')
            speed.traces.append(ws)

            wd = TimeSeries.Trace(degrees)
            wd.legend = legend.format(type='Direction')
            wd.data_record = record
            wd.data_field = field.format(code='WD')
            wd.script_incoming_data = r"""(function() {
const plotIncomingData = incomingData;
const wrapper = new Winds.DirectionWrapper();
incomingData = (plotTime, values, epoch) => {
    const r = wrapper.apply(values, plotTime, epoch);
    plotIncomingData(r.times, r.direction, r.epoch);
};
})();"""
            direction.traces.append(wd)

    def required_components(self) -> typing.List[str]:
        return super().required_components() + ['winds']
