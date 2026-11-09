import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries
from ..met.temperature import Temperature


class Ambient(TimeSeries):
    def __init__(self, mode: str,
                 trh: typing.Optional[typing.Dict[str, str]] = None,
                 winds: typing.Optional[typing.Dict[str, str]] = None,
                 omit_traces: typing.Optional[typing.Set[str]] = None, **kwargs):
        super().__init__(**kwargs)
        self.title = "Ambient Conditions"

        self.processing[f'{mode}-temperature'] = Temperature.CalculateMissing()

        if trh is None:
            trh = OrderedDict([
                ('{code}ambient', '{type}'),
            ])
        if winds is None:
            winds = OrderedDict([
                ('{code}ambient', '{type}'),
            ])

        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)
        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperature.axes.append(T_C)

        rh = TimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)
        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        for field, legend in trh.items():
            trace = TimeSeries.Trace(T_C)
            trace.legend = legend.format(type='Temperature', code='T')
            trace.data_record = f'{mode}-temperature'
            trace.data_field = field.format(code='T')
            if not omit_traces or trace.data_field not in omit_traces:
                temperature.traces.append(trace)

            trace = TimeSeries.Trace(rh_percent)
            trace.legend = legend.format(type='RH', code='U')
            trace.data_record = f'{mode}-temperature'
            trace.data_field = field.format(code='U')
            if not omit_traces or trace.data_field not in omit_traces:
                rh.traces.append(trace)


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)
        hpa = TimeSeries.Axis()
        hpa.title = "hPa"
        hpa.format_code = '.1f'
        pressure.axes.append(hpa)

        trace = TimeSeries.Trace(hpa)
        trace.legend = "Pressure"
        trace.data_record = f'{mode}-pressure'
        trace.data_field = 'Pambient'
        pressure.traces.append(trace)


        speed = TimeSeries.Graph()
        speed.title = "Wind Speed"
        self.graphs.append(speed)
        mps = TimeSeries.Axis()
        mps.title = "m/s"
        mps.range = 0
        mps.format_code = '.1f'
        speed.axes.append(mps)

        direction = TimeSeries.Graph()
        direction.title = "Wind Direction"
        self.graphs.append(direction)
        degrees = TimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        direction.axes.append(degrees)

        for field, legend in winds.items():
            ws = TimeSeries.Trace(mps)
            ws.legend = legend.format(type='Speed', code='WS')
            ws.data_record = f'{mode}-wind'
            ws.data_field = field.format(code='WS')
            speed.traces.append(ws)

            wd = TimeSeries.Trace(degrees)
            wd.legend = legend.format(type='Direction')
            wd.data_record = f'{mode}-wind'
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

    @property
    def required_components(self) -> typing.List[str]:
        return super().required_components + ['winds']
