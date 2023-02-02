import typing
from forge.vis.view.timeseries import TimeSeries
from ..met.temperature import Temperature


class Ambient(TimeSeries):
    def __init__(self, record: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Ambient Conditions"

        self.processing[record] = Temperature.CalculateMissing()

        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)
        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperature.axes.append(T_C)

        trace = TimeSeries.Trace(T_C)
        trace.legend = "Temperature"
        trace.data_record = record
        trace.data_field = 'Tambient'
        temperature.traces.append(trace)


        rh = TimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)
        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        trace = TimeSeries.Trace(rh_percent)
        trace.legend = "Relative Humidity"
        trace.data_record = record
        trace.data_field = 'Uambient'
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
        trace.data_record = record
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

        trace = TimeSeries.Trace(mps)
        trace.legend = "Wind Speed"
        trace.data_record = record
        trace.data_field = 'WS'
        speed.traces.append(trace)


        direction = TimeSeries.Graph()
        direction.title = "Wind Direction"
        self.graphs.append(direction)
        degrees = TimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        direction.axes.append(degrees)

        trace = TimeSeries.Trace(degrees)
        trace.legend = "Wind Direction"
        trace.data_record = record
        trace.data_field = 'WD'
        trace.script_incoming_data = r"""(function() {
const plotIncomingData = incomingData;
const wrapper = new Winds.DirectionWrapper();
incomingData = (plotTime, values, epoch) => {
    const r = wrapper.apply(values, plotTime, epoch);
    plotIncomingData(r.times, r.direction, r.epoch);
};
})();"""
        direction.traces.append(trace)

    @property
    def required_components(self) -> typing.List[str]:
        return super().required_components + ['winds']
