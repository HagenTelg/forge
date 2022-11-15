import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.met.temperature import Temperature
from . import Site


class Ambient(TimeSeries):
    _TRACE_FORMAT = '{site.name} {parameter} ({code}_{site.instrument_code})'

    def __init__(self, record: str, sites: typing.List[Site], **kwargs):
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

        for site in sites:
            trace = TimeSeries.Trace(T_C)
            trace.legend = self._TRACE_FORMAT.format(site=site, parameter="Temperature", code='T')
            trace.data_record = record
            trace.data_field = f'T_{site.instrument_code}'
            temperature.traces.append(trace)


        rh = TimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)
        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        for site in sites:
            trace = TimeSeries.Trace(rh_percent)
            trace.legend = self._TRACE_FORMAT.format(site=site, parameter="RH", code='U')
            trace.data_record = record
            trace.data_field = f'U_{site.instrument_code}'
            rh.traces.append(trace)


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)
        hpa = TimeSeries.Axis()
        hpa.title = "hPa"
        hpa.format_code = '.1f'
        pressure.axes.append(hpa)

        for site in sites:
            trace = TimeSeries.Trace(hpa)
            trace.legend = self._TRACE_FORMAT.format(site=site, parameter="Pressure", code='P')
            trace.data_record = record
            trace.data_field = f'P_{site.instrument_code}'
            pressure.traces.append(trace)


        speed = TimeSeries.Graph()
        speed.title = "Wind Speed"
        self.graphs.append(speed)
        mps = TimeSeries.Axis()
        mps.title = "m/s"
        mps.range = 0
        mps.format_code = '.1f'
        speed.axes.append(mps)

        for site in sites:
            trace = TimeSeries.Trace(mps)
            trace.legend = self._TRACE_FORMAT.format(site=site, parameter="Speed", code='WS')
            trace.data_record = record
            trace.data_field = f'WS_{site.instrument_code}'
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

        for site in sites:
            trace = TimeSeries.Trace(degrees)
            trace.legend = self._TRACE_FORMAT.format(site=site, parameter="Direction", code='WD')
            trace.data_record = record
            trace.data_field = f'WD_{site.instrument_code}'
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
