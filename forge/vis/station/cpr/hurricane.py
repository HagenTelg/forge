import typing
from forge.vis.view.timeseries import TimeSeries


class Hurricane(TimeSeries):
    class CalculatePurpleAirScattering(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('purpleair')
            self.script = r"""(function(dataName) {
    return new PurpleAir.CalculateDispatch(dataName, 'IBsa', 'IBsb', 'Bs');
})"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Hurricane Hardened Monitoring"

        scattering = TimeSeries.Graph()
        scattering.title = "Light Scattering"
        self.graphs.append(scattering)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        scattering.axes.append(Mm_1)

        purpleair = TimeSeries.Trace(Mm_1)
        purpleair.legend = "PurpleAir Scattering"
        purpleair.data_record = f'{mode}-hurricane'
        purpleair.data_field = 'Bs'
        scattering.traces.append(purpleair)
        self.processing[purpleair.data_record] = self.CalculatePurpleAirScattering()


        conditions = TimeSeries.Graph()
        conditions.title = "Conditions"
        self.graphs.append(conditions)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        conditions.axes.append(T_C)

        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        conditions.axes.append(rh_percent)

        temperature = TimeSeries.Trace(T_C)
        temperature.legend = "Temperature"
        temperature.data_record = f'{mode}-hurricane'
        temperature.data_field = 'T'
        conditions.traces.append(temperature)

        rh = TimeSeries.Trace(rh_percent)
        rh.legend = "RH"
        rh.data_record = f'{mode}-hurricane'
        rh.data_field = 'U'
        conditions.traces.append(rh)


        ambient_pressure = TimeSeries.Graph()
        ambient_pressure.title = "Pressure"
        self.graphs.append(ambient_pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        ambient_pressure.axes.append(hPa)

        pressure = TimeSeries.Trace(hPa)
        pressure.legend = "Pressure"
        pressure.data_record = f'{mode}-hurricane'
        pressure.data_field = 'P'
        ambient_pressure.traces.append(pressure)


        winds = TimeSeries.Graph()
        winds.title = "Winds"
        self.graphs.append(winds)

        mps = TimeSeries.Axis()
        mps.title = "m/s"
        mps.format_code = '.1f'
        mps.range = 0
        winds.axes.append(mps)

        degrees = TimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        winds.axes.append(degrees)

        ws = TimeSeries.Trace(mps)
        ws.legend = "Wind Speed"
        ws.data_record = f'{mode}-hurricane'
        ws.data_field = 'WS'
        winds.traces.append(ws)

        wd = TimeSeries.Trace(degrees)
        wd.legend = "Wind Direction"
        wd.data_record = f'{mode}-hurricane'
        wd.data_field = 'WD'
        wd.script_incoming_data = r"""(function() {
const plotIncomingData = incomingData;
const wrapper = new Winds.DirectionWrapper();
incomingData = (plotTime, values, epoch) => {
    const r = wrapper.apply(values, plotTime, epoch);
    plotIncomingData(r.times, r.direction, r.epoch);
};
})();"""
        winds.traces.append(wd)

    @property
    def required_components(self) -> typing.List[str]:
        return super().required_components + ['winds']

