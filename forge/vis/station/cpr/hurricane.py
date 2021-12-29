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
        conditions.title = "Ambient Conditions"
        self.graphs.append(conditions)

        ms = TimeSeries.Axis()
        ms.title = "m/s"
        ms.format_code = '.1f'
        ms.range = 0
        conditions.axes.append(ms)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        conditions.axes.append(hPa)

        wind_speed = TimeSeries.Trace(ms)
        wind_speed.legend = "Wind Speed"
        wind_speed.data_record = f'{mode}-hurricane'
        wind_speed.data_field = 'WS'
        conditions.traces.append(wind_speed)

        pressure = TimeSeries.Trace(hPa)
        pressure.legend = "Pressure"
        pressure.data_record = f'{mode}-hurricane'
        pressure.data_field = 'pressure'
        conditions.traces.append(pressure)


        precipitation = TimeSeries.Graph()
        precipitation.title = "Precipitation"
        self.graphs.append(precipitation)

        mm_h = TimeSeries.Axis()
        mm_h.title = "mm/h"
        mm_h.format_code = '.2f'
        mm_h.range = 0
        precipitation.axes.append(mm_h)

        precip = TimeSeries.Trace(mm_h)
        precip.legend = "Precipitation"
        precip.data_record = f'{mode}-hurricane'
        precip.data_field = 'precipitation'
        precipitation.traces.append(precip)

