import typing
from ..default.aerosol.pressure import TimeSeries


class Pressure(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)

        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        ambient = TimeSeries.Trace(hPa)
        ambient.legend = "Ambient"
        ambient.data_record = f'{mode}-pressure'
        ambient.data_field = 'ambient'
        pressure.traces.append(ambient)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm2.5'), ("PM1", 'pm1')]:
            nephelometer = TimeSeries.Trace(hPa)
            nephelometer.legend = f"TSI Nephelometer ({size[0]})"
            nephelometer.data_record = f'{mode}-samplepressure-{size[1]}'
            nephelometer.data_field = 'neph'
            pressure.traces.append(nephelometer)

            nephelometer = TimeSeries.Trace(hPa)
            nephelometer.legend = f"NE-300 Nephelometer ({size[0]})"
            nephelometer.data_record = f'{mode}-samplepressure-{size[1]}'
            nephelometer.data_field = 'neph2'
            pressure.traces.append(nephelometer)
