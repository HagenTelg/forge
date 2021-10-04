import typing
from forge.vis.view.timeseries import TimeSeries


class Pressure(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()

        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hpa_ambient = TimeSeries.Axis()
        hpa_ambient.title = "hPa"
        hpa_ambient.format_code = '.1f'
        pressure.axes.append(hpa_ambient)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            nephelometer = TimeSeries.Trace(hpa_ambient)
            nephelometer.legend = f"Dry Nephelometer ({size[0]})"
            nephelometer.data_record = f'{mode}-samplepressure-{size[1]}'
            nephelometer.data_field = 'neph'
            pressure.traces.append(nephelometer)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            nephelometer = TimeSeries.Trace(hpa_ambient)
            nephelometer.legend = f"Wet Nephelometer ({size[0]})"
            nephelometer.data_record = f'{mode}-samplepressure-{size[1]}'
            nephelometer.data_field = 'neph2'
            pressure.traces.append(nephelometer)


