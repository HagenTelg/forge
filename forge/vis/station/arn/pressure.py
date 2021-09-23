import typing
from forge.vis.view.timeseries import TimeSeries


class Pressure(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()

        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm2.5'), ("PM1", 'pm1')]:
            nephelometer = TimeSeries.Trace(hPa)
            nephelometer.legend = f"Nephelometer ({size[0]})"
            nephelometer.data_record = f'{mode}-samplepressure-{size[1]}'
            nephelometer.data_field = 'neph'
            pressure.traces.append(nephelometer)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm2.5'), ("PM1", 'pm1')]:
            sample = TimeSeries.Trace(hPa)
            sample.legend = f"Sample ({size[0]})"
            sample.data_record = f'{mode}-samplepressure-{size[1]}'
            sample.data_field = 'sample'
            pressure.traces.append(sample)

        dilution = TimeSeries.Trace(hPa)
        dilution.legend = "Dilution"
        dilution.data_record = f'{mode}-pressure'
        dilution.data_field = 'dilution'
        pressure.traces.append(dilution)
