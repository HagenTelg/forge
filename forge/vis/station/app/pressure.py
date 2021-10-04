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

        hpa_delta = TimeSeries.Axis()
        hpa_delta.title = "Delta Pressure (hPa)"
        hpa_delta.range = 0
        hpa_delta.format_code = '.3f'
        pressure.axes.append(hpa_delta)

        ambient = TimeSeries.Trace(hpa_ambient)
        ambient.legend = "Ambient"
        ambient.data_record = f'{mode}-pressure'
        ambient.data_field = 'ambient'
        pressure.traces.append(ambient)

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

        pitot = TimeSeries.Trace(hpa_delta)
        pitot.legend = "Pitot"
        pitot.data_record = f'{mode}-pressure'
        pitot.data_field = 'pitot'
        pressure.traces.append(pitot)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            impactor = TimeSeries.Trace(hpa_delta)
            impactor.legend = f"Impactor ({size[0]})"
            impactor.data_record = f'{mode}-samplepressure-{size[1]}'
            impactor.data_field = 'impactor'
            pressure.traces.append(impactor)


        system_vacuum = TimeSeries.Graph()
        system_vacuum.title = "System Vacuum"
        self.graphs.append(system_vacuum)

        hpa_vacuum = TimeSeries.Axis()
        hpa_vacuum.title = "hPa"
        hpa_vacuum.format_code = '.2f'
        system_vacuum.axes.append(hpa_vacuum)

        vacuum = TimeSeries.Trace(hpa_vacuum)
        vacuum.legend = "Vacuum"
        vacuum.data_record = f'{mode}-pressure'
        vacuum.data_field = 'vacuum'
        system_vacuum.traces.append(vacuum)

