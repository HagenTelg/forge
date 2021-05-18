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
        pressure.axes.append(hpa_ambient)

        hpa_delta = TimeSeries.Axis()
        hpa_delta.title = "Delta Pressure (hPa)"
        hpa_delta.range = 0
        pressure.axes.append(hpa_delta)

        ambient = TimeSeries.Trace(hpa_ambient)
        ambient.legend = "Ambient"
        ambient.data_record = f'{mode}-pressure'
        ambient.data_field = 'ambient'
        pressure.traces.append(ambient)

        nephelometer_pm10 = TimeSeries.Trace(hpa_ambient)
        nephelometer_pm10.legend = "Nephelometer (PM10)"
        nephelometer_pm10.data_record = f'{mode}-pressure'
        nephelometer_pm10.data_field = 'neph-pm10'
        pressure.traces.append(nephelometer_pm10)

        nephelometer_pm1 = TimeSeries.Trace(hpa_ambient)
        nephelometer_pm1.legend = "Nephelometer (PM1)"
        nephelometer_pm1.data_record = f'{mode}-pressure'
        nephelometer_pm1.data_field = 'neph-pm1'
        pressure.traces.append(nephelometer_pm1)

        pitot = TimeSeries.Trace(hpa_delta)
        pitot.legend = "Pitot"
        pitot.data_record = f'{mode}-pressure'
        pitot.data_field = 'pitot'
        pressure.traces.append(pitot)

        impactor_pm10 = TimeSeries.Trace(hpa_delta)
        impactor_pm10.legend = "impactor (PM10)"
        impactor_pm10.data_record = f'{mode}-pressure'
        impactor_pm10.data_field = 'impactor-pm10'
        pressure.traces.append(impactor_pm10)

        impactor_pm1 = TimeSeries.Trace(hpa_delta)
        impactor_pm1.legend = "impactor (PM1)"
        impactor_pm1.data_record = f'{mode}-pressure'
        impactor_pm1.data_field = 'impactor-pm1'
        pressure.traces.append(impactor_pm1)


        system_vacuum = TimeSeries.Graph()
        system_vacuum.title = "System Vacuum"
        self.graphs.append(system_vacuum)

        hpa_vacuum = TimeSeries.Axis()
        hpa_vacuum.title = "hPa"
        system_vacuum.axes.append(hpa_vacuum)

        vacuum = TimeSeries.Trace(hpa_vacuum)
        vacuum.legend = "Vacuum"
        vacuum.data_record = f'{mode}-pressure'
        vacuum.data_field = 'vacuum'
        system_vacuum.traces.append(vacuum)
