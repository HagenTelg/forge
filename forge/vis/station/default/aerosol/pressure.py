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

        nephelometer_coarse = TimeSeries.Trace(hpa_ambient)
        nephelometer_coarse.legend = "Nephelometer (Coarse)"
        nephelometer_coarse.data_record = f'{mode}-pressure'
        nephelometer_coarse.data_field = 'neph-coarse'
        pressure.traces.append(nephelometer_coarse)

        nephelometer_fine = TimeSeries.Trace(hpa_ambient)
        nephelometer_fine.legend = "Nephelometer (Fine)"
        nephelometer_fine.data_record = f'{mode}-pressure'
        nephelometer_fine.data_field = 'neph-fine'
        pressure.traces.append(nephelometer_fine)

        pitot = TimeSeries.Trace(hpa_delta)
        pitot.legend = "Pitot"
        pitot.data_record = f'{mode}-pressure'
        pitot.data_field = 'pitot'
        pressure.traces.append(pitot)

        impactor_coarse = TimeSeries.Trace(hpa_delta)
        impactor_coarse.legend = "impactor (Coarse)"
        impactor_coarse.data_record = f'{mode}-pressure'
        impactor_coarse.data_field = 'impactor-coarse'
        pressure.traces.append(impactor_coarse)

        impactor_fine = TimeSeries.Trace(hpa_delta)
        impactor_fine.legend = "impactor (Fine)"
        impactor_fine.data_record = f'{mode}-pressure'
        impactor_fine.data_field = 'impactor-fine'
        pressure.traces.append(impactor_fine)


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
