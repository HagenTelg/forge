import typing
from forge.vis.view.timeseries import TimeSeries


class Pressure(TimeSeries):
    class CalculateNephPressureDrop(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
    return new GenericOperations.SingleOutput(dataName, GenericOperations.difference, 'dPneph', 'ambient', 'neph');
})"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)

        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hpa_ambient = TimeSeries.Axis()
        hpa_ambient.title = "hPa"
        hpa_ambient.format_code = '.1f'
        pressure.axes.append(hpa_ambient)

        ambient = TimeSeries.Trace(hpa_ambient)
        ambient.legend = "Ambient"
        ambient.data_record = f'{mode}-pressure'
        ambient.data_field = 'ambient'
        pressure.traces.append(ambient)

        nephelometer_fine = TimeSeries.Trace(hpa_ambient)
        nephelometer_fine.legend = "Nephelometer"
        nephelometer_fine.data_record = f'{mode}-pressure'
        nephelometer_fine.data_field = 'neph'
        pressure.traces.append(nephelometer_fine)


        pressure_drop = TimeSeries.Graph()
        pressure_drop.title = "Ambient Pressure Minus Nephelometer Pressure"
        self.graphs.append(pressure_drop)

        hpa_drop = TimeSeries.Axis()
        hpa_drop.title = "hPa"
        hpa_drop.format_code = '.2f'
        pressure_drop.axes.append(hpa_drop)

        neph_drop = TimeSeries.Trace(hpa_drop)
        neph_drop.legend = "Neph Pressure Drop"
        neph_drop.data_record = f'{mode}-pressure'
        neph_drop.data_field = 'dPneph'
        pressure_drop.traces.append(neph_drop)
        self.processing[neph_drop.data_record] = self.CalculateNephPressureDrop()
