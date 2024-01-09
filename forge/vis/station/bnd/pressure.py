import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.pressure import Pressure as BasePressure


class Pressure(BasePressure):
    class CalculateNephPressureDrop(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
return new GenericOperations.ApplyToFields(dataName, {
    'dPneph-whole': (neph, ambient) => { return GenericOperations.difference(ambient, neph); },
    'dPneph-pm10': (neph, ambient) => { return GenericOperations.difference(ambient, neph); },
    'dPneph-pm25': (neph, ambient) => { return GenericOperations.difference(ambient, neph); },
    'dPneph-pm1': (neph, ambient) => { return GenericOperations.difference(ambient, neph); },
}, 'ambient');
})"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(mode, **kwargs)

        pressure_drop = TimeSeries.Graph()
        pressure_drop.title = "Ambient Pressure Minus Nephelometer Pressure"
        self.graphs.append(pressure_drop)

        hpa_drop = TimeSeries.Axis()
        hpa_drop.title = "hPa"
        hpa_drop.format_code = '.2f'
        pressure_drop.axes.append(hpa_drop)

        # for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
        for size in [("PM10", 'pm10'), ("PM1", 'pm1')]:
            impactor = TimeSeries.Trace(hpa_drop)
            impactor.legend = f"Neph dP ({size[0]})"
            impactor.data_record = f'{mode}-pressure'
            impactor.data_field = f'dPneph-{size[1]}'
            pressure_drop.traces.append(impactor)

        self.processing[f'{mode}-pressure'] = self.CalculateNephPressureDrop()
