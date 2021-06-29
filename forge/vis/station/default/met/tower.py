import typing
from forge.vis.view.timeseries import TimeSeries


class TowerTemperatureDifference(TimeSeries):
    class CalculateTowerDifference(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
    return new GenericOperations.SingleOutput(dataName, GenericOperations.difference, 'dT', 'Tmiddle', 'Ttop');
})"""

    def __init__(self, mode: str):
        super().__init__()

        temperature_difference = TimeSeries.Graph()
        temperature_difference.title = "Tower Middle Minus Top"
        self.graphs.append(temperature_difference)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperature_difference.axes.append(T_C)

        middle_minus_top = TimeSeries.Trace(T_C)
        middle_minus_top.data_record = f'{mode}-tower'
        middle_minus_top.data_field = 'dT'
        temperature_difference.traces.append(middle_minus_top)
        self.processing[f'{mode}-tower'] = self.CalculateTowerDifference()
