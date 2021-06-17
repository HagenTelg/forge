import typing
from forge.vis.view.timeseries import TimeSeries
from ..tower import TowerTemperatureDifference


class EditingTowerTemperatureDifference(TimeSeries):
    def __init__(self, profile: str = 'met'):
        super().__init__()
        self.title = "Tower Middle Minus Top"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        raw.axes.append(T_C)

        middle_minus_top = TimeSeries.Trace(T_C)
        middle_minus_top.legend = "Raw"
        middle_minus_top.data_record = f'{profile}-raw-tower'
        middle_minus_top.data_field = 'dT'
        raw.traces.append(middle_minus_top)
        self.processing[f'{profile}-raw-tower'] = TowerTemperatureDifference.CalculateTowerDifference()


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        edited.axes.append(T_C)

        middle_minus_top = TimeSeries.Trace(T_C)
        middle_minus_top.legend = "Edited"
        middle_minus_top.data_record = f'{profile}-editing-tower'
        middle_minus_top.data_field = 'dT'
        edited.traces.append(middle_minus_top)
        self.processing[f'{profile}-editing-tower'] = TowerTemperatureDifference.CalculateTowerDifference()
