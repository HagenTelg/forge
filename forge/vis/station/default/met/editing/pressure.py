import typing
from forge.vis.view.timeseries import TimeSeries


class EditingPressure(TimeSeries):
    def __init__(self, profile: str = 'met', **kwargs):
        super().__init__(**kwargs)
        self.title = "Pressure"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        hpa = TimeSeries.Axis()
        hpa.title = "hPa"
        hpa.format_code = '.1f'
        raw.axes.append(hpa)

        ambient = TimeSeries.Trace(hpa)
        ambient.legend = "Raw"
        ambient.data_record = f'{profile}-raw-pressure'
        ambient.data_field = 'ambient'
        raw.traces.append(ambient)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        hpa = TimeSeries.Axis()
        hpa.title = "hPa"
        hpa.format_code = '.1f'
        edited.axes.append(hpa)

        ambient = TimeSeries.Trace(hpa)
        ambient.legend = "Edited"
        ambient.data_record = f'{profile}-editing-pressure'
        ambient.data_field = 'ambient'
        edited.traces.append(ambient)
