import typing
from forge.vis.view.timeseries import TimeSeries


class EditingOzoneConcentration(TimeSeries):
    def __init__(self, profile: str = 'ozone'):
        super().__init__()
        self.title = "Ozone Concentration"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        raw.axes.append(ppb)

        ozone = TimeSeries.Trace(ppb)
        ozone.legend = "Ozone"
        ozone.data_record = f'{profile}-raw-ozone'
        ozone.data_field = 'ozone'
        raw.traces.append(ozone)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        edited.axes.append(ppb)

        ozone = TimeSeries.Trace(ppb)
        ozone.legend = "Ozone"
        ozone.data_record = f'{profile}-editing-ozone'
        ozone.data_field = 'ozone'
        edited.traces.append(ozone)
