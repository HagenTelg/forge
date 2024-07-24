import typing
from forge.vis.view.timeseries import TimeSeries


class EditingNOxConcentration(TimeSeries):
    def __init__(self, profile: str = 'ozone', **kwargs):
        super().__init__(**kwargs)
        self.title = "NOₓ Concentration"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        raw.axes.append(ppb)

        no = TimeSeries.Trace(ppb)
        no.legend = "NO"
        no.data_record = f'{profile}-raw-nox'
        no.data_field = 'no'
        raw.traces.append(no)

        no2 = TimeSeries.Trace(ppb)
        no2.legend = "NO₂"
        no2.data_record = f'{profile}-raw-nox'
        no2.data_field = 'no2'
        raw.traces.append(no2)

        nox = TimeSeries.Trace(ppb)
        nox.legend = "NOₓ"
        nox.data_record = f'{profile}-raw-nox'
        nox.data_field = 'nox'
        raw.traces.append(nox)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        edited.axes.append(ppb)

        no = TimeSeries.Trace(ppb)
        no.legend = "NO"
        no.data_record = f'{profile}-editing-nox'
        no.data_field = 'no'
        edited.traces.append(no)

        no2 = TimeSeries.Trace(ppb)
        no2.legend = "NO₂"
        no2.data_record = f'{profile}-editing-nox'
        no2.data_field = 'no2'
        edited.traces.append(no2)

        nox = TimeSeries.Trace(ppb)
        nox.legend = "NOₓ"
        nox.data_record = f'{profile}-editing-nox'
        nox.data_field = 'nox'
        edited.traces.append(nox)
