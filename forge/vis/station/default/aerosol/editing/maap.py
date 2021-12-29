import typing
from forge.vis.view.timeseries import TimeSeries


class EditingMAA5012P(TimeSeries):
    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "MAAP"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        raw.axes.append(ugm3)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        raw.axes.append(Mm_1)

        maap = TimeSeries.Trace(ugm3)
        maap.legend = "Raw MAAP EBC"
        maap.data_record = f'{profile}-raw-maap'
        maap.data_field = 'X'
        raw.traces.append(maap)

        maap = TimeSeries.Trace(Mm_1)
        maap.legend = "Raw MAAP Absorption"
        maap.data_record = f'{profile}-raw-maap'
        maap.data_field = 'Ba'
        raw.traces.append(maap)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        edited.axes.append(ugm3)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        edited.axes.append(Mm_1)

        maap = TimeSeries.Trace(ugm3)
        maap.legend = "Edited MAAP EBC"
        maap.data_record = f'{profile}-editing-maap'
        maap.data_field = 'X'
        edited.traces.append(maap)

        maap = TimeSeries.Trace(Mm_1)
        maap.legend = "Edited MAAP Absorption"
        maap.data_record = f'{profile}-editing-maap'
        maap.data_field = 'Ba'
        edited.traces.append(maap)
