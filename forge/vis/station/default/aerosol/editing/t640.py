import typing
from forge.vis.view.timeseries import TimeSeries


class EditingT640(TimeSeries):
    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "T640 Mass Concentration"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        ugm3.range = 0
        raw.axes.append(ugm3)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'),
                     ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            trace = TimeSeries.Trace(ugm3)
            trace.legend = f"Raw {size[0]}"
            trace.data_record = f'{profile}-raw-t640-{size[1]}'
            trace.data_field = 'X'
            raw.traces.append(trace)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        ugm3.range = 0
        edited.axes.append(ugm3)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'),
                     ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            trace = TimeSeries.Trace(ugm3)
            trace.legend = f"Edited {size[0]}"
            trace.data_record = f'{profile}-editing-t640-{size[1]}'
            trace.data_field = 'X'
            edited.traces.append(trace)
