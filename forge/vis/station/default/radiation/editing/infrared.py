import typing
from forge.vis.view.solar import SolarTimeSeries


class EditingInfrared(SolarTimeSeries):
    TRACE_CONTENTS = (
        ("Downwelling", "Rdi"),
        ("Upwelling", "Rui"),
    )

    def __init__(self, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 profile: str = 'radiation', **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Infrared"

        raw = SolarTimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        wm2 = SolarTimeSeries.Axis()
        wm2.title = "W/m²"
        wm2.format_code = '.1f'
        raw.axes.append(wm2)

        for title, field in self.TRACE_CONTENTS:
            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = f"Raw {title}"
            trace.data_record = f'{profile}-raw-ir'
            trace.data_field = field
            raw.traces.append(trace)

        edited = SolarTimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        wm2 = SolarTimeSeries.Axis()
        wm2.title = "W/m²"
        wm2.format_code = '.1f'
        edited.axes.append(wm2)

        for title, field in self.TRACE_CONTENTS:
            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = f"Edited {title}"
            trace.data_record = f'{profile}-editing-ir'
            trace.data_field = field
            edited.traces.append(trace)


class EditingPyrgeometerTemperature(SolarTimeSeries):
    TRACE_CONTENTS = (
        ("Downwelling PIR Case", "Tdic"),
        ("Downwelling PIR Dome", "Tdid"),
        ("Upwelling PIR Case", "Tuic"),
        ("Upwelling PIR Dome", "Tuid"),
    )

    def __init__(self, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 profile: str = 'radiation', **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Pyrgeometer Temperature"

        raw = SolarTimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        T_C = SolarTimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        raw.axes.append(T_C)

        for title, field in self.TRACE_CONTENTS:
            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = f"Raw {title}"
            trace.data_record = f'{profile}-raw-pyranometertemperature'
            trace.data_field = field
            raw.traces.append(trace)

        trace = SolarTimeSeries.Trace(T_C)
        trace.legend = f"Air Temperature"
        trace.data_record = f'{profile}-raw-ambient'
        trace.data_field = "Tambient"
        raw.traces.append(trace)

        edited = SolarTimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        T_C = SolarTimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        edited.axes.append(T_C)

        for title, field in self.TRACE_CONTENTS:
            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = f"Edited {title}"
            trace.data_record = f'{profile}-editing-pyranometertemperature'
            trace.data_field = field
            edited.traces.append(trace)
