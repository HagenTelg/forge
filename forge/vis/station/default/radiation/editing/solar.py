import typing
from forge.vis.view.solar import SolarTimeSeries


class EditingSolar(SolarTimeSeries):
    TRACE_CONTENTS = (
        ("Downwelling Solar", "Rdg"),
        ("Downwelling Solar Auxiliary", "Rdg2"),
        ("Upwelling Solar", "Rug"),
        ("Upwelling Solar Auxiliary", "Rug2"),
        ("Direct Normal", "Rdn"),
        ("Direct Normal Auxiliary", "Rdn2"),
        ("Diffuse", "Rdf"),
        ("Diffuse Auxiliary", "Rdf2"),
        ("SPN1 Total", "Rst"),
        ("SPN1 Diffuse", "Rsd"),
        ("Ultraviolet", "Rv"),
        ("Photosynthetically Active", "Rp"),
    )

    def __init__(self, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 profile: str = 'radiation', **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Solar"

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
            trace.data_record = f'{profile}-raw-solar'
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
            trace.data_record = f'{profile}-editing-solar'
            trace.data_field = field
            edited.traces.append(trace)
