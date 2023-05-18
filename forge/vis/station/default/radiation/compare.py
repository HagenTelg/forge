import typing
from forge.vis.view.solar import SolarTimeSeries


class ShortwaveCompare(SolarTimeSeries):
    INSTRUMENTS = (
        ("Downwelling Shortwave", "Rdg"),
        ("Downwelling Shortwave Auxiliary 2", "Rdg2"),
        ("Downwelling Shortwave Auxiliary 3", "Rdg3"),
        ("Upwelling Shortwave", "Rug"),
        ("Upwelling Shortwave Auxiliary 2", "Rug2"),
        ("Upwelling Shortwave Auxiliary 3", "Rug3"),
        ("Direct Normal", "Rdn"),
        ("Direct Normal Auxiliary", "Rdn2"),
        ("Diffuse", "Rdf"),
        ("Diffuse Auxiliary", "Rdf2"),
    )

    def __init__(self, mode: str, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Shortwave Test Comparison"

        shortwave = SolarTimeSeries.Graph()
        shortwave.title = "Radiation"
        shortwave.contamination = f'{mode}-contamination'
        self.graphs.append(shortwave)

        wm2 = SolarTimeSeries.Axis()
        wm2.title = "W/m²"
        wm2.format_code = '.1f'
        shortwave.axes.append(wm2)

        for title, field in self.INSTRUMENTS:
            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = f"{title}"
            trace.data_record = f'{mode}-shortwave'
            trace.data_field = field
            shortwave.traces.append(trace)
