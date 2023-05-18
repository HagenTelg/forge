import typing
from forge.vis.view.solar import SolarTimeSeries
from .ratio import Ratios


class Shortwave(SolarTimeSeries):
    INSTRUMENTS = (
        ("Downwelling Shortwave", "Rdg"),
        ("Downwelling Shortwave Auxiliary 2", "Rdg2"),
        ("Downwelling Shortwave Auxiliary 3", "Rdg3"),
        ("Upwelling Shortwave", "Rug"),
        ("Upwelling Shortwave Auxiliary 2", "Rug2"),
        ("Upwelling Shortwave Auxiliary 3", "Rug3"),
        ("Direct Normal", "Rdn"),
        ("Direct Normal Auxiliary 2", "Rdn2"),
        ("Direct Normal Auxiliary 3", "Rdn3"),
        ("Diffuse", "Rdf"),
        ("Diffuse Auxiliary 2", "Rdf2"),
        ("Diffuse Auxiliary 3", "Rdf3"),
        ("SPN1 Total", "Rst"),
        ("SPN1 Diffuse", "Rsd"),
        ("Ultraviolet", "Rv"),
        ("Photosynthetically Active", "Rp"),
    )

    CalculateDirectDiffuseGlobal = Ratios.CalculateDirectDiffuseGlobal
    CalculateTotalGlobal = Ratios.CalculateTotalGlobal
    CalculateAlbedo = Ratios.CalculateAlbedo

    def __init__(self, mode: str, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Shortwave"

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


        total_global = SolarTimeSeries.Graph()
        total_global.title = "Total/Global"
        total_global.contamination = f'{mode}-contamination'
        self.graphs.append(total_global)

        ratio = SolarTimeSeries.Axis()
        ratio.format_code = '.3f'
        total_global.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "Direct+Diffuse / Global"
        trace.data_record = f'{mode}-totalratio'
        trace.data_field = 'ratio'
        total_global.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateDirectDiffuseGlobal()

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "SPN1 Total/Global"
        trace.data_record = f'{mode}-spn1ratio'
        trace.data_field = 'ratio'
        total_global.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateTotalGlobal()


        albedo = SolarTimeSeries.Graph()
        albedo.title = "Albedo"
        albedo.contamination = f'{mode}-contamination'
        self.graphs.append(albedo)

        ratio = SolarTimeSeries.Axis()
        ratio.format_code = '.3f'
        ratio.range = [0.0, 1.5]
        albedo.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "Upwelling/Downwelling"
        trace.data_record = f'{mode}-albedo'
        trace.data_field = 'albedo'
        albedo.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateAlbedo()


class ShortwaveSimplified(Shortwave):
    def __init__(self, mode: str, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 **kwargs):
        super().__init__(mode, latitude, longitude, **kwargs)
        self.graphs.pop()
        self.graphs.pop()
