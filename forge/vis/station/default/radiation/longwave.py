import typing
from forge.vis.view.solar import SolarTimeSeries
from .ratio import Ratios


class Longwave(SolarTimeSeries):
    INSTRUMENTS = (
        ("Downwelling", "Rdi"),
        ("Upwelling", "Rui"),
    )

    LongwaveTemperature = Ratios.CalculatePIRTemperature

    def __init__(self, mode: str, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Longwave"

        longwave = SolarTimeSeries.Graph()
        longwave.title = "Radiation"
        longwave.contamination = f'{mode}-contamination'
        self.graphs.append(longwave)

        wm2 = SolarTimeSeries.Axis()
        wm2.title = "W/m²"
        wm2.format_code = '.1f'
        longwave.axes.append(wm2)

        for title, field in self.INSTRUMENTS:
            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = title
            trace.data_record = f'{mode}-longwave'
            trace.data_field = field
            longwave.traces.append(trace)


        pir_temperature = SolarTimeSeries.Graph()
        pir_temperature.title = "Downwelling PIR/Air Temperature"
        pir_temperature.contamination = f'{mode}-contamination'
        self.graphs.append(pir_temperature)

        ratio = SolarTimeSeries.Axis()
        ratio.title = "W/m² / K"
        ratio.format_code = '.3f'
        pir_temperature.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "Longwave/Temperature"
        trace.data_record = f'{mode}-pirdownratio'
        trace.data_field = 'ratio'
        pir_temperature.traces.append(trace)
        self.processing[trace.data_record] = self.LongwaveTemperature()


class LongwaveSimplified(Longwave):
    def __init__(self, mode: str, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 **kwargs):
        super().__init__(mode, latitude, longitude, **kwargs)
        #self.graphs.pop()


class PyrgeometerTemperature(SolarTimeSeries):
    TRACE_CONTENTS = (
        ("Downwelling PIR Case", "Tdic"),
        ("Downwelling PIR Dome", "Tdid"),
        ("Upwelling PIR Case", "Tuic"),
        ("Upwelling PIR Dome", "Tuid"),
        ("Ambient", "Tambient"),
    )

    def __init__(self, mode: str, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Pyrgeometer Temperature"

        temperatures = SolarTimeSeries.Graph()
        temperatures.contamination = f'{mode}-contamination'
        self.graphs.append(temperatures)

        T_C = SolarTimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperatures.axes.append(T_C)

        for title, field in self.TRACE_CONTENTS:
            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = title
            trace.data_record = f'{mode}-pyranometertemperature'
            trace.data_field = field
            temperatures.traces.append(trace)
