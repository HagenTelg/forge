from .timeseries import TimeSeries
from .solar import SolarTimeSeries, SolarPosition


class _ExampleTimeSeries(TimeSeries):
    def __init__(self):
        super().__init__()
        self.title = "Example Time Series"

        light_extinction = TimeSeries.Graph()
        light_extinction.title = "Light Extinction"
        self.graphs.append(light_extinction)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        light_extinction.axes.append(Mm_1)

        BsG = TimeSeries.Trace(Mm_1)
        BsG.legend = "BsG"
        BsG.data_record = 'example-timeseries'
        BsG.data_field = 'BsG'
        BsG.color = '#0f0'
        light_extinction.traces.append(BsG)

        BaG = TimeSeries.Trace(Mm_1)
        BaG.legend = "BaG"
        BaG.data_record = 'example-timeseries'
        BaG.data_field = 'BaG'
        BaG.color = '#070'
        light_extinction.traces.append(BaG)


        sample_conditions = TimeSeries.Graph()
        sample_conditions.title = "Sample Conditions"
        self.graphs.append(sample_conditions)

        C = TimeSeries.Axis()
        C.title = "°C"
        sample_conditions.axes.append(C)
        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        sample_conditions.axes.append(hPa)

        T = TimeSeries.Trace(C)
        T.legend = "T"
        T.data_record = 'example-timeseries'
        T.data_field = 'Tsample'
        sample_conditions.traces.append(T)

        P = TimeSeries.Trace(hPa)
        P.legend = "P"
        P.data_record = 'example-timeseries'
        P.data_field = 'Psample'
        sample_conditions.traces.append(P)


        ambient_conditions = TimeSeries.Graph()
        ambient_conditions.title = "Ambient Conditions"
        self.graphs.append(ambient_conditions)

        C = TimeSeries.Axis()
        C.title = "°C"
        ambient_conditions.axes.append(C)

        Tambient = TimeSeries.Trace(C)
        Tambient.legend = "Ambient"
        Tambient.data_record = 'example-timeseries'
        Tambient.data_field = 'Tambient'
        ambient_conditions.traces.append(Tambient)

        Tsample = TimeSeries.Trace(C)
        Tsample.legend = "Internal"
        Tsample.data_record = 'example-timeseries'
        Tsample.data_field = 'Tsample'
        ambient_conditions.traces.append(Tsample)


example_timeseries = _ExampleTimeSeries()


class _ExampleSolarTimeSeries(SolarTimeSeries):
    def __init__(self):
        super().__init__(40, -105)
        self.title = "Example Solar Time Series"

        sample_conditions = TimeSeries.Graph()
        sample_conditions.title = "Sample Conditions"
        self.graphs.append(sample_conditions)

        C = TimeSeries.Axis()
        C.title = "°C"
        sample_conditions.axes.append(C)
        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        sample_conditions.axes.append(hPa)

        T = TimeSeries.Trace(C)
        T.legend = "T"
        T.data_record = 'example-timeseries'
        T.data_field = 'Tsample'
        sample_conditions.traces.append(T)

        P = TimeSeries.Trace(hPa)
        P.legend = "P"
        P.data_record = 'example-timeseries'
        P.data_field = 'Psample'
        sample_conditions.traces.append(P)


example_solartimeseries = _ExampleSolarTimeSeries()
example_solarposition = SolarPosition(40, -105)
