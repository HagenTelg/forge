import typing
from forge.vis.view.timeseries import TimeSeries


class Intensive(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Intensive Parameters at 550nm"

        albedo = TimeSeries.Graph()
        albedo.title = "Single Scattering Albedo"
        self.graphs.append(albedo)

        no_unit = TimeSeries.Axis()
        albedo.axes.append(no_unit)

        G0 = TimeSeries.Trace(no_unit)
        G0.legend = "SSA (PM10)"
        G0.data_record = f'{mode}-intensive-pm10'
        G0.data_field = 'SSAG'
        G0.color = '#0f0'
        albedo.traces.append(G0)

        G1 = TimeSeries.Trace(no_unit)
        G1.legend = "SSA (PM1)"
        G1.data_record = f'{mode}-intensive-pm1'
        G1.data_field = 'SSAG'
        G1.color = '#070'
        albedo.traces.append(G1)


        bfr = TimeSeries.Graph()
        bfr.title = "Backscatter Fraction"
        self.graphs.append(bfr)

        no_unit = TimeSeries.Axis()
        bfr.axes.append(no_unit)

        G0 = TimeSeries.Trace(no_unit)
        G0.legend = "BbsG/BsG (PM10)"
        G0.data_record = f'{mode}-intensive-pm10'
        G0.data_field = 'BfrG'
        G0.color = '#0f0'
        bfr.traces.append(G0)

        G1 = TimeSeries.Trace(no_unit)
        G1.legend = "BbsG/BsG (PM1)"
        G1.data_record = f'{mode}-intensive-pm1'
        G1.data_field = 'BfrG'
        G1.color = '#070'
        bfr.traces.append(G1)


        angstrom = TimeSeries.Graph()
        angstrom.title = "Ångström Exponent at 450 nm to 700nm"
        self.graphs.append(angstrom)

        no_unit = TimeSeries.Axis()
        angstrom.axes.append(no_unit)

        G0 = TimeSeries.Trace(no_unit)
        G0.legend = "Å (PM10)"
        G0.data_record = f'{mode}-intensive-pm10'
        G0.data_field = 'AngBR'
        G0.color = '#0f0'
        angstrom.traces.append(G0)

        G1 = TimeSeries.Trace(no_unit)
        G1.legend = "Å (PM1)"
        G1.data_record = f'{mode}-intensive-pm1'
        G1.data_field = 'AngBR'
        G1.color = '#070'
        angstrom.traces.append(G1)
