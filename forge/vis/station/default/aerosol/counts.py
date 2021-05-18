import typing
from forge.vis.view.timeseries import TimeSeries


class ParticleConcentration(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Particle Concentration"

        cnc = TimeSeries.Graph()
        self.graphs.append(cnc)

        cm_3 = TimeSeries.Axis()
        cm_3.title = "cm⁻³"
        cm_3.range = 0
        cnc.axes.append(cm_3)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "CNC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc'
        cnc.traces.append(n_cnc)
