import typing
from forge.vis.view.timeseries import TimeSeries
from forge.vis.view.sizedistribution import SizeCounts


class RealtimeParticleConcentration(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(realtime=True, **kwargs)
        self.title = "Particle Concentration"

        cnc = TimeSeries.Graph()
        cnc.contamination = f'{mode}-contamination'
        self.graphs.append(cnc)

        cm_3 = TimeSeries.Axis()
        cm_3.title = "cm⁻³"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        cnc.axes.append(cm_3)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "CNC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc'
        cnc.traces.append(n_cnc)

        n_pops = TimeSeries.Trace(cm_3)
        n_pops.legend = "POPS"
        n_pops.data_record = f'{mode}-pops'
        n_pops.data_field = 'N'
        cnc.traces.append(n_pops)
        self.processing[n_pops.data_record] = SizeCounts.IntegrateSizeDistribution('N')
