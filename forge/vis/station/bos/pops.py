import typing
from forge.vis.view.sizedistribution import SizeCounts


class POPSCounts(SizeCounts):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Particle Concentration"

        self.contamination = f'{mode}-contamination'
        self.size_record = f'{mode}-pops'
        self.processing[self.size_record] = self.IntegrateSizeDistribution('N')

        n_cnc = SizeCounts.Trace()
        n_cnc.legend = "CNC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc'
        self.traces.append(n_cnc)

        n_cnc = SizeCounts.Trace()
        n_cnc.legend = "CNC2"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc2'
        self.traces.append(n_cnc)

        n_pops = SizeCounts.Trace()
        n_pops.legend = "POPS"
        n_pops.data_record = f'{mode}-pops'
        n_pops.data_field = 'N'
        self.traces.append(n_pops)
