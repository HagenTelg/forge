import typing
from forge.vis.view.sizedistribution import SizeDistribution, SizeCounts


class SMPSDistribution(SizeDistribution):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "SMPS Size Distribution"

        self.contamination = f'{mode}-contamination'
        self.size_record = f'{mode}-smps'
        self.measured_record = f'{mode}-scattering-pm1'


class SMPSCounts(SizeCounts):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Particle Concentration"

        self.contamination = f'{mode}-contamination'
        self.size_record = f'{mode}-smps'
        self.processing[self.size_record] = self.IntegrateSizeDistribution('N')

        n_cnc = SizeCounts.Trace()
        n_cnc.legend = "CNC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc'
        self.traces.append(n_cnc)

        n_smps = SizeCounts.Trace()
        n_smps.legend = "SMPS"
        n_smps.data_record = f'{mode}-smps'
        n_smps.data_field = 'N'
        self.traces.append(n_smps)

        n_grimm = SizeCounts.Trace()
        n_grimm.legend = "Grimm"
        n_grimm.data_record = f'{mode}-grimm'
        n_grimm.data_field = 'N'
        self.traces.append(n_grimm)
        self.processing[n_grimm.data_record] = self.IntegrateSizeDistribution('N')
