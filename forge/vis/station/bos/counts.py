import typing
from forge.vis.view.timeseries import TimeSeries, PublicTimeSeries
from forge.vis.view.sizedistribution import SizeCounts
from ..default.aerosol.admagiccpc import ADMagicCPC250Status
from ..default.aerosol.public.counts import PublicCountsShort as PublicCountsShortBase


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

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "CNC2"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc2'
        cnc.traces.append(n_cnc)

        n_pops = TimeSeries.Trace(cm_3)
        n_pops.legend = "POPS"
        n_pops.data_record = f'{mode}-pops'
        n_pops.data_field = 'N'
        cnc.traces.append(n_pops)
        self.processing[n_pops.data_record] = SizeCounts.IntegrateSizeDistribution('N')


class EditingParticleConcentration(TimeSeries):
    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)
        self.title = "Particle Concentration"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        cm_3 = TimeSeries.Axis()
        cm_3.title = "cm⁻³"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        raw.axes.append(cm_3)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "Raw CNC"
        n_cnc.data_record = f'{profile}-raw-cnc'
        n_cnc.data_field = 'cnc'
        raw.traces.append(n_cnc)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "Raw CNC2 (MAGIC)"
        n_cnc.data_record = f'{profile}-raw-cnc'
        n_cnc.data_field = 'cnc2'
        raw.traces.append(n_cnc)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        cm_3 = TimeSeries.Axis()
        cm_3.title = "cm⁻³"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        edited.axes.append(cm_3)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "Edited CNC"
        n_cnc.data_record = f'{profile}-editing-cnc'
        n_cnc.data_field = 'cnc'
        edited.traces.append(n_cnc)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "Edited CNC2 (MAGIC)"
        n_cnc.data_record = f'{profile}-editing-cnc'
        n_cnc.data_field = 'cnc2'
        edited.traces.append(n_cnc)


class ADMagicCPC250StatusStatusSecondary(ADMagicCPC250Status):
    def __init__(self, mode: str, **kwargs):
        super().__init__(mode, **kwargs)
        self.title = "MAGIC CPC Status"

        for g in self.graphs:
            for t in g.traces:
                t.data_record += '2'


class PublicCountsShort(PublicTimeSeries):
    STP = PublicCountsShortBase.STP

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(**kwargs)

        cnc = PublicTimeSeries.Graph()
        cnc.title = "Condensation Nucleus Concentration"
        self.graphs.append(cnc)

        cm_3 = PublicTimeSeries.Axis()
        cm_3.title = "Concentration (cm⁻³)"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        cnc.axes.append(cm_3)

        n_cnc = PublicTimeSeries.Trace(cm_3)
        n_cnc.legend = "CPC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'N'
        cnc.traces.append(n_cnc)
        self.processing[n_cnc.data_record] = self.STP()

        n_cnc2 = PublicTimeSeries.Trace(cm_3)
        n_cnc2.legend = "Magic"
        n_cnc2.data_record = f'{mode}-cnc2'
        n_cnc2.data_field = 'N'
        cnc.traces.append(n_cnc2)
        self.processing[n_cnc2.data_record] = self.STP()


        cpc_flow = PublicTimeSeries.Graph()
        cpc_flow.title = "CPC Flow"
        self.graphs.append(cpc_flow)

        lpm_cpc = PublicTimeSeries.Axis()
        lpm_cpc.title = "CPC (lpm)"
        lpm_cpc.format_code = '.3f'
        cpc_flow.axes.append(lpm_cpc)

        lpm_drier = PublicTimeSeries.Axis()
        lpm_drier.title = "Drier (lpm)"
        lpm_drier.format_code = '.3f'
        cpc_flow.axes.append(lpm_drier)

        sample = PublicTimeSeries.Trace(lpm_cpc)
        sample.legend = "Outlet"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Qsample'
        cpc_flow.traces.append(sample)

        drier = PublicTimeSeries.Trace(lpm_drier)
        drier.legend = "Drier"
        drier.data_record = f'{mode}-cpcstatus'
        drier.data_field = 'Qdrier'
        cpc_flow.traces.append(drier)

class PublicCountsLong(PublicCountsShort):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR
