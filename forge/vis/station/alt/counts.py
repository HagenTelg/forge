import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.tsi377Xcpc import TSI377XCPCStatus
from .smps import SMPSCounts


class EditingParticleConcentration(TimeSeries):
    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
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
        n_cnc.legend = "Raw CNC2"
        n_cnc.data_record = f'{profile}-raw-cnc'
        n_cnc.data_field = 'cnc2'
        raw.traces.append(n_cnc)

        n_smps = TimeSeries.Trace(cm_3)
        n_smps.legend = "Raw SMPS"
        n_smps.data_record = f'{profile}-raw-smps'
        n_smps.data_field = 'N'
        raw.traces.append(n_smps)
        self.processing[n_smps.data_record] = SMPSCounts.IntegrateSizeDistribution('N')

        n_grimm = TimeSeries.Trace(cm_3)
        n_grimm.legend = "Raw Grimm"
        n_grimm.data_record = f'{profile}-raw-grimm'
        n_grimm.data_field = 'N'
        raw.traces.append(n_grimm)
        self.processing[n_grimm.data_record] = SMPSCounts.IntegrateSizeDistribution('N')


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
        n_cnc.legend = "Edited CNC2"
        n_cnc.data_record = f'{profile}-editing-cnc'
        n_cnc.data_field = 'cnc2'
        edited.traces.append(n_cnc)

        n_smps = TimeSeries.Trace(cm_3)
        n_smps.legend = "Edited SMPS"
        n_smps.data_record = f'{profile}-editing-smps'
        n_smps.data_field = 'N'
        edited.traces.append(n_smps)
        self.processing[n_smps.data_record] = SMPSCounts.IntegrateSizeDistribution('N')

        n_grimm = TimeSeries.Trace(cm_3)
        n_grimm.legend = "Edited Grimm"
        n_grimm.data_record = f'{profile}-editing-grimm'
        n_grimm.data_field = 'N'
        edited.traces.append(n_grimm)
        self.processing[n_grimm.data_record] = SMPSCounts.IntegrateSizeDistribution('N')


class TSI377XCPCStatusSecondary(TSI377XCPCStatus):
    def __init__(self, mode: str):
        super().__init__(mode)
        self.title = "Secondary CPC Status"

        for g in self.graphs:
            for t in g.traces:
                t.data_record += '2'
