import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.tsi377Xcpc import TSI3772CPCStatus


class ParticleConcentration(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
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
        n_cnc.legend = "CNC (N71)"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc'
        cnc.traces.append(n_cnc)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "CNC2 (N72)"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc2'
        cnc.traces.append(n_cnc)


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
        n_cnc.legend = "Raw CNC (N71)"
        n_cnc.data_record = f'{profile}-raw-cnc'
        n_cnc.data_field = 'cnc'
        raw.traces.append(n_cnc)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "Raw CNC2 (N72)"
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
        n_cnc.legend = "Edited CNC (N71)"
        n_cnc.data_record = f'{profile}-editing-cnc'
        n_cnc.data_field = 'cnc'
        edited.traces.append(n_cnc)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "Edited CNC2 (N72)"
        n_cnc.data_record = f'{profile}-editing-cnc'
        n_cnc.data_field = 'cnc'
        edited.traces.append(n_cnc)


class TSI3772CPCStatusSecondary(TSI3772CPCStatus):
    def __init__(self, mode: str):
        super().__init__(mode)
        self.title = "Secondary CPC Status"

        for g in self.graphs:
            for t in g.traces:
                t.data_record += '2'
