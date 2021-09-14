import typing
from forge.vis.view.timeseries import TimeSeries
from forge.vis.view.sizedistribution import SizeCounts


class ParticleConcentration(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Particle Concentration"

        cnc = TimeSeries.Graph()
        cnc.title = "Condensation Nucleus Concentration"
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

        n_grimm = TimeSeries.Trace(cm_3)
        n_grimm.legend = "Grimm"
        n_grimm.data_record = f'{mode}-grimm'
        n_grimm.data_field = 'N'
        cnc.traces.append(n_grimm)
        self.processing[n_grimm.data_record] = SizeCounts.IntegrateSizeDistribution('N')


        mass = TimeSeries.Graph()
        mass.title = "Mass Concentration"
        mass.contamination = f'{mode}-contamination'
        self.graphs.append(mass)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.1f'
        mass.axes.append(ugm3)

        pm10 = TimeSeries.Trace(ugm3)
        pm10.legend = "Grimm PM10"
        pm10.data_record = f'{mode}-grimm'
        pm10.data_field = 'PM10'
        mass.traces.append(pm10)

        pm25 = TimeSeries.Trace(ugm3)
        pm25.legend = "Grimm PM2.5"
        pm25.data_record = f'{mode}-grimm'
        pm25.data_field = 'PM25'
        mass.traces.append(pm25)

        pm1 = TimeSeries.Trace(ugm3)
        pm1.legend = "Grimm PM1"
        pm1.data_record = f'{mode}-grimm'
        pm1.data_field = 'PM1'
        mass.traces.append(pm1)


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

        n_grimm = TimeSeries.Trace(cm_3)
        n_grimm.legend = "Raw Grimm"
        n_grimm.data_record = f'{profile}-raw-grimm'
        n_grimm.data_field = 'N'
        raw.traces.append(n_grimm)
        self.processing[n_grimm.data_record] = SizeCounts.IntegrateSizeDistribution('N')


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

        n_grimm = TimeSeries.Trace(cm_3)
        n_grimm.legend = "Edited Grimm"
        n_grimm.data_record = f'{profile}-editing-grimm'
        n_grimm.data_field = 'N'
        edited.traces.append(n_grimm)
        self.processing[n_grimm.data_record] = SizeCounts.IntegrateSizeDistribution('N')


class EditingGrimm(TimeSeries):
    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "OPC Mass Concentration"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.1f'
        raw.axes.append(ugm3)

        pm10 = TimeSeries.Trace(ugm3)
        pm10.legend = "Raw PM10"
        pm10.data_record = f'{profile}-raw-grimm'
        pm10.data_field = 'PM10'
        raw.traces.append(pm10)

        pm25 = TimeSeries.Trace(ugm3)
        pm25.legend = "Raw PM2.5"
        pm25.data_record = f'{profile}-raw-grimm'
        pm25.data_field = 'PM25'
        raw.traces.append(pm25)

        pm1 = TimeSeries.Trace(ugm3)
        pm1.legend = "Raw PM1"
        pm1.data_record = f'{profile}-raw-grimm'
        pm1.data_field = 'PM1'
        raw.traces.append(pm1)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.1f'
        edited.axes.append(ugm3)

        pm10 = TimeSeries.Trace(ugm3)
        pm10.legend = "Edited PM10"
        pm10.data_record = f'{profile}-editing-grimm'
        pm10.data_field = 'PM10'
        edited.traces.append(pm10)

        pm25 = TimeSeries.Trace(ugm3)
        pm25.legend = "Edited PM2.5"
        pm25.data_record = f'{profile}-editing-grimm'
        pm25.data_field = 'PM25'
        edited.traces.append(pm25)

        pm1 = TimeSeries.Trace(ugm3)
        pm1.legend = "Edited PM1"
        pm1.data_record = f'{profile}-editing-grimm'
        pm1.data_field = 'PM1'
        edited.traces.append(pm1)
