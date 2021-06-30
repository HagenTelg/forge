import typing
from forge.vis.view.timeseries import TimeSeries
from forge.vis.view.sizedistribution import SizeDistribution, SizeCounts


class DMPSStatus(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "DMPS Status"

        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperature.axes.append(degrees)

        aerosol = TimeSeries.Trace(degrees)
        aerosol.legend = "Aerosol Temperature"
        aerosol.data_record = f'{mode}-dmpsstatus'
        aerosol.data_field = 'Taerosol'
        temperature.traces.append(aerosol)

        sheath = TimeSeries.Trace(degrees)
        sheath.legend = "Sheath Temperature"
        sheath.data_record = f'{mode}-dmpsstatus'
        sheath.data_field = 'Tsheath'
        temperature.traces.append(sheath)


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        aerosol = TimeSeries.Trace(hPa)
        aerosol.legend = "Aerosol Pressure"
        aerosol.data_record = f'{mode}-dmpsstatus'
        aerosol.data_field = 'Paerosol'
        pressure.traces.append(aerosol)

        sheath = TimeSeries.Trace(hPa)
        sheath.legend = "Sheath Pressure"
        sheath.data_record = f'{mode}-dmpsstatus'
        sheath.data_field = 'Psheath'
        pressure.traces.append(sheath)


        flow = TimeSeries.Graph()
        flow.title = "Flow"
        self.graphs.append(flow)

        lpm_aerosol = TimeSeries.Axis()
        lpm_aerosol.title = "Aerosol (lpm)"
        lpm_aerosol.format_code = '.2f'
        flow.axes.append(lpm_aerosol)

        lpm_sheath = TimeSeries.Axis()
        lpm_sheath.title = "Sheath (lpm)"
        lpm_sheath.format_code = '.2f'
        flow.axes.append(lpm_sheath)

        aerosol = TimeSeries.Trace(lpm_aerosol)
        aerosol.legend = "Aerosol Flow"
        aerosol.data_record = f'{mode}-dmpsstatus'
        aerosol.data_field = 'Qaerosol'
        flow.traces.append(aerosol)

        sheath = TimeSeries.Trace(lpm_sheath)
        sheath.legend = "Sheath Flow"
        sheath.data_record = f'{mode}-dmpsstatus'
        sheath.data_field = 'Qsheath'
        flow.traces.append(sheath)


class DMPSDistribution(SizeDistribution):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "DMPS Size Distribution"

        self.contamination = f'{mode}-contamination'
        self.size_record = f'{mode}-dmps'
        self.measured_record = f'{mode}-scattering-fine'


class DMPSCounts(SizeCounts):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Particle Concentration"

        self.contamination = f'{mode}-contamination'
        self.size_record = f'{mode}-dmps'
        self.processing[self.size_record] = self.IntegrateSizeDistribution('N', 'Nabove', 0.014)

        n_cnc = SizeCounts.Trace()
        n_cnc.legend = "CNC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'cnc'
        self.traces.append(n_cnc)

        n_dmps = SizeCounts.Trace()
        n_dmps.legend = "DMPS"
        n_dmps.data_record = f'{mode}-dmps'
        n_dmps.data_field = 'N'
        self.traces.append(n_dmps)

        n_dmps_cpc = SizeCounts.Trace()
        n_dmps_cpc.legend = "DMPS (above 14nm)"
        n_dmps_cpc.data_record = f'{mode}-dmps'
        n_dmps_cpc.data_field = 'Nabove'
        self.traces.append(n_dmps_cpc)

        n_dmps_raw = SizeCounts.Trace()
        n_dmps_raw.legend = "DMPS (raw)"
        n_dmps_raw.data_record = f'{mode}-dmps'
        n_dmps_raw.data_field = 'Nraw'
        self.traces.append(n_dmps_raw)

        n_pops = SizeCounts.Trace()
        n_pops.legend = "POPS"
        n_pops.data_record = f'{mode}-pops'
        n_pops.data_field = 'N'
        self.traces.append(n_pops)
        self.processing[n_pops.data_record] = self.IntegrateSizeDistribution('N')
