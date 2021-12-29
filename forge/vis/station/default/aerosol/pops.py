import typing
from forge.vis.view.timeseries import TimeSeries
from forge.vis.view.sizedistribution import SizeDistribution, SizeCounts


class POPSStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "POPS Status"

        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperature.axes.append(degrees)

        t_of_p = TimeSeries.Trace(degrees)
        t_of_p.legend = "Temperature of Pressure"
        t_of_p.data_record = f'{mode}-popsstatus'
        t_of_p.data_field = 'Tpressure'
        temperature.traces.append(t_of_p)

        laser = TimeSeries.Trace(degrees)
        laser.legend = "Laser Temperature"
        laser.data_record = f'{mode}-popsstatus'
        laser.data_field = 'Tlaser'
        temperature.traces.append(laser)

        internal = TimeSeries.Trace(degrees)
        internal.legend = "Internal Temperature"
        internal.data_record = f'{mode}-popsstatus'
        internal.data_field = 'Tinternal'
        temperature.traces.append(internal)


        flow = TimeSeries.Graph()
        flow.title = "Flow"
        self.graphs.append(flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.3f'
        flow.axes.append(lpm)

        sample = TimeSeries.Trace(lpm)
        sample.legend = "Sample"
        sample.data_record = f'{mode}-popsstatus'
        sample.data_field = 'Qsample'
        flow.traces.append(sample)


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        aerosol = TimeSeries.Trace(hPa)
        aerosol.legend = "Board Pressure"
        aerosol.data_record = f'{mode}-popsstatus'
        aerosol.data_field = 'Pboard'
        pressure.traces.append(aerosol)


class POPSDistribution(SizeDistribution):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "POPS Size Distribution"

        self.contamination = f'{mode}-contamination'
        self.size_record = f'{mode}-pops'
        self.measured_record = f'{mode}-scattering-pm10'


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

        n_pops = SizeCounts.Trace()
        n_pops.legend = "POPS"
        n_pops.data_record = f'{mode}-pops'
        n_pops.data_field = 'N'
        self.traces.append(n_pops)
