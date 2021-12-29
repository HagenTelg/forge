import typing
from forge.vis.view.timeseries import TimeSeries


class PSAPStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "PSAP Status"

        flow_transmittance = TimeSeries.Graph()
        flow_transmittance.title = "Flow and Transmittance"
        self.graphs.append(flow_transmittance)

        transmittance = TimeSeries.Axis()
        transmittance.title = "Transmittance"
        transmittance.format_code = '.7f'
        flow_transmittance.axes.append(transmittance)

        flow = TimeSeries.Axis()
        flow.title = "lpm"
        flow.format_code = '.3f'
        flow_transmittance.axes.append(flow)

        IrG = TimeSeries.Trace(transmittance)
        IrG.legend = "Transmittance"
        IrG.data_record = f'{mode}-psapstatus'
        IrG.data_field = 'IrG'
        flow_transmittance.traces.append(IrG)

        Q = TimeSeries.Trace(flow)
        Q.legend = "Flow"
        Q.data_record = f'{mode}-psapstatus'
        Q.data_field = 'Q'
        flow_transmittance.traces.append(Q)


        intensities = TimeSeries.Graph()
        intensities.title = "Intensities"
        self.graphs.append(intensities)

        intensity = TimeSeries.Axis()
        intensity.title = "Intensity"
        intensity.format_code = '.2f'
        intensities.axes.append(intensity)

        reference = TimeSeries.Trace(intensity)
        reference.legend = "Reference"
        reference.data_record = f'{mode}-psapstatus'
        reference.data_field = 'IfG'
        intensities.traces.append(reference)

        sample = TimeSeries.Trace(intensity)
        sample.legend = "Sample"
        sample.data_record = f'{mode}-psapstatus'
        sample.data_field = 'IpG'
        intensities.traces.append(sample)
