import typing
from forge.vis.view.timeseries import TimeSeries


class CLAPStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "CLAP Status"

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
        IrG.data_record = f'{mode}-clapstatus'
        IrG.data_field = 'IrG'
        flow_transmittance.traces.append(IrG)

        Q = TimeSeries.Trace(flow)
        Q.legend = "Flow"
        Q.data_record = f'{mode}-clapstatus'
        Q.data_field = 'Q'
        flow_transmittance.traces.append(Q)


        intensities = TimeSeries.Graph()
        intensities.title = "Intensities"
        self.graphs.append(intensities)

        intensity = TimeSeries.Axis()
        intensity.title = "Intensity"
        intensity.format_code = '.2f'
        intensities.axes.append(intensity)

        spot_number = TimeSeries.Axis()
        spot_number.title = "Spot"
        spot_number.range = [0, 9]
        intensities.axes.append(spot_number)

        reference = TimeSeries.Trace(intensity)
        reference.legend = "Reference"
        reference.data_record = f'{mode}-clapstatus'
        reference.data_field = 'IfG'
        intensities.traces.append(reference)

        sample = TimeSeries.Trace(intensity)
        sample.legend = "Sample"
        sample.data_record = f'{mode}-clapstatus'
        sample.data_field = 'IpG'
        intensities.traces.append(sample)

        active_spot = TimeSeries.Trace(spot_number)
        active_spot.legend = "Spot"
        active_spot.data_record = f'{mode}-clapstatus'
        active_spot.data_field = 'spot'
        intensities.traces.append(active_spot)


        temperatures = TimeSeries.Graph()
        temperatures.title = "Temperatures"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.3f'
        temperatures.axes.append(degrees)

        sample = TimeSeries.Trace(degrees)
        sample.legend = "Sample Temperature"
        sample.data_record = f'{mode}-clapstatus'
        sample.data_field = 'Tsample'
        temperatures.traces.append(sample)

        case_temperature = TimeSeries.Trace(degrees)
        case_temperature.legend = "Case Temperature"
        case_temperature.data_record = f'{mode}-clapstatus'
        case_temperature.data_field = 'Tcase'
        temperatures.traces.append(case_temperature)
