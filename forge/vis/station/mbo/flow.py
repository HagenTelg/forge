import typing
from forge.vis.view.timeseries import TimeSeries


class Flow(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "System Flow"

        system_flow = TimeSeries.Graph()
        self.graphs.append(system_flow)

        lpm_sample = TimeSeries.Axis()
        lpm_sample.title = "lpm"
        lpm_sample.range = [0, 50]
        lpm_sample.format_code = '.2f'
        system_flow.axes.append(lpm_sample)

        lpm_gas = TimeSeries.Axis()
        lpm_gas.title = "Gas Sampler (lpm)"
        lpm_gas.format_code = '.3f'
        system_flow.axes.append(lpm_gas)

        sample_flow = TimeSeries.Trace(lpm_sample)
        sample_flow.legend = "Q_Q11 (sample)"
        sample_flow.data_record = f'{mode}-flow'
        sample_flow.data_field = 'sample'
        system_flow.traces.append(sample_flow)

        dilution_flow = TimeSeries.Trace(lpm_sample)
        dilution_flow.legend = "Q_Q12 (dilution)"
        dilution_flow.data_record = f'{mode}-flow'
        dilution_flow.data_field = 'dilution'
        system_flow.traces.append(dilution_flow)

        gas_flow = TimeSeries.Trace(lpm_sample)
        gas_flow.legend = "Q_Q81 (gas)"
        gas_flow.data_record = f'{mode}-flow'
        gas_flow.data_field = 'gas'
        system_flow.traces.append(gas_flow)
