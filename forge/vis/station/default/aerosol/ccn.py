import typing
from forge.vis.view.timeseries import TimeSeries


class CCNStatus(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "CCN Status"

        temperatures = TimeSeries.Graph()
        temperatures.title = "Temperature"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        inlet = TimeSeries.Trace(degrees)
        inlet.legend = "Inlet"
        inlet.data_record = f'{mode}-ccnstatus'
        inlet.data_field = 'Tinlet'
        temperatures.traces.append(inlet)

        tec = TimeSeries.Trace(degrees)
        tec.legend = "TEC 1"
        tec.data_record = f'{mode}-ccnstatus'
        tec.data_field = 'Ttec1'
        temperatures.traces.append(tec)

        tec = TimeSeries.Trace(degrees)
        tec.legend = "TEC 2"
        tec.data_record = f'{mode}-ccnstatus'
        tec.data_field = 'Ttec2'
        temperatures.traces.append(tec)

        tec = TimeSeries.Trace(degrees)
        tec.legend = "TEC 3"
        tec.data_record = f'{mode}-ccnstatus'
        tec.data_field = 'Ttec3'
        temperatures.traces.append(tec)

        sample = TimeSeries.Trace(degrees)
        sample.legend = "Sample"
        sample.data_record = f'{mode}-ccnstatus'
        sample.data_field = 'Tsample'
        temperatures.traces.append(sample)

        opc = TimeSeries.Trace(degrees)
        opc.legend = "OPC"
        opc.data_record = f'{mode}-ccnstatus'
        opc.data_field = 'Topc'
        temperatures.traces.append(opc)

        nafion = TimeSeries.Trace(degrees)
        nafion.legend = "Nafion"
        nafion.data_record = f'{mode}-ccnstatus'
        nafion.data_field = 'Tnafion'
        temperatures.traces.append(nafion)


        ccn_flow = TimeSeries.Graph()
        ccn_flow.title = "Flow"
        self.graphs.append(ccn_flow)

        lpm_sample = TimeSeries.Axis()
        lpm_sample.title = "Sample (lpm)"
        lpm_sample.format_code = '.3f'
        ccn_flow.axes.append(lpm_sample)

        lpm_sheath = TimeSeries.Axis()
        lpm_sheath.title = "Sheath (lpm)"
        lpm_sheath.format_code = '.2f'
        ccn_flow.axes.append(lpm_sheath)

        sample = TimeSeries.Trace(lpm_sample)
        sample.legend = "Sample Flow"
        sample.data_record = f'{mode}-ccnstatus'
        sample.data_field = 'Qsample'
        ccn_flow.traces.append(sample)

        sheath = TimeSeries.Trace(lpm_sheath)
        sheath.legend = "Sheath Flow"
        sheath.data_record = f'{mode}-ccnstatus'
        sheath.data_field = 'Qsheath'
        ccn_flow.traces.append(sheath)


        ccn_ss = TimeSeries.Graph()
        ccn_ss.title = "Supersaturation"
        self.graphs.append(ccn_ss)

        percent = TimeSeries.Axis()
        percent.title = "%"
        percent.format_code = '.2f'
        ccn_ss.axes.append(percent)

        calculated = TimeSeries.Trace(percent)
        calculated.legend = "Calculated"
        calculated.data_record = f'{mode}-ccnstatus'
        calculated.data_field = 'SScalc'
        ccn_ss.traces.append(calculated)
