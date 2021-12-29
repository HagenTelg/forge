import typing
from forge.vis.view.timeseries import TimeSeries


class TSI3772CPCStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "CPC Status"

        temperatures = TimeSeries.Graph()
        temperatures.title = "Temperature"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        saturator = TimeSeries.Trace(degrees)
        saturator.legend = "Saturator"
        saturator.data_record = f'{mode}-cpcstatus'
        saturator.data_field = 'Tsaturator'
        temperatures.traces.append(saturator)

        condenser = TimeSeries.Trace(degrees)
        condenser.legend = "Condenser"
        condenser.data_record = f'{mode}-cpcstatus'
        condenser.data_field = 'Tcondenser'
        temperatures.traces.append(condenser)

        optics = TimeSeries.Trace(degrees)
        optics.legend = "Optics"
        optics.data_record = f'{mode}-cpcstatus'
        optics.data_field = 'Toptics'
        temperatures.traces.append(optics)

        cabinet = TimeSeries.Trace(degrees)
        cabinet.legend = "Cabinet"
        cabinet.data_record = f'{mode}-cpcstatus'
        cabinet.data_field = 'Tcabinet'
        temperatures.traces.append(cabinet)


        cpc_flow = TimeSeries.Graph()
        cpc_flow.title = "Flow"
        self.graphs.append(cpc_flow)

        lpm_sample = TimeSeries.Axis()
        lpm_sample.title = "Sample (lpm)"
        lpm_sample.format_code = '.3f'
        cpc_flow.axes.append(lpm_sample)

        lpm_inlet = TimeSeries.Axis()
        lpm_inlet.title = "Inlet (lpm)"
        lpm_inlet.format_code = '.2f'
        cpc_flow.axes.append(lpm_inlet)

        sample = TimeSeries.Trace(lpm_sample)
        sample.legend = "Sample"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Qsample'
        cpc_flow.traces.append(sample)

        inlet = TimeSeries.Trace(lpm_inlet)
        inlet.legend = "Inlet"
        inlet.data_record = f'{mode}-cpcstatus'
        inlet.data_field = 'Qinlet'
        cpc_flow.traces.append(inlet)


class TSI3775CPCStatus(TSI3772CPCStatus):
    def __init__(self, mode: str, **kwargs):
        super().__init__(mode, **kwargs)

        pressure = TimeSeries.Graph()
        pressure.title = "Absolute Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        sample = TimeSeries.Trace(hPa)
        sample.legend = "Sample Pressure"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Psample'
        pressure.traces.append(sample)


        pressure_drop = TimeSeries.Graph()
        pressure_drop.title = "Pressure Drop"
        self.graphs.append(pressure_drop)

        hPa_nozzle = TimeSeries.Axis()
        hPa_nozzle.title = "Nozzle (hPa)"
        hPa_nozzle.format_code = '.3f'
        pressure_drop.axes.append(hPa_nozzle)

        hPa_orifice = TimeSeries.Axis()
        hPa_orifice.title = "Orifice (hPa)"
        hPa_orifice.format_code = '.1f'
        pressure_drop.axes.append(hPa_orifice)

        nozzle = TimeSeries.Trace(hPa_nozzle)
        nozzle.legend = "Nozzle Pressure Drop"
        nozzle.data_record = f'{mode}-cpcstatus'
        nozzle.data_field = 'PDnozzle'
        pressure_drop.traces.append(nozzle)

        orifice = TimeSeries.Trace(hPa_orifice)
        orifice.legend = "Orifice Pressure Drop"
        orifice.data_record = f'{mode}-cpcstatus'
        orifice.data_field = 'ODorifice'
        pressure_drop.traces.append(orifice)


        laser = TimeSeries.Graph()
        laser.title = "Laser"
        self.graphs.append(laser)

        mA = TimeSeries.Axis()
        mA.title = "mA"
        mA.format_code = '.0f'
        laser.axes.append(mA)

        sample = TimeSeries.Trace(mA)
        sample.legend = "Laser Current"
        sample.data_record = f'{mode}-cpcstatus'
        sample.data_field = 'Alaser'
        laser.traces.append(sample)


TSI3776CPCStatus = TSI3775CPCStatus
