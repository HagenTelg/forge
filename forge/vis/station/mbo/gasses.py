import typing
from forge.vis.view.timeseries import TimeSeries


class Gasses(TimeSeries):
    class ScaleCO2(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
    const op = new GenericOperations.SingleOutput(dataName, GenericOperations.divide, 'CO2', 'CO2');
    op.after.push(1000.0);
    return op;
})"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Gas Concentrations"

        ozone = TimeSeries.Graph()
        ozone.title = "Ozone"
        ozone.contamination = f'{mode}-contamination'
        self.graphs.append(ozone)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        ozone.axes.append(ppb)

        thermo = TimeSeries.Trace(ppb)
        thermo.legend = "Thermo"
        thermo.data_record = f'{mode}-ozone'
        thermo.data_field = 'thermo'
        ozone.traces.append(thermo)

        ecotech = TimeSeries.Trace(ppb)
        ecotech.legend = "Ecotech"
        ecotech.data_record = f'{mode}-ozone'
        ecotech.data_field = 'ecotech'
        ozone.traces.append(ecotech)


        cox = TimeSeries.Graph()
        cox.title = "CO and CO₂"
        cox.contamination = f'{mode}-contamination'
        self.graphs.append(cox)

        co_ppb = TimeSeries.Axis()
        co_ppb.title = "CO (ppb)"
        co_ppb.format_code = '.1f'
        cox.axes.append(co_ppb)

        co2_ppm = TimeSeries.Axis()
        co2_ppm.title = "CO₂ (ppm)"
        co2_ppm.format_code = '.1f'
        cox.axes.append(co2_ppm)

        CO = TimeSeries.Trace(co_ppb)
        CO.legend = "CO"
        CO.data_record = f'{mode}-gasses'
        CO.data_field = 'CO'
        cox.traces.append(CO)

        CO2 = TimeSeries.Trace(co2_ppm)
        CO2.legend = "CO₂"
        CO2.data_record = f'{mode}-gasses'
        CO2.data_field = 'CO2'
        cox.traces.append(CO2)
        self.processing[CO2.data_record] = self.ScaleCO2()
