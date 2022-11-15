import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.optical import Optical
from ..default.met.temperature import Temperature
from ..default.aerosol.aethalometer import AethalometerOptical


class EditingContaminationDetails(TimeSeries):
    ThreeWavelength = Optical.ThreeWavelength
    SevenWavelength = AethalometerOptical.SevenWavelength
    CalculateMissing = Temperature.CalculateMissing

    def __init__(self, profile: str = 'aerosol', **kwargs):
        super().__init__(**kwargs)

        self.title = "Contamination Details"

        speed = TimeSeries.Graph()
        speed.title = "Edited Wind Speed"
        speed.contamination = f'{profile}-editing-contamination'
        self.graphs.append(speed)

        mps = TimeSeries.Axis()
        mps.title = "m/s"
        mps.range = 0
        mps.format_code = '.1f'
        speed.axes.append(mps)

        ws = TimeSeries.Trace(mps)
        ws.legend = "Wind Speed"
        ws.data_record = f'{profile}-editing-wind'
        ws.data_field = 'WS'
        speed.traces.append(ws)


        direction = TimeSeries.Graph()
        direction.title = "Edited Wind Direction"
        direction.contamination = f'{profile}-editing-contamination'
        self.graphs.append(direction)

        degrees = TimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        direction.axes.append(degrees)

        wd = TimeSeries.Trace(degrees)
        wd.legend = "Wind Direction"
        wd.data_record = f'{profile}-editing-wind'
        wd.data_field = 'WD'
        wd.script_incoming_data = r"""(function() {
const plotIncomingData = incomingData;
const wrapper = new Winds.DirectionWrapper();
incomingData = (plotTime, values, epoch) => {
    const r = wrapper.apply(values, plotTime, epoch);
    plotIncomingData(r.times, r.direction, r.epoch);
};
})();"""
        direction.traces.append(wd)


        measurements = OrderedDict([
            ('{code}inlet', '{code}_V51 (inlet)'),
            ('{code}sample', '{code}_V11 (sample)'),
            ('{code}nephinlet', '{code}u_S11 (neph inlet)'),
            ('{code}neph', '{code}_S11 (neph sample)'),
            ('{code}aux', 'Auxiliary {type}'),
            ('{code}ambient', 'Ambient {type}'),
        ])
        omit_traces = {'TDnephinlet'}

        rh = TimeSeries.Graph()
        rh.title = "Raw Relative Humidity"
        rh.contamination = f'{profile}-editing-contamination'
        self.graphs.append(rh)
        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        temperature = TimeSeries.Graph()
        temperature.title = "Raw Temperature"
        temperature.contamination = f'{profile}-editing-contamination'
        self.graphs.append(temperature)
        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperature.axes.append(T_C)

        dewpoint = TimeSeries.Graph()
        dewpoint.title = "Raw Dewpoint"
        dewpoint.contamination = f'{profile}-editing-contamination'
        self.graphs.append(dewpoint)
        TD_C = TimeSeries.Axis()
        TD_C.title = "°C"
        TD_C.format_code = '.1f'
        dewpoint.axes.append(TD_C)

        for field, legend in measurements.items():
            trace = TimeSeries.Trace(rh_percent)
            trace.legend = legend.format(type='RH', code='U')
            trace.data_record = f'{profile}-raw-temperature'
            trace.data_field = field.format(code='U')
            if not omit_traces or trace.data_field not in omit_traces:
                rh.traces.append(trace)

            trace = TimeSeries.Trace(T_C)
            trace.legend = legend.format(type='Temperature', code='T')
            trace.data_record = f'{profile}-raw-temperature'
            trace.data_field = field.format(code='T')
            if not omit_traces or trace.data_field not in omit_traces:
                temperature.traces.append(trace)

            trace = TimeSeries.Trace(TD_C)
            trace.legend = legend.format(type='Dewpoint', code='TD')
            trace.data_record = f'{profile}-raw-temperature'
            trace.data_field = field.format(code='TD')
            if not omit_traces or trace.data_field not in omit_traces:
                dewpoint.traces.append(trace)

        self.processing[f'{profile}-raw-temperature'] = self.CalculateMissing()


        cnc = TimeSeries.Graph()
        cnc.title = "Edited Counts"
        cnc.contamination = f'{profile}-editing-contamination'
        self.graphs.append(cnc)

        cm_3 = TimeSeries.Axis()
        cm_3.title = "cm⁻³"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        cnc.axes.append(cm_3)

        n_cnc = TimeSeries.Trace(cm_3)
        n_cnc.legend = "CNC"
        n_cnc.data_record = f'{profile}-editing-cnc'
        n_cnc.data_field = 'cnc'
        cnc.traces.append(n_cnc)


        absorption = self.ThreeWavelength(f'{profile}-editing-absorption', 'Ba')
        absorption.title = "Edited Light Absorption"
        absorption.contamination = f'{profile}-editing-contamination'
        self.graphs.append(absorption)


        total_scattering = self.ThreeWavelength(f'{profile}-editing-scattering', 'Bs')
        total_scattering.title = "Edited Total Light Scattering"
        total_scattering.contamination = f'{profile}-editing-contamination'
        self.graphs.append(total_scattering)


        ebc = self.SevenWavelength("μg/m³", '.3f', "X ({wavelength} nm)", f'{profile}-editing-aethalometer', 'X{index}')
        ebc.title = "Edited Equivalent Black Carbon"
        ebc.contamination = f'{profile}-editing-contamination'
        self.graphs.append(ebc)

    @property
    def required_components(self) -> typing.List[str]:
        return super().required_components + ['winds']
