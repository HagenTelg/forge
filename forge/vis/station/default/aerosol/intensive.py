import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries


class Intensive(TimeSeries):
    class Calculate(TimeSeries.Processing):
        def __init__(self, input_scattering: typing.Optional[typing.Dict[str, float]] = None,
                     input_backscattering: typing.Optional[typing.Dict[str, float]] = None,
                     input_absorption: typing.Optional[typing.Dict[str, float]] = None,
                     input_extinction: typing.Optional[typing.Dict[str, float]] = None):
            super().__init__()

            if input_scattering is None:
                input_scattering = OrderedDict([
                    ('BsB', 450), ('BsG', 550), ('BsR', 700),
                ])
            if input_backscattering is None:
                input_backscattering = OrderedDict([
                    ('BbsB', 450), ('BbsG', 550), ('BbsR', 700),
                ])
            if input_absorption is None:
                input_absorption = OrderedDict([
                    ('BaB', 467), ('BaG', 528), ('BaR', 652),
                ])
            if input_extinction is None:
                input_extinction = dict()

            self.components.append('wavelength_adjust')
            self.components.append('intensive')
            self.script = r"""(function(dataName) {
const outputNames = new Map();
outputNames.set('G', 550);
const inputScattering = new Map();
const inputBackscattering = new Map();
const inputAbsorption = new Map();
const inputExtinction = new Map();
"""
            for field, wavelength in input_scattering.items():
                self.script += f"inputScattering.set('{field}', {wavelength});\n"
            for field, wavelength in input_backscattering.items():
                self.script += f"inputBackscattering.set('{field}', {wavelength});\n"
            for field, wavelength in input_absorption.items():
                self.script += f"inputAbsorption.set('{field}', {wavelength});\n"
            for field, wavelength in input_extinction.items():
                self.script += f"inputExtinction.set('{field}', {wavelength});\n"
            self.script += r"""
return new Intensive.CalculateDispatch(dataName, outputNames, 
    inputScattering, inputBackscattering, inputAbsorption, inputExtinction);
    })"""

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Intensive Parameters at 550nm"

        albedo = TimeSeries.Graph()
        albedo.title = "Single Scattering Albedo"
        albedo.contamination = f'{mode}-contamination'
        self.graphs.append(albedo)

        albedo_unit = TimeSeries.Axis()
        albedo_unit.format_code = '.3f'
        albedo.axes.append(albedo_unit)

        bfr = TimeSeries.Graph()
        bfr.title = "Backscatter Fraction"
        bfr.contamination = f'{mode}-contamination'
        self.graphs.append(bfr)

        bfr_unit = TimeSeries.Axis()
        bfr_unit.format_code = '.3f'
        bfr.axes.append(bfr_unit)

        angstrom = TimeSeries.Graph()
        angstrom.title = "Ångström Exponent"
        angstrom.contamination = f'{mode}-contamination'
        self.graphs.append(angstrom)

        angstrom_unit = TimeSeries.Axis()
        angstrom_unit.format_code = '.3f'
        angstrom.axes.append(angstrom_unit)

        for size in [("Whole", 'whole', '#0f0', '#70f'), ("PM10", 'pm10', '#0f0', '#70f'),
                     ("PM2.5", 'pm25', '#070', '#407'), ("PM1", 'pm1', '#070', '#407')]:
            self.processing[f'{mode}-intensive-{size[1]}'] = self.Calculate()

            trace = TimeSeries.Trace(albedo_unit)
            trace.legend = f"SSA ({size[0]})"
            trace.data_record = f'{mode}-intensive-{size[1]}'
            trace.data_field = 'SSAG'
            trace.color = size[2]
            albedo.traces.append(trace)

            trace = TimeSeries.Trace(bfr_unit)
            trace.legend = f"BbsG/BsG ({size[0]})"
            trace.data_record = f'{mode}-intensive-{size[1]}'
            trace.data_field = 'BfrG'
            trace.color = size[2]
            bfr.traces.append(trace)

            trace = TimeSeries.Trace(angstrom_unit)
            trace.legend = f"Åₛₚ ({size[0]})"
            trace.data_record = f'{mode}-intensive-{size[1]}'
            trace.data_field = 'AngBs'
            trace.color = size[2]
            angstrom.traces.append(trace)

            trace = TimeSeries.Trace(angstrom_unit)
            trace.legend = f"Åₐₚ ({size[0]})"
            trace.data_record = f'{mode}-intensive-{size[1]}'
            trace.data_field = 'AngBa'
            trace.color = size[3]
            angstrom.traces.append(trace)

