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

    def __init__(self, mode: str):
        super().__init__()
        self.title = "Intensive Parameters at 550nm"

        self.processing[f'{mode}-intensive-coarse'] = self.Calculate()
        self.processing[f'{mode}-intensive-fine'] = self.Calculate()

        albedo = TimeSeries.Graph()
        albedo.title = "Single Scattering Albedo"
        albedo.contamination = f'{mode}-contamination'
        self.graphs.append(albedo)

        no_unit = TimeSeries.Axis()
        no_unit.format_code = '.3f'
        albedo.axes.append(no_unit)

        G0 = TimeSeries.Trace(no_unit)
        G0.legend = "SSA (Coarse)"
        G0.data_record = f'{mode}-intensive-coarse'
        G0.data_field = 'SSAG'
        G0.color = '#0f0'
        albedo.traces.append(G0)

        G1 = TimeSeries.Trace(no_unit)
        G1.legend = "SSA (Fine)"
        G1.data_record = f'{mode}-intensive-fine'
        G1.data_field = 'SSAG'
        G1.color = '#070'
        albedo.traces.append(G1)


        bfr = TimeSeries.Graph()
        bfr.title = "Backscatter Fraction"
        bfr.contamination = f'{mode}-contamination'
        self.graphs.append(bfr)

        no_unit = TimeSeries.Axis()
        no_unit.format_code = '.3f'
        bfr.axes.append(no_unit)

        G0 = TimeSeries.Trace(no_unit)
        G0.legend = "BbsG/BsG (Coarse)"
        G0.data_record = f'{mode}-intensive-coarse'
        G0.data_field = 'BfrG'
        G0.color = '#0f0'
        bfr.traces.append(G0)

        G1 = TimeSeries.Trace(no_unit)
        G1.legend = "BbsG/BsG (Fine)"
        G1.data_record = f'{mode}-intensive-fine'
        G1.data_field = 'BfrG'
        G1.color = '#070'
        bfr.traces.append(G1)


        angstrom = TimeSeries.Graph()
        angstrom.title = "Ångström Exponent"
        angstrom.contamination = f'{mode}-contamination'
        self.graphs.append(angstrom)

        no_unit = TimeSeries.Axis()
        no_unit.format_code = '.3f'
        angstrom.axes.append(no_unit)

        G0 = TimeSeries.Trace(no_unit)
        G0.legend = "Å (Coarse)"
        G0.data_record = f'{mode}-intensive-coarse'
        G0.data_field = 'Ang'
        G0.color = '#0f0'
        angstrom.traces.append(G0)

        G1 = TimeSeries.Trace(no_unit)
        G1.legend = "Å (Fine)"
        G1.data_record = f'{mode}-intensive-fine'
        G1.data_field = 'Ang'
        G1.color = '#070'
        angstrom.traces.append(G1)
