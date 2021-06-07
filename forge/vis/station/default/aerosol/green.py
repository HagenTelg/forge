import typing
from collections import OrderedDict
from forge.vis.view.timeseries import TimeSeries
from .aethalometer import AethalometerOptical


class Green(TimeSeries):
    class AdjustWavelength(TimeSeries.Processing):
        def __init__(self, input_fields: typing.Dict[str, float]):
            super().__init__()
            self.components.append('wavelength_adjust')
            self.script = r"""(function(dataName) {
const outputFields = new Map();
outputFields.set('G', 550);
const inputFields = new Map();
"""
            for field, wavelength in input_fields.items():
                self.script += f"inputFields.set('{field}', {wavelength});\n"
            self.script += r"""
return new WavelengthAdjust.AdjustedDispatch(dataName, inputFields, outputFields, 
    { center: 550, validDistance: 50, angstrom: 1.0 });
})"""

    def __init__(self, mode: str):
        super().__init__()
        self.title = "Optical Properties Adjusted to 550nm"

        scattering = TimeSeries.Graph()
        scattering.title = "Light Scattering"
        scattering.contamination = f'{mode}-contamination'
        self.graphs.append(scattering)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        scattering.axes.append(Mm_1)

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "Scattering (PM10)"
        G0.data_record = f'{mode}-scattering-pm10'
        G0.data_field = 'G'
        G0.color = '#0f0'
        scattering.traces.append(G0)
        self.processing[G0.data_record] = self.AdjustWavelength(OrderedDict([
            ('BsB', 450), ('BsG', 550), ('BsR', 700),
        ]))

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "Scattering (PM1)"
        G1.data_record = f'{mode}-scattering-pm1'
        G1.data_field = 'G'
        G1.color = '#070'
        scattering.traces.append(G1)
        self.processing[G1.data_record] = self.AdjustWavelength(OrderedDict([
            ('BsB', 450), ('BsG', 550), ('BsR', 700),
        ]))


        absorption = TimeSeries.Graph()
        absorption.title = "Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        absorption.axes.append(Mm_1)

        G0 = TimeSeries.Trace(Mm_1)
        G0.legend = "Absorption (PM10)"
        G0.data_record = f'{mode}-absorption-pm10'
        G0.data_field = 'G'
        G0.color = '#0f0'
        absorption.traces.append(G0)
        self.processing[G0.data_record] = self.AdjustWavelength(OrderedDict([
            ('BaB', 467), ('BaG', 528), ('BaR', 652),
        ]))

        G1 = TimeSeries.Trace(Mm_1)
        G1.legend = "Absorption (PM1)"
        G1.data_record = f'{mode}-absorption-pm1'
        G1.data_field = 'G'
        G1.color = '#070'
        absorption.traces.append(G1)
        self.processing[G1.data_record] = self.AdjustWavelength(OrderedDict([
            ('BaB', 467), ('BaG', 528), ('BaR', 652),
        ]))

        aethalometer = TimeSeries.Trace(Mm_1)
        aethalometer.legend = "Aethalometer"
        aethalometer.data_record = f'{mode}-aethalometer'
        aethalometer.data_field = 'G'
        absorption.traces.append(aethalometer)
        input_fields = OrderedDict()
        for index in range(len(AethalometerOptical.SevenWavelength.WAVELENGTH_NM)):
            input_fields[f'Ba{index+1}'] = AethalometerOptical.SevenWavelength.WAVELENGTH_NM[index]
        self.processing[aethalometer.data_record] = self.AdjustWavelength(input_fields)
