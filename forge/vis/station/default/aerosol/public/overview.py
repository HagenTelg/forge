import typing
from collections import OrderedDict
from forge.vis.view.timeseries import PublicTimeSeries
from .counts import PublicCountsShort
from .nephelometer import PublicTSI3563Short
from .clap import PublicCLAPShort


class PublicOverviewShort(PublicTimeSeries):
    class Calculate(PublicTimeSeries.Processing):
        TRUNCATION_COARSE = "Truncation.AndersonOgren1998Coarse"
        TRUNCATION_FINE = "Truncation.AndersonOgren1998Fine"
        SCATTERING_TEMPERATURE = "Tneph"
        SCATTERING_PRESSURE = "Pneph"

        def __init__(
                self,
                input_scattering: typing.Optional[typing.Dict[str, float]] = None,
                input_backscattering: typing.Optional[typing.Dict[str, float]] = None,
                input_absorption: typing.Optional[typing.Dict[str, float]] = None,
                input_transmittance: typing.Optional[typing.Dict[str, str]] = None,
                scattering_truncation: str = "Truncation.AndersonOgren1998Coarse",
                scattering_temperature: str = "Tneph",
                scattering_pressure: str = "Pneph",
        ):
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
            if input_transmittance is None:
                input_transmittance = {ba: f"Ir{ba[-1:]}" for ba in input_absorption.keys()}

            self.components.append('stp')
            self.components.append('bond1999')
            self.components.append('truncation')
            self.components.append('wavelength_adjust')
            self.components.append('intensive')
            self.script = r"""(function(dataName) {
const scatteringSTPVars = [];
const truncationTotal = new Map();
const truncationBack = new Map();
const bond1999Absorption = new Map();
const intensiveOutputs = new Map();
intensiveOutputs.set('GI', 550);
const intensiveScattering = new Map();
const intensiveBackscattering = new Map();
const intensiveAbsorption = new Map();
"""
            self.script += f"const scatteringTruncation = {scattering_truncation};\n"
            if scattering_temperature:
                self.script += f"const scatteringTemperatureInput = '{scattering_temperature}';\n"
            else:
                self.script += f"const scatteringTemperatureInput = undefined;\n"
            if scattering_pressure:
                self.script += f"const scatteringPressureInput = '{scattering_pressure}';\n"
            else:
                self.script += f"const scatteringPressureInput = undefined;\n"

            for field, wavelength in input_scattering.items():
                self.script += f"intensiveScattering.set('{field}', {wavelength});\n"
                self.script += f"scatteringSTPVars.push('{field}');\n"
                self.script += f"truncationTotal.set('{field}', scatteringTruncation['{field}']);\n"
            for field, wavelength in input_backscattering.items():
                self.script += f"intensiveBackscattering.set('{field}', {wavelength});\n"
                self.script += f"scatteringSTPVars.push('{field}');\n"
                self.script += f"truncationBack.set('{field}', scatteringTruncation['{field}']);\n"
            for field, wavelength in input_absorption.items():
                self.script += f"intensiveAbsorption.set('{field}', {wavelength});\n"
                self.script += f"bond1999Absorption.set('{field}', {'{'} wavelength: {wavelength}, transmittance: '{input_transmittance.get(field)}' {'}'});\n"
            self.script += r"""
return new (class extends DataSocket.RecordDispatch {
    constructor(dataName) {
        super(dataName);
        if (scatteringTemperatureInput && scatteringPressureInput) {
            this.stp_scattering = new STP.CorrectOpticalRecord(scatteringSTPVars, scatteringTemperatureInput, scatteringPressureInput);
        } else {
            this.stp_scattering = undefined;
        }
        this.correct_absorption = new Bond1999.CorrectRecord(bond1999Absorption, intensiveScattering);
        this.correct_scattering_total = new Truncation.CorrectRecord(truncationTotal);
        this.correct_scattering_back = new Truncation.CorrectRecord(truncationBack);
        this.intensive = new Intensive.CalculateRecord(intensiveOutputs, intensiveScattering, intensiveBackscattering,
                intensiveAbsorption, new Map());
    }
    
    processRecord(record, epoch) {
        if (this.stp_scattering) {
            this.stp_scattering.correctRecord(record, epoch.length);
        }        
        this.correct_absorption.correctRecord(record, epoch.length);
        this.correct_scattering_total.correctRecord(record, epoch.length);
        this.correct_scattering_back.correctRecord(record, epoch.length);
        this.intensive.calculateRecord(record, epoch.length);
    }
})(dataName);
})"""

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(**kwargs)

        cnc = PublicTimeSeries.Graph()
        cnc.title = "Condensation Nucleus Concentration"
        self.graphs.append(cnc)

        cm_3 = PublicTimeSeries.Axis()
        cm_3.title = "Concentration (cm⁻³)"
        cm_3.range = 0
        cm_3.format_code = '.1f'
        cnc.axes.append(cm_3)

        n_cnc = PublicTimeSeries.Trace(cm_3)
        n_cnc.legend = "CPC"
        n_cnc.data_record = f'{mode}-cnc'
        n_cnc.data_field = 'N'
        cnc.traces.append(n_cnc)
        self.processing[n_cnc.data_record] = PublicCountsShort.STP()

        for size in ('whole', 'pm10'):
            self.processing[f'{mode}-intensive-{size}'] = self.Calculate(
                scattering_truncation=self.Calculate.TRUNCATION_COARSE,
                scattering_temperature=self.Calculate.SCATTERING_TEMPERATURE,
                scattering_pressure=self.Calculate.SCATTERING_PRESSURE,
            )
        for size in ('pm25', 'pm1'):
            self.processing[f'{mode}-intensive-{size}'] = self.Calculate(
                scattering_truncation=self.Calculate.TRUNCATION_FINE,
                scattering_temperature=self.Calculate.SCATTERING_TEMPERATURE,
                scattering_pressure=self.Calculate.SCATTERING_PRESSURE,
            )

        total_scattering = PublicTSI3563Short.ThreeWavelength(f'{mode}-intensive', 'Bs', "σsp {code} ({size})")
        total_scattering.title = "Scattering Coefficient"
        total_scattering.axes[-1].title = "σsp (Mm⁻¹)"
        self.graphs.append(total_scattering)

        absorption = PublicCLAPShort.ThreeWavelength(f'{mode}-intensive', 'Ba', "σap {code} ({size})")
        absorption.title = "Absorption Coefficient"
        absorption.axes[-1].title = "σap (Mm⁻¹)"
        self.graphs.append(absorption)

        albedo = PublicTimeSeries.Graph()
        albedo.title = "Single Scattering Albedo"
        self.graphs.append(albedo)

        albedo_unit = PublicTimeSeries.Axis()
        albedo_unit.title = "ω₀"
        albedo_unit.format_code = '.3f'
        albedo.axes.append(albedo_unit)

        bfr = PublicTimeSeries.Graph()
        bfr.title = "Backscatter Fraction"
        self.graphs.append(bfr)

        bfr_unit = PublicTimeSeries.Axis()
        bfr_unit.title = "σbsp / σsp"
        bfr_unit.format_code = '.3f'
        bfr.axes.append(bfr_unit)

        angstrom = PublicTimeSeries.Graph()
        angstrom.title = "Ångström Exponent"
        self.graphs.append(angstrom)

        angstrom_unit = PublicTimeSeries.Axis()
        angstrom_unit.format_code = '.3f'
        angstrom.axes.append(angstrom_unit)

        for size in [("Whole", 'whole', '#0f0', '#70f'), ("PM10", 'pm10', '#0f0', '#70f'),
                     ("PM2.5", 'pm25', '#070', '#407'), ("PM1", 'pm1', '#070', '#407')]:
            self.processing[f'{mode}-intensive-{size[1]}'] = self.Calculate()

            trace = PublicTimeSeries.Trace(albedo_unit)
            trace.legend = f"SSA ({size[0]})"
            trace.data_record = f'{mode}-intensive-{size[1]}'
            trace.data_field = 'SSAGI'
            trace.color = size[2]
            albedo.traces.append(trace)

            trace = PublicTimeSeries.Trace(bfr_unit)
            trace.legend = f"BbsG/BsG ({size[0]})"
            trace.data_record = f'{mode}-intensive-{size[1]}'
            trace.data_field = 'BfrGI'
            trace.color = size[2]
            bfr.traces.append(trace)

            trace = PublicTimeSeries.Trace(angstrom_unit)
            trace.legend = f"Åₛₚ ({size[0]})"
            trace.data_record = f'{mode}-intensive-{size[1]}'
            trace.data_field = 'AngBs'
            trace.color = size[2]
            angstrom.traces.append(trace)


class PublicOverviewLong(PublicOverviewShort):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR

