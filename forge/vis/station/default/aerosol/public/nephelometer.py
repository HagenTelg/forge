import typing
from forge.vis.view.timeseries import PublicTimeSeries
from ..optical import Optical
from ..tsi3563nephelometer import NephelometerZero


class _NephelometerBase(PublicTimeSeries):
    ThreeWavelength = Optical.ThreeWavelength
    ZeroThreeWavelength = NephelometerZero.ThreeWavelength

    class Truncation(PublicTimeSeries.Processing):
        DISPATCH: str = None
        COARSE: str = None
        FINE: str = None

        def __init__(self, constants: str):
            super().__init__()
            self.components.append('stp')
            self.components.append('truncation')
            self.script = r"""(function(dataName) {
    return new """ + self.DISPATCH + r"""(dataName, """ + constants + """);
})"""

        @classmethod
        def install(cls, processing: typing.Dict[str, PublicTimeSeries.Processing], record: str) -> None:
            for size in ('whole', 'pm10'):
                processing[f'{record}-{size}'] = cls(cls.COARSE)
            for size in ('pm25', 'pm1'):
                processing[f'{record}-{size}'] = cls(cls.FINE)

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(**kwargs)

        total_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bs', "σsp {code} ({size})")
        total_scattering.title = "Scattering Coefficient"
        total_scattering.axes[-1].title = "σsp (Mm⁻¹)"
        self.graphs.append(total_scattering)

        back_scattering = self.ThreeWavelength(f'{mode}-scattering', 'Bbs', "σbsp {code} ({size})")
        back_scattering.title = "Backwards-hemispheric Scattering Coefficient"
        total_scattering.axes[-1].title = "σbsp (Mm⁻¹)"
        self.graphs.append(back_scattering)

        self.Truncation.install(self.processing, f'{mode}-scattering')


class PublicTSI3563Short(_NephelometerBase):
    class Truncation(_NephelometerBase.Truncation):
        DISPATCH = "Truncation.TSI3563Dispatch"
        COARSE = "Truncation.AndersonOgren1998Coarse"
        FINE = "Truncation.AndersonOgren1998Fine"

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(mode=mode, **kwargs)

        reference = PublicTimeSeries.Graph()
        reference.title = "Calibrator Count Rate"
        self.graphs.append(reference)

        Hz = PublicTimeSeries.Axis()
        Hz.title = "Hz"
        Hz.format_code = '.0f'
        reference.axes.append(Hz)

        CfG = PublicTimeSeries.Trace(Hz)
        CfG.legend = "Green Reference"
        CfG.data_record = f'{mode}-nephstatus'
        CfG.data_field = 'CfG'
        CfG.color = '#0f0'
        reference.traces.append(CfG)


        lamp = PublicTimeSeries.Graph()
        lamp.title = "Lamp"
        self.graphs.append(lamp)

        A = PublicTimeSeries.Axis()
        A.title = "A"
        A.format_code = '.1f'
        lamp.axes.append(A)

        V = PublicTimeSeries.Axis()
        V.title = "V"
        V.format_code = '.1f'
        lamp.axes.append(V)

        lamp_current = PublicTimeSeries.Trace(A)
        lamp_current.legend = "Current"
        lamp_current.data_record = f'{mode}-nephstatus'
        lamp_current.data_field = 'Al'
        lamp.traces.append(lamp_current)

        lamp_voltage = PublicTimeSeries.Trace(V)
        lamp_voltage.legend = "Voltage"
        lamp_voltage.data_record = f'{mode}-nephstatus'
        lamp_voltage.data_field = 'Vl'
        lamp.traces.append(lamp_voltage)


        zero_total_scattering = self.ZeroThreeWavelength(f'{mode}-nephzero', 'Bsw')
        zero_total_scattering.title = "Wall Scattering Contribution"
        self.graphs.append(zero_total_scattering)

        zero_back_scattering = self.ZeroThreeWavelength(f'{mode}-nephzero', 'Bbsw')
        zero_back_scattering.title = "Backwards-hemispheric Wall Scattering Contribution"
        self.graphs.append(zero_back_scattering)


class PublicTSI3563Long(PublicTSI3563Short):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR

