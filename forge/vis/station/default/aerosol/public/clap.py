import typing
from forge.vis.view.timeseries import PublicTimeSeries
from ..optical import Optical
from ..aethalometer import AethalometerOptical


class _CLAPBase(PublicTimeSeries):
    ThreeWavelength = Optical.ThreeWavelength

    class _Bond1999(PublicTimeSeries.Processing):
        ABSORPTION_WAVELENGTHS: typing.Dict[str, float] = {
            'BaB': 467,
            'BaG': 528,
            'BaR': 652,
        }
        SCATTERING_WAVELENGTHS: typing.Dict[str, float] = None
        SCATTERING_STP_TEMPERATURE: typing.Optional[str] = None
        SCATTERING_STP_PRESSURE: typing.Optional[str] = None

        def __init__(self):
            super().__init__()
            self.components.append('stp')
            self.components.append('wavelength_adjust')
            self.components.append('bond1999')
            self.script = r"""(function(dataName) {
const absorption = new Map();
const scattering = new Map();
"""
            for field, wavelength in self.ABSORPTION_WAVELENGTHS.items():
                self.script += f"absorption.set('{field}', {'{'} wavelength: {wavelength}, transmittance: 'Ir{field[-1:]}' {'}'});\n"
            for field, wavelength in self.SCATTERING_WAVELENGTHS.items():
                self.script += f"scattering.set('{field}', {wavelength});\n"

            self.script += r"""return new Bond1999.CorrectDispatch(dataName, absorption, scattering"""
            if self.SCATTERING_STP_TEMPERATURE and self.SCATTERING_STP_PRESSURE:
                self.script += ", '" + self.SCATTERING_STP_TEMPERATURE + "', '" + self.SCATTERING_STP_PRESSURE + "'"
            self.script += r"""); })"""

        @classmethod
        def install(cls, processing: typing.Dict[str, PublicTimeSeries.Processing], record: str) -> None:
            for size in ('whole', 'pm10', 'pm25', 'pm1'):
                processing[f'{record}-{size}'] = cls()

    class Bond1999TSINeph(_Bond1999):
        SCATTERING_WAVELENGTHS = {
            'BsB': 450,
            'BsG': 550,
            'BsR': 700,
        }
        SCATTERING_STP_TEMPERATURE = "Tneph"
        SCATTERING_STP_PRESSURE = "Pneph"

    class Bond1999EcotechNeph(_Bond1999):
        SCATTERING_WAVELENGTHS = {
            'BsB': 450,
            'BsG': 525,
            'BsR': 635,
        }

    Bond1999: typing.Type["_CLAPBase._Bond1999"] = None

    def __init__(self, mode: str = 'public-aerosolweb', aethalometer: bool = False, **kwargs):
        super().__init__(**kwargs)

        absorption = self.ThreeWavelength(f'{mode}-absorption', 'Ba', "σap {code} ({size})")
        absorption.title = "Absorption Coefficient"
        absorption.axes[-1].title = "σap (Mm⁻¹)"
        self.graphs.append(absorption)

        self.Bond1999.install(self.processing, f'{mode}-absorption')


class PublicCLAPShort(_CLAPBase):
    Bond1999 = _CLAPBase.Bond1999TSINeph

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(mode=mode, **kwargs)

        flow_transmittance = PublicTimeSeries.Graph()
        flow_transmittance.title = "Flow and Transmittance"
        self.graphs.append(flow_transmittance)

        transmittance = PublicTimeSeries.Axis()
        transmittance.title = "Transmittance"
        transmittance.format_code = '.7f'
        flow_transmittance.axes.append(transmittance)

        flow = PublicTimeSeries.Axis()
        flow.title = "Flow (lpm)"
        flow.format_code = '.3f'
        flow_transmittance.axes.append(flow)

        IrG = PublicTimeSeries.Trace(transmittance)
        IrG.legend = "Transmittance"
        IrG.data_record = f'{mode}-clapstatus'
        IrG.data_field = 'IrG'
        flow_transmittance.traces.append(IrG)

        Q = PublicTimeSeries.Trace(flow)
        Q.legend = "Flow"
        Q.data_record = f'{mode}-clapstatus'
        Q.data_field = 'Q'
        flow_transmittance.traces.append(Q)


        intensities = PublicTimeSeries.Graph()
        intensities.title = "Intensities"
        self.graphs.append(intensities)

        intensity = PublicTimeSeries.Axis()
        intensity.title = "Intensity"
        intensity.format_code = '.2f'
        intensities.axes.append(intensity)

        spot_number = PublicTimeSeries.Axis()
        spot_number.title = "Spot"
        spot_number.range = [0, 9]
        intensities.axes.append(spot_number)

        reference = PublicTimeSeries.Trace(intensity)
        reference.legend = "Reference"
        reference.data_record = f'{mode}-clapstatus'
        reference.data_field = 'IfG'
        intensities.traces.append(reference)

        sample = PublicTimeSeries.Trace(intensity)
        sample.legend = "Sample"
        sample.data_record = f'{mode}-clapstatus'
        sample.data_field = 'IpG'
        intensities.traces.append(sample)

        active_spot = PublicTimeSeries.Trace(spot_number)
        active_spot.legend = "Spot"
        active_spot.data_record = f'{mode}-clapstatus'
        active_spot.data_field = 'spot'
        intensities.traces.append(active_spot)


class PublicCLAPLong(PublicCLAPShort):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR


class PublicCLAPAethalometerShort(_CLAPBase):
    Bond1999 = _CLAPBase.Bond1999TSINeph
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(mode=mode, **kwargs)

        absorption_ae = self.SevenWavelength("σap (Mm⁻¹)", '.2f', "σap {wavelength} nm",
                                             f'{mode}-aethalometeroverview', 'Ba{index}')
        absorption_ae.title = "Aethalometer Absorption Coefficient"
        self.graphs.append(absorption_ae)


class PublicCLAPAethalometerLong(PublicCLAPAethalometerShort):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR
