import typing
from forge.vis.view.timeseries import PublicTimeSeries
from ..aethalometer import AethalometerOptical


class _AethalometerBase(PublicTimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(**kwargs)

        absorption = self.SevenWavelength("σap (Mm⁻¹)", '.2f', "σap {wavelength} nm", f'{mode}-aethalometer',
                                          'Ba{index}')
        absorption.title = "Aethalometer Absorption Coefficient"
        self.graphs.append(absorption)

        ebc = self.SevenWavelength("μg/m³", '.3f', "EBC {wavelength} nm", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        self.graphs.append(ebc)

        transmittance = self.SevenWavelength(None, '.7f', "Transmittance {wavelength} nm",
                                             f'{mode}-aethalometer', 'Ir{index}')
        transmittance.title = "Transmittance"
        self.graphs.append(transmittance)


class PublicAE33Short(_AethalometerBase):
    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(mode=mode, **kwargs)

        factor = self.SevenWavelength(None, '.6f', "Correction ({wavelength} nm)", f'{mode}-aethalometer', 'CF{index}')
        factor.title = "Loading Correction Factor"
        self.graphs.append(factor)


        temperatures = PublicTimeSeries.Graph()
        temperatures.title = "Temperature"
        self.graphs.append(temperatures)

        degrees = PublicTimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        controller = PublicTimeSeries.Trace(degrees)
        controller.legend = "Controller"
        controller.data_record = f'{mode}-aethalometer'
        controller.data_field = 'Tcontroller'
        temperatures.traces.append(controller)

        supply = PublicTimeSeries.Trace(degrees)
        supply.legend = "Supply"
        supply.data_record = f'{mode}-aethalometer'
        supply.data_field = 'Tsupply'
        temperatures.traces.append(supply)

        led = PublicTimeSeries.Trace(degrees)
        led.legend = "LED"
        led.data_record = f'{mode}-aethalometer'
        led.data_field = 'Tled'
        temperatures.traces.append(led)


        flow = PublicTimeSeries.Graph()
        flow.title = "Flow"
        self.graphs.append(flow)

        lpm = PublicTimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.3f'
        flow.axes.append(lpm)

        Q = PublicTimeSeries.Trace(lpm)
        Q.legend = "Spot 1"
        Q.data_record = f'{mode}-aethalometer'
        Q.data_field = 'Q1'
        flow.traces.append(Q)

        Q = PublicTimeSeries.Trace(lpm)
        Q.legend = "Spot 2"
        Q.data_record = f'{mode}-aethalometer'
        Q.data_field = 'Q2'
        flow.traces.append(Q)


class PublicAE33Long(PublicAE33Short):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR