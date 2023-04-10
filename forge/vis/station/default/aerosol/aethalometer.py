import typing
from forge.vis.view.timeseries import TimeSeries


class AethalometerOptical(TimeSeries):
    class SevenWavelength(TimeSeries.Graph):
        WAVELENGTH_NM = [370, 470, 520, 590, 660, 880, 950]

        def __init__(self, axis_title: typing.Optional[str], axis_format: typing.Optional[str], trace_title: str, record: str, field: str):
            super().__init__()

            axis = TimeSeries.Axis()
            axis.title = axis_title
            axis.format_code = axis_format
            self.axes.append(axis)

            for wavelength in range(7):
                trace = TimeSeries.Trace(axis)
                trace.legend = trace_title.format(index=wavelength+1, wavelength=self.WAVELENGTH_NM[wavelength])
                trace.data_record = record
                trace.data_field = field.format(index=wavelength+1, wavelength=self.WAVELENGTH_NM[wavelength])
                self.traces.append(trace)

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Aethalometer"

        absorption = self.SevenWavelength("Mm⁻¹", '.2f', "Absorption ({wavelength} nm)", f'{mode}-aethalometer', 'Ba{index}')
        absorption.title = "Absorption Coefficient"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        ebc = self.SevenWavelength("μg/m³", '.3f', "EBC ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        ebc.contamination = f'{mode}-contamination'
        self.graphs.append(ebc)


class AE33(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "AE33"

        absorption = self.SevenWavelength("Mm⁻¹", '.2f', "Ba ({wavelength} nm)", f'{mode}-aethalometer', 'Ba{index}')
        absorption.title = "Absorption Coefficient"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        ebc = self.SevenWavelength("μg/m³", '.2f', "X ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        ebc.contamination = f'{mode}-contamination'
        self.graphs.append(ebc)

        factor = self.SevenWavelength(None, '.6f', "Correction ({wavelength} nm)", f'{mode}-aethalometer', 'CF{index}')
        factor.title = "Loading Correction Factor"
        self.graphs.append(factor)


class AE33Status(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "AE33 Status"

        transmittance = self.SevenWavelength(None, '.7f', "Transmittance ({wavelength} nm)",
                                             f'{mode}-aethalometer', 'Ir{index}')
        transmittance.title = "Transmittance"
        self.graphs.append(transmittance)


        temperatures = TimeSeries.Graph()
        temperatures.title = "Temperature"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        controller = TimeSeries.Trace(degrees)
        controller.legend = "Controller"
        controller.data_record = f'{mode}-aethalometerstatus'
        controller.data_field = 'Tcontroller'
        temperatures.traces.append(controller)

        supply = TimeSeries.Trace(degrees)
        supply.legend = "Supply"
        supply.data_record = f'{mode}-aethalometerstatus'
        supply.data_field = 'Tsupply'
        temperatures.traces.append(supply)

        led = TimeSeries.Trace(degrees)
        led.legend = "LED"
        led.data_record = f'{mode}-aethalometerstatus'
        led.data_field = 'Tled'
        temperatures.traces.append(led)


        flow = TimeSeries.Graph()
        flow.title = "Flow"
        self.graphs.append(flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.3f'
        flow.axes.append(lpm)

        Q = TimeSeries.Trace(lpm)
        Q.legend = "Spot 1"
        Q.data_record = f'{mode}-aethalometerstatus'
        Q.data_field = 'Q1'
        flow.traces.append(Q)

        Q = TimeSeries.Trace(lpm)
        Q.legend = "Spot 2"
        Q.data_record = f'{mode}-aethalometerstatus'
        Q.data_field = 'Q2'
        flow.traces.append(Q)


class AE31(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "AE31"

        absorption = self.SevenWavelength("Mm⁻¹", '.2f', "Ba ({wavelength} nm)", f'{mode}-aethalometer', 'Ba{index}')
        absorption.title = "Absorption Coefficient"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        ebc = self.SevenWavelength("μg/m³", '.2f', "X ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        ebc.contamination = f'{mode}-contamination'
        self.graphs.append(ebc)


class AE31Status(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "AE31 Status"

        transmittance = self.SevenWavelength(None, '.7f', "Transmittance ({wavelength} nm)",
                                             f'{mode}-aethalometer', 'Ir{index}')
        transmittance.title = "Transmittance"
        self.graphs.append(transmittance)

        flow = TimeSeries.Graph()
        flow.title = "Flow"
        self.graphs.append(flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.3f'
        flow.axes.append(lpm)

        Q = TimeSeries.Trace(lpm)
        Q.legend = "Flow"
        Q.data_record = f'{mode}-aethalometerstatus'
        Q.data_field = 'Q'
        flow.traces.append(Q)

