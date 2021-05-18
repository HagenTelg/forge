import typing
from forge.vis.view.timeseries import TimeSeries


class AethalometerOptical(TimeSeries):
    class SevenWavelength(TimeSeries.Graph):
        WAVELENGTH_NM = [370, 470, 520, 590, 660, 880, 950]

        def __init__(self, axis_title: typing.Optional[str], trace_title: str, record: str, field: str):
            super().__init__()

            axis = TimeSeries.Axis()
            axis.title = axis_title
            self.axes.append(axis)

            for wavelength in range(7):
                trace = TimeSeries.Trace(axis)
                trace.legend = trace_title.format(index=wavelength+1, wavelength=self.WAVELENGTH_NM[wavelength])
                trace.data_record = record
                trace.data_field = field.format(index=wavelength+1, wavelength=self.WAVELENGTH_NM[wavelength])
                self.traces.append(trace)

    def __init__(self, mode: str):
        super().__init__()
        self.title = "Aethalometer"

        absorption = self.SevenWavelength("Mm⁻¹", "Absorption ({wavelength} nm)", f'{mode}-aethalometer', 'Ba{index}')
        absorption.title = "Absorption Coefficient"
        self.graphs.append(absorption)

        ebc = self.SevenWavelength("μg/m³", "EBC ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        self.graphs.append(ebc)


class AE33(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str):
        super().__init__()
        self.title = "AE33"

        absorption = self.SevenWavelength("Mm⁻¹", "Ba ({wavelength} nm)", f'{mode}-aethalometer', 'Ba{index}')
        absorption.title = "Absorption Coefficient"
        self.graphs.append(absorption)

        ebc = self.SevenWavelength("μg/m³", "X ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        self.graphs.append(ebc)

        factor = self.SevenWavelength(None, "Correction ({wavelength} nm)", f'{mode}-aethalometer', 'CF{index}')
        factor.title = "Loading Correction Factor"
        self.graphs.append(factor)


class AE33Status(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str):
        super().__init__()
        self.title = "AE33 Status"

        transmittance = self.SevenWavelength(None, "Transmittance ({wavelength} nm)",
                                             f'{mode}-aethalometer', 'Ir{index}')
        transmittance.title = "Transmittance"
        self.graphs.append(transmittance)


        temperatures = TimeSeries.Graph()
        temperatures.title = "Temperature"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
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
        led.legend = "Supply"
        led.data_record = f'{mode}-aethalometerstatus'
        led.data_field = 'Tled'
        temperatures.traces.append(led)


class AE33OpticalStatus(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str):
        super().__init__()
        self.title = "AE33 Status"

        transmittance = self.SevenWavelength(None, "Transmittance ({wavelength} nm)",
                                             f'{mode}-aethalometer', 'Ir{index}')
        transmittance.title = "Transmittance"
        self.graphs.append(transmittance)

        ebc = self.SevenWavelength("μg/m³", "X ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        self.graphs.append(ebc)

        factor = self.SevenWavelength(None, "FACTOR ({wavelength} nm)", f'{mode}-aethalometer', 'CF{index}')
        factor.title = "Loading Correction Factor"
        self.graphs.append(factor)

