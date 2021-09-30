import typing
from forge.vis.view.timeseries import TimeSeries


class MAAP5012Optical(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "MAAP"

        mass = TimeSeries.Graph()
        mass.title = "Equivalent Black Carbon"
        mass.contamination = f'{mode}-contamination'
        self.graphs.append(mass)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        mass.axes.append(ugm3)

        maap = TimeSeries.Trace(ugm3)
        maap.legend = "MAAP EBC"
        maap.data_record = f'{mode}-maap'
        maap.data_field = 'X'
        mass.traces.append(maap)

        aethalometer = TimeSeries.Trace(ugm3)
        aethalometer.legend = "Aethalometer (660 nm)"
        aethalometer.data_record = f'{mode}-aethalometer'
        aethalometer.data_field = 'X5'
        mass.traces.append(aethalometer)


        absorption = TimeSeries.Graph()
        absorption.title = "Light Absorption"
        absorption.contamination = f'{mode}-contamination'
        self.graphs.append(absorption)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        absorption.axes.append(Mm_1)

        maap = TimeSeries.Trace(Mm_1)
        maap.legend = "MAAP Absorption"
        maap.data_record = f'{mode}-maap'
        maap.data_field = 'Ba'
        absorption.traces.append(maap)

        aethalometer = TimeSeries.Trace(Mm_1)
        aethalometer.legend = "Aethalometer (660 nm)"
        aethalometer.data_record = f'{mode}-aethalometer'
        aethalometer.data_field = 'Ba5'
        absorption.traces.append(aethalometer)


class MAAP5012Status(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "MAAP Status"

        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        sample = TimeSeries.Trace(hPa)
        sample.legend = "Sample Pressure"
        sample.data_record = f'{mode}-maapstatus'
        sample.data_field = 'Psample'
        pressure.traces.append(sample)


        temperature_flow = TimeSeries.Graph()
        temperature_flow.title = "Temperature and Flow"
        self.graphs.append(temperature_flow)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperature_flow.axes.append(degrees)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.1f'
        temperature_flow.axes.append(lpm)

        ambient = TimeSeries.Trace(degrees)
        ambient.legend = "Ambient Temperature"
        ambient.data_record = f'{mode}-maapstatus'
        ambient.data_field = 'Tambient'
        temperature_flow.traces.append(ambient)

        measurement = TimeSeries.Trace(degrees)
        measurement.legend = "Measurement Head Temperature"
        measurement.data_record = f'{mode}-maapstatus'
        measurement.data_field = 'Tmeasurementhead'
        temperature_flow.traces.append(measurement)

        system = TimeSeries.Trace(degrees)
        system.legend = "System Temperature"
        system.data_record = f'{mode}-maapstatus'
        system.data_field = 'Tsystem'
        temperature_flow.traces.append(system)

        sample = TimeSeries.Trace(lpm)
        sample.legend = "Sample Flow"
        sample.data_record = f'{mode}-maapstatus'
        sample.data_field = 'Qsample'
        temperature_flow.traces.append(sample)


        intensities = TimeSeries.Graph()
        intensities.title = "Intensities"
        self.graphs.append(intensities)

        intensity_nounit = TimeSeries.Axis()
        intensity_nounit.title = "Intensity"
        intensity_nounit.format_code = '.0f'
        intensities.axes.append(intensity_nounit)

        transmittance_nounit = TimeSeries.Axis()
        transmittance_nounit.title = "Transmittance"
        transmittance_nounit.format_code = '.7f'
        intensities.axes.append(transmittance_nounit)

        transmittance = TimeSeries.Trace(transmittance_nounit)
        transmittance.legend = "Transmittance"
        transmittance.data_record = f'{mode}-maapstatus'
        transmittance.data_field = 'Ir'
        intensities.traces.append(transmittance)

        reference = TimeSeries.Trace(intensity_nounit)
        reference.legend = "Reference Intensity"
        reference.data_record = f'{mode}-maapstatus'
        reference.data_field = 'If'
        intensities.traces.append(reference)

        forward = TimeSeries.Trace(intensity_nounit)
        forward.legend = "Forward Intensity"
        forward.data_record = f'{mode}-maapstatus'
        forward.data_field = 'Ip'
        intensities.traces.append(forward)

        back135 = TimeSeries.Trace(intensity_nounit)
        back135.legend = "135° Backscatter Intensity"
        back135.data_record = f'{mode}-maapstatus'
        back135.data_field = 'Is1'
        intensities.traces.append(back135)

        back165 = TimeSeries.Trace(intensity_nounit)
        back165.legend = "165° Backscatter Intensity"
        back165.data_record = f'{mode}-maapstatus'
        back165.data_field = 'Is2'
        intensities.traces.append(back165)
