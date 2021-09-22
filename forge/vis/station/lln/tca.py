import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.aethalometer import AethalometerOptical


class TCA08Mass(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Mass Concentration"

        mass = TimeSeries.Graph()
        mass.title = "TCA"
        mass.contamination = f'{mode}-contamination'
        self.graphs.append(mass)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        mass.axes.append(ugm3)

        tca = TimeSeries.Trace(ugm3)
        tca.legend = "TCA"
        tca.data_record = f'{mode}-tca'
        tca.data_field = 'X'
        mass.traces.append(tca)

        aethalometer = TimeSeries.Trace(ugm3)
        aethalometer.legend = "Aethalometer (880nm)"
        aethalometer.data_record = f'{mode}-aethalometer'
        aethalometer.data_field = 'X6'
        mass.traces.append(aethalometer)

        ebc = AethalometerOptical.SevenWavelength("μg/m³", '.3f', "EBC ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        ebc.contamination = f'{mode}-contamination'
        self.graphs.append(ebc)


class EditingTCA(TimeSeries):
    def __init__(self, profile: str = 'aerosol'):
        super().__init__()
        self.title = "Mass Concentration"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        raw.axes.append(ugm3)

        mass = TimeSeries.Trace(ugm3)
        mass.legend = "Raw"
        mass.data_record = f'{profile}-raw-tca'
        mass.data_field = 'X'
        raw.traces.append(mass)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        edited.axes.append(ugm3)

        mass = TimeSeries.Trace(ugm3)
        mass.legend = "Edited"
        mass.data_record = f'{profile}-editing-tca'
        mass.data_field = 'X'
        edited.traces.append(mass)


class TCA08Status(TimeSeries):
    class ScaleCO2(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
    const op = new GenericOperations.SingleOutput(dataName, GenericOperations.divide, 'CO2', 'CO2');
    op.after.push(1000.0);
    return op;
})"""

    def __init__(self, mode: str):
        super().__init__()
        self.title = "TCA Status"


        temperatures = TimeSeries.Graph()
        temperatures.title = "Chamber Temperature"
        self.graphs.append(temperatures)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperatures.axes.append(degrees)

        chamber = TimeSeries.Trace(degrees)
        chamber.legend = "Chamber 1"
        chamber.data_record = f'{mode}-tcastatus'
        chamber.data_field = 'Tchamber1'
        temperatures.traces.append(chamber)

        chamber = TimeSeries.Trace(degrees)
        chamber.legend = "Chamber 2"
        chamber.data_record = f'{mode}-tcastatus'
        chamber.data_field = 'Tchamber2'
        temperatures.traces.append(chamber)


        flow = TimeSeries.Graph()
        flow.title = "Flow"
        self.graphs.append(flow)

        lpm_sample = TimeSeries.Axis()
        lpm_sample.title = "Sample (lpm)"
        lpm_sample.format_code = '.1f'
        flow.axes.append(lpm_sample)

        lpm_analytic = TimeSeries.Axis()
        lpm_analytic.title = "Analytic (lpm)"
        lpm_analytic.format_code = '.3f'
        flow.axes.append(lpm_analytic)

        sample = TimeSeries.Trace(lpm_sample)
        sample.legend = "Sample Flow"
        sample.data_record = f'{mode}-tcastatus'
        sample.data_field = 'Qsample'
        flow.traces.append(sample)

        analytic = TimeSeries.Trace(lpm_analytic)
        analytic.legend = "Analytic Flow"
        analytic.data_record = f'{mode}-tcastatus'
        analytic.data_field = 'Qanalytic'
        flow.traces.append(analytic)


        co2 = TimeSeries.Graph()
        co2.title = "CO₂"
        co2.contamination = f'{mode}-tcastatus'
        self.graphs.append(co2)

        ppm = TimeSeries.Axis()
        ppm.title = "ppm"
        ppm.format_code = '.1f'
        co2.axes.append(ppm)

        ambient_co2 = TimeSeries.Trace(ppm)
        ambient_co2.legend = "Ambient CO₂"
        ambient_co2.data_record = f'{mode}-tcastatus'
        ambient_co2.data_field = 'CO2'
        co2.traces.append(ambient_co2)
        self.processing[ambient_co2.data_record] = self.ScaleCO2()


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        aerosol = TimeSeries.Trace(hPa)
        aerosol.legend = "LI-COR Pressure"
        aerosol.data_record = f'{mode}-tcastatus'
        aerosol.data_field = 'Plicor'
        pressure.traces.append(aerosol)


        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperature.axes.append(degrees)

        t_licor = TimeSeries.Trace(degrees)
        t_licor.legend = "LI-COR Temperature"
        t_licor.data_record = f'{mode}-tcastatus'
        t_licor.data_field = 'Tlicor'
        temperature.traces.append(t_licor)

        td_licor = TimeSeries.Trace(degrees)
        td_licor.legend = "LI-COR Dewpoint"
        td_licor.data_record = f'{mode}-tcastatus'
        td_licor.data_field = 'TDlicor'
        temperature.traces.append(td_licor)
