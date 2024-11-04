import typing
from forge.vis.view.timeseries import TimeSeries
from ..default.aerosol.temperature import Temperature
from .gasses import Gasses


class Summary(TimeSeries):
    CalculateMissing = Temperature.CalculateMissing

    THERMO_OZONE_SLOPE = 1.35135
    THERMO_OZONE_INTERCEPT = 1.29191
    ECOTECH_OZONE_SLOPE = 0.882
    ECOTECH_OZONE_INTERCEPT = -4.82
    TWOB_OZONE_SLOPE = 1
    TWOB_OZONE_INTERCEPT = 0

    class CalibrateOzone(TimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
return new GenericOperations.ApplyToFields(dataName, {
    'thermo': (value) => {
        """ + f'return GenericOperations.calibration(value, {Summary.THERMO_OZONE_INTERCEPT}, {Summary.THERMO_OZONE_SLOPE});' + r"""
    },
    
    'ecotech': (value) => {
        """ + f'return GenericOperations.calibration(value, {Summary.ECOTECH_OZONE_INTERCEPT}, {Summary.ECOTECH_OZONE_SLOPE});' + r"""
    },
    
    'twob': (value) => {
        """ + f'return GenericOperations.calibration(value, {Summary.TWOB_OZONE_INTERCEPT}, {Summary.TWOB_OZONE_SLOPE});' + r"""
    },
});
})"""

    def __init__(self, optical_mode: str = 'aerosol-editing', gas_mode: str = 'aerosol-raw', **kwargs):
        super().__init__(**kwargs)
        self.title = "Optical Properties Adjusted to 550nm"

        scattering = TimeSeries.Graph()
        scattering.title = "Light Scattering"
        scattering.contamination = f'{optical_mode}-contamination'
        self.graphs.append(scattering)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        scattering.axes.append(Mm_1)

        for size in [("Whole", 'whole', '#0f0'), ("PM10", 'pm10', '#0f0'),
                     ("PM2.5", 'pm25', '#070'), ("PM1", 'pm1', '#070')]:
            trace = TimeSeries.Trace(Mm_1)
            trace.legend = f"Scattering ({size[0]})"
            trace.data_record = f'{optical_mode}-scattering-{size[1]}'
            trace.data_field = 'BsG'
            trace.color = size[2]
            scattering.traces.append(trace)


        absorption = TimeSeries.Graph()
        absorption.title = "Light Absorption"
        absorption.contamination = f'{optical_mode}-contamination'
        self.graphs.append(absorption)

        Mm_1 = TimeSeries.Axis()
        Mm_1.title = "Mm⁻¹"
        Mm_1.format_code = '.2f'
        absorption.axes.append(Mm_1)

        for size in [("Whole", 'whole', '#0f0'), ("PM10", 'pm10', '#0f0'),
                     ("PM2.5", 'pm25', '#070'), ("PM1", 'pm1', '#070')]:
            trace = TimeSeries.Trace(Mm_1)
            trace.legend = f"Absorption ({size[0]})"
            trace.data_record = f'{optical_mode}-absorption-{size[1]}'
            trace.data_field = 'BaG'
            trace.color = size[2]
            absorption.traces.append(trace)


        cox = TimeSeries.Graph()
        cox.title = "CO and CO₂"
        cox.contamination = f'{gas_mode}-contamination'
        self.graphs.append(cox)

        co_ppb = TimeSeries.Axis()
        co_ppb.title = "CO (ppb)"
        co_ppb.format_code = '.1f'
        cox.axes.append(co_ppb)

        co2_ppm = TimeSeries.Axis()
        co2_ppm.title = "CO₂ (ppm)"
        co2_ppm.format_code = '.1f'
        cox.axes.append(co2_ppm)

        CO = TimeSeries.Trace(co_ppb)
        CO.legend = "CO"
        CO.data_record = f'{gas_mode}-gasses'
        CO.data_field = 'CO'
        cox.traces.append(CO)

        CO2 = TimeSeries.Trace(co2_ppm)
        CO2.legend = "CO₂"
        CO2.data_record = f'{gas_mode}-gasses'
        CO2.data_field = 'CO2'
        cox.traces.append(CO2)


        ozone = TimeSeries.Graph()
        ozone.title = "Ozone"
        ozone.contamination = f'{gas_mode}-contamination'
        self.graphs.append(ozone)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        ozone.axes.append(ppb)

        thermo = TimeSeries.Trace(ppb)
        thermo.legend = "Thermo"
        thermo.data_record = f'{gas_mode}-ozone'
        thermo.data_field = 'thermo'
        ozone.traces.append(thermo)

        ecotech = TimeSeries.Trace(ppb)
        ecotech.legend = "Ecotech"
        ecotech.data_record = f'{gas_mode}-ozone'
        ecotech.data_field = 'ecotech'
        ozone.traces.append(ecotech)

        twob = TimeSeries.Trace(ppb)
        twob.legend = "2B Tech"
        twob.data_record = f'{gas_mode}-ozone'
        twob.data_field = 'twob'
        ozone.traces.append(twob)

        self.processing[f'{gas_mode}-ozone'] = self.CalibrateOzone()


        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)

        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperature.axes.append(T_C)

        for parameter in [("Ambient", 'Tambient'), ("Sheltered", 'Tsheltered'),
                          ("Room 1", 'Troom'), ("Room 2", 'Troom2'), ("CR1000 Room2", 'Tcr1000')]:
            trace = TimeSeries.Trace(T_C)
            trace.legend = parameter[0]
            trace.data_record = f'{gas_mode}-temperature'
            trace.data_field = parameter[1]
            temperature.traces.append(trace)


        rh = TimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)

        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        for parameter in [("Ambient", 'Uambient'), ("Sheltered", 'Usheltered')]:
            trace = TimeSeries.Trace(rh_percent)
            trace.legend = parameter[0]
            trace.data_record = f'{gas_mode}-temperature'
            trace.data_field = parameter[1]
            rh.traces.append(trace)

        self.processing[f'{gas_mode}-temperature'] = self.CalculateMissing()
