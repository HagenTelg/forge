import typing
from collections import OrderedDict
from forge.vis.view.timeseries import PublicTimeSeries
from ...met.wind import Wind as BaseWind
from ...met.temperature import Temperature as BaseTemperature
from ..flow import Flow as BaseFlow


class PublicHousekeepingShort(PublicTimeSeries):
    WIND_DIRECTION_BREAK_SCRIPT = BaseWind.DIRECTION_BREAK_SCRIPT
    CalculatePitotFlow = BaseFlow.CalculatePitotFlow
    CalculateMissingTemperature = BaseTemperature.CalculateMissing

    def __init__(self, mode: str = 'public-aerosolweb', **kwargs):
        super().__init__(**kwargs)

        speed = PublicTimeSeries.Graph()
        speed.title = "Wind Speed"
        self.graphs.append(speed)
        mps = PublicTimeSeries.Axis()
        mps.title = "m/s"
        mps.range = 0
        mps.format_code = '.1f'
        speed.axes.append(mps)

        ws = PublicTimeSeries.Trace(mps)
        ws.legend = "Speed"
        ws.data_record = f'{mode}-wind'
        ws.data_field = 'WS'
        speed.traces.append(ws)

        direction = PublicTimeSeries.Graph()
        direction.title = "Wind Direction"
        self.graphs.append(direction)

        degrees = PublicTimeSeries.Axis()
        degrees.title = "degrees"
        degrees.range = [0, 360]
        degrees.ticks = [0, 90, 180, 270, 360]
        degrees.format_code = '.0f'
        direction.axes.append(degrees)

        wd = PublicTimeSeries.Trace(degrees)
        wd.legend = "Direction"
        wd.data_record = f'{mode}-wind'
        wd.data_field = 'WD'
        wd.script_incoming_data = self.WIND_DIRECTION_BREAK_SCRIPT
        direction.traces.append(wd)


        system_flow = PublicTimeSeries.Graph()
        system_flow.title = "Flow"
        self.graphs.append(system_flow)

        lpm = PublicTimeSeries.Axis()
        lpm.title = "Analyzer Flow (lpm)"
        lpm.range = [0, 50]
        lpm.format_code = '.2f'
        system_flow.axes.append(lpm)

        sample_flow = PublicTimeSeries.Trace(lpm)
        sample_flow.legend = "Analyzer"
        sample_flow.data_record = f'{mode}-flow'
        sample_flow.data_field = 'sample'
        system_flow.traces.append(sample_flow)

        stack_lpm = PublicTimeSeries.Axis()
        stack_lpm.title = "Stack Flow (lpm)"
        stack_lpm.format_code = '.1f'
        system_flow.axes.append(stack_lpm)

        stack_flow = PublicTimeSeries.Trace(stack_lpm)
        stack_flow.legend = "Stack"
        stack_flow.data_record = f'{mode}-flow'
        stack_flow.data_field = 'pitot'
        system_flow.traces.append(stack_flow)
        self.processing[stack_flow.data_record] = self.CalculatePitotFlow()


        pressure = PublicTimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hpa_ambient = PublicTimeSeries.Axis()
        hpa_ambient.title = "Absolute (hPa)"
        hpa_ambient.format_code = '.1f'
        pressure.axes.append(hpa_ambient)

        hpa_delta = PublicTimeSeries.Axis()
        hpa_delta.title = "Delta (hPa)"
        hpa_delta.range = 0
        hpa_delta.format_code = '.3f'
        pressure.axes.append(hpa_delta)

        ambient = PublicTimeSeries.Trace(hpa_ambient)
        ambient.legend = "Ambient"
        ambient.data_record = f'{mode}-pressure'
        ambient.data_field = 'ambient'
        pressure.traces.append(ambient)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            nephelometer = PublicTimeSeries.Trace(hpa_ambient)
            nephelometer.legend = f"Nephelometer ({size[0]})"
            nephelometer.data_record = f'{mode}-samplepressure-{size[1]}'
            nephelometer.data_field = 'neph'
            pressure.traces.append(nephelometer)

        pitot = PublicTimeSeries.Trace(hpa_delta)
        pitot.legend = "Pitot"
        pitot.data_record = f'{mode}-pressure'
        pitot.data_field = 'pitot'
        pressure.traces.append(pitot)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'), ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            impactor = PublicTimeSeries.Trace(hpa_delta)
            impactor.legend = f"Impactor ({size[0]})"
            impactor.data_record = f'{mode}-samplepressure-{size[1]}'
            impactor.data_field = 'impactor'
            pressure.traces.append(impactor)


        self.processing[f'{mode}-temperature'] = self.CalculateMissingTemperature()

        rh = PublicTimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)
        rh_percent = PublicTimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        temperature = PublicTimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)
        T_C = PublicTimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperature.axes.append(T_C)

        dewpoint = PublicTimeSeries.Graph()
        dewpoint.title = "Dewpoint"
        self.graphs.append(dewpoint)
        TD_C = PublicTimeSeries.Axis()
        TD_C.title = "°C"
        TD_C.format_code = '.1f'
        dewpoint.axes.append(TD_C)

        for field, legend in (
                ('{code}ambient', 'Ambient {type}'),
                ('{code}sample', 'Sample {type}'),
                ('{code}nephinlet', 'Nephelometer Inlet {type}'),
                ('{code}neph', 'Nephelometer Sample {type}')
        ):
            trace = PublicTimeSeries.Trace(rh_percent)
            trace.legend = legend.format(type='RH', code='U')
            trace.data_record = f'{mode}-temperature'
            trace.data_field = field.format(code='U')
            rh.traces.append(trace)

            trace = PublicTimeSeries.Trace(T_C)
            trace.legend = legend.format(type='Temperature', code='T')
            trace.data_record = f'{mode}-temperature'
            trace.data_field = field.format(code='T')
            temperature.traces.append(trace)

            trace = PublicTimeSeries.Trace(TD_C)
            trace.legend = legend.format(type='Dewpoint', code='TD')
            trace.data_record = f'{mode}-temperature'
            trace.data_field = field.format(code='TD')
            dewpoint.traces.append(trace)


        umac_status = PublicTimeSeries.Graph()
        umac_status.title = "μMAC/CR1000 Parameters"
        self.graphs.append(umac_status)

        T_C = PublicTimeSeries.Axis()
        T_C.title = "Temperature (°C)"
        T_C.format_code = '.1f'
        umac_status.axes.append(T_C)

        V = PublicTimeSeries.Axis()
        V.title = "Voltage"
        V.format_code = '.3f'
        umac_status.axes.append(V)

        supply = PublicTimeSeries.Trace(V)
        supply.legend = "Supply"
        supply.data_record = f'{mode}-umacstatus'
        supply.data_field = 'V'
        umac_status.traces.append(supply)

        internal = PublicTimeSeries.Trace(T_C)
        internal.legend = "Temperature"
        internal.data_record = f'{mode}-umacstatus'
        internal.data_field = 'T'
        umac_status.traces.append(internal)

    @property
    def required_components(self) -> typing.List[str]:
        return super().required_components + ['winds']


class PublicHousekeepingLong(PublicHousekeepingShort):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.average = self.Averaging.HOUR

