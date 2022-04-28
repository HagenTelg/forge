import typing
from forge.vis.view.timeseries import TimeSeries
from .aethalometer import AethalometerOptical


class T640Mass(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "T640"

        mass = TimeSeries.Graph()
        mass.title = "Mass Concentration"
        mass.contamination = f'{mode}-contamination'
        self.graphs.append(mass)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.2f'
        ugm3.range = 0
        mass.axes.append(ugm3)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'),
                     ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            trace = TimeSeries.Trace(ugm3)
            trace.legend = f"{size[0]}"
            trace.data_record = f'{mode}-t640-{size[1]}'
            trace.data_field = 'X'
            mass.traces.append(trace)


class T640MassAethalometer(TimeSeries):
    SevenWavelength = AethalometerOptical.SevenWavelength

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "T640"

        mass = TimeSeries.Graph()
        mass.title = "Mass Concentration"
        mass.contamination = f'{mode}-contamination'
        self.graphs.append(mass)

        ugm3 = TimeSeries.Axis()
        ugm3.title = "μg/m³"
        ugm3.format_code = '.3f'
        ugm3.range = 0
        mass.axes.append(ugm3)

        for size in [("Whole", 'whole'), ("PM10", 'pm10'),
                     ("PM2.5", 'pm25'), ("PM1", 'pm1')]:
            trace = TimeSeries.Trace(ugm3)
            trace.legend = f"{size[0]}"
            trace.data_record = f'{mode}-t640-{size[1]}'
            trace.data_field = 'X'
            mass.traces.append(trace)

        ebc = self.SevenWavelength("μg/m³", '.3f', "EBC ({wavelength} nm)", f'{mode}-aethalometer', 'X{index}')
        ebc.title = "Equivalent Black Carbon"
        ebc.contamination = f'{mode}-contamination'
        ebc.axes[0].range = 0
        self.graphs.append(ebc)


class T640Status(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "T640 Status"

        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)

        degrees = TimeSeries.Axis()
        degrees.title = "°C"
        degrees.format_code = '.1f'
        temperature.axes.append(degrees)

        sample = TimeSeries.Trace(degrees)
        sample.legend = "Sample Temperature"
        sample.data_record = f'{mode}-t640status'
        sample.data_field = 'Tsample'
        temperature.traces.append(sample)

        ambient = TimeSeries.Trace(degrees)
        ambient.legend = "Ambient Temperature"
        ambient.data_record = f'{mode}-t640status'
        ambient.data_field = 'Tambient'
        temperature.traces.append(ambient)

        asc = TimeSeries.Trace(degrees)
        asc.legend = "ASC Tube Temperature"
        asc.data_record = f'{mode}-t640status'
        asc.data_field = 'Tasc'
        temperature.traces.append(asc)

        led = TimeSeries.Trace(degrees)
        led.legend = "LED Temperature"
        led.data_record = f'{mode}-t640status'
        led.data_field = 'Tled'
        temperature.traces.append(led)

        box = TimeSeries.Trace(degrees)
        box.legend = "Box Temperature"
        box.data_record = f'{mode}-t640status'
        box.data_field = 'Tbox'
        temperature.traces.append(box)


        rh = TimeSeries.Graph()
        rh.title = "Relative Humidity"
        self.graphs.append(rh)

        rh_percent = TimeSeries.Axis()
        rh_percent.title = "%"
        rh_percent.format_code = '.1f'
        rh.axes.append(rh_percent)

        inlet = TimeSeries.Trace(rh_percent)
        inlet.legend = "Inlet Humidity"
        inlet.data_record = f'{mode}-t640status'
        inlet.data_field = 'Usample'
        rh.traces.append(inlet)


        pressure = TimeSeries.Graph()
        pressure.title = "Pressure"
        self.graphs.append(pressure)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        pressure.axes.append(hPa)

        sample = TimeSeries.Trace(hPa)
        sample.legend = "Sample Pressure"
        sample.data_record = f'{mode}-t640status'
        sample.data_field = 'Psample'
        pressure.traces.append(sample)


        flow = TimeSeries.Graph()
        flow.title = "Flow"
        self.graphs.append(flow)

        lpm = TimeSeries.Axis()
        lpm.title = "lpm"
        lpm.format_code = '.1f'
        flow.axes.append(lpm)

        sample = TimeSeries.Trace(lpm)
        sample.legend = "Sample Flow"
        sample.data_record = f'{mode}-t640status'
        sample.data_field = 'Qsample'
        flow.traces.append(sample)

        bypass = TimeSeries.Trace(lpm)
        bypass.legend = "Bypass Flow"
        bypass.data_record = f'{mode}-t640status'
        bypass.data_field = 'Qbypass'
        flow.traces.append(bypass)
