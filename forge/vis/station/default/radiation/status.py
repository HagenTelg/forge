import typing
from forge.vis.view.timeseries import TimeSeries


class Status(TimeSeries):
    FAN_TACHOMETERS = (
        ("Global 1", "Cg1"),
        ("Global 2", "Cg2"),
        ("Diffuse", "Cf"),
        ("Infrared", "Ci"),
        ("Upwelling infrared", "Cui"),
        ("Upwelling global", "Cug"),
    )

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Status"

        tachometers = TimeSeries.Graph()
        tachometers.title = "Tachometer RPM"
        self.graphs.append(tachometers)
        rpm = TimeSeries.Axis()
        rpm.title = "rpm"
        rpm.format_code = '.0f'
        tachometers.axes.append(rpm)

        for title, field in self.FAN_TACHOMETERS:
            trace = TimeSeries.Trace(rpm)
            trace.legend = title
            trace.data_record = f'{mode}-status'
            trace.data_field = field
            tachometers.traces.append(trace)


        temperature = TimeSeries.Graph()
        temperature.title = "Temperature"
        self.graphs.append(temperature)
        T_C = TimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        temperature.axes.append(T_C)

        trace = TimeSeries.Trace(T_C)
        trace.legend = "Logger Temperature"
        trace.data_record = f'{mode}-status'
        trace.data_field = 'Tlogger'
        temperature.traces.append(trace)


        voltage = TimeSeries.Graph()
        voltage.title = "Battery Voltage"
        self.graphs.append(voltage)

        V = TimeSeries.Axis()
        V.title = "V"
        V.format_code = '.3f'
        voltage.axes.append(V)

        supply = TimeSeries.Trace(V)
        supply.legend = "Battery Voltage"
        supply.data_record = f'{mode}-status'
        supply.data_field = 'Vbattery'
        voltage.traces.append(supply)
