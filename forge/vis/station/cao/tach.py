import typing
from forge.vis.view.timeseries import TimeSeries


class Tachometer(TimeSeries):
    FAN_TACHOMETERS = (
        ("30m", "C1"),
        ("100m", "C2"),
        ("495m", "C3"),
    )

    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)

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
            trace.data_record = f'{mode}-tach'
            trace.data_field = field
            tachometers.traces.append(trace)


class EditingTachometer(TimeSeries):
    FAN_TACHOMETERS = Tachometer.FAN_TACHOMETERS

    def __init__(self, profile: str = 'met', **kwargs):
        super().__init__(**kwargs)
        self.title = "Fan Tachometers"

        raw = TimeSeries.Graph()
        raw.title = "Raw"
        self.graphs.append(raw)

        rpm = TimeSeries.Axis()
        rpm.title = "rpm"
        rpm.format_code = '.0f'
        raw.axes.append(rpm)

        for title, field in self.FAN_TACHOMETERS:
            trace = TimeSeries.Trace(rpm)
            trace.legend = title
            trace.data_record = f'{profile}-raw-tach'
            trace.data_field = field
            raw.traces.append(trace)


        edited = TimeSeries.Graph()
        edited.title = "Edited"
        self.graphs.append(edited)

        rpm = TimeSeries.Axis()
        rpm.title = "rpm"
        rpm.format_code = '.0f'
        edited.axes.append(rpm)

        for title, field in self.FAN_TACHOMETERS:
            trace = TimeSeries.Trace(rpm)
            trace.legend = title
            trace.data_record = f'{profile}-edited-tach'
            trace.data_field = field
            edited.traces.append(trace)
