import typing
from forge.vis.view.timeseries import TimeSeries
from ..met.temperature import Temperature


class FanStatus(TimeSeries):
    TRACE_CONTENTS = (
        ("Global 1", "Cg1"),
        ("Global 2", "Cg2"),
        ("Diffuse", "Cf"),
        ("Infrared", "Ci"),
        ("Upwelling infrared", "Cui"),
        ("Upwelling global", "Cug"),
    )

    def __init__(self, record: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "Fan Status"

        self.processing[record] = Temperature.CalculateMissing()

        tachometers = TimeSeries.Graph()
        tachometers.title = "Tachometer RPM"
        self.graphs.append(tachometers)
        rpm = TimeSeries.Axis()
        rpm.title = "rpm"
        rpm.format_code = '.0f'
        tachometers.axes.append(rpm)

        for title, field in self.TRACE_CONTENTS:
            trace = TimeSeries.Trace(rpm)
            trace.legend = title
            trace.data_record = record
            trace.data_field = field
            tachometers.traces.append(trace)
