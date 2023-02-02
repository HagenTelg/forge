import typing
from forge.vis.view.solar import SolarTimeSeries


class EditingTotalRatio(SolarTimeSeries):
    class CalculateTotal(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
function calc(total, global) {
    if (!isFinite(total) || !isFinite(global)) {
        return undefined;
    }
    if (total <= 0.01) {
        return undefined;
    }
    if (global <= 0.01) {
        return undefined;
    }
    return total / global;
}
    return new GenericOperations.SingleOutput(dataName, calc, 'ratio', 'total', 'global');
})"""

    class CalculateDirectDiffuse(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
function calc(direct, diffuse, global, zsa) {
    if (!isFinite(direct) || !isFinite(diffuse) || !isFinite(global) || !isFinite(zsa)) {
        return undefined;
    }
    if (direct <= 0.01) {
        return undefined;
    }
    if (diffuse <= 0.01) {
        return undefined;
    }
    if (global <= 0.01) {
        return undefined;
    }
    return (direct * Math.cos(zsa * Math.PI/180.0) + diffuse) / global;
}
    return new GenericOperations.SingleOutput(dataName, calc, 'ratio', 'direct', 'diffuse', 'global', 'zsa');
})"""

    def __init__(self, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 profile: str = 'radiation', **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Infrared"

        raw = SolarTimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ratio = SolarTimeSeries.Axis()
        ratio.title = "Total/Global"
        ratio.format_code = '.3f'
        ratio.range = [0.0, 5.0]
        raw.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = f"Raw"
        trace.data_record = f'{profile}-raw-totalratio'
        trace.data_field = 'ratio'
        raw.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateDirectDiffuse()

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = f"Raw SPN1"
        trace.data_record = f'{profile}-raw-spn1ratio'
        trace.data_field = 'ratio'
        raw.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateTotal()

        edited = SolarTimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ratio = SolarTimeSeries.Axis()
        ratio.title = "Total/Global"
        ratio.format_code = '.3f'
        ratio.range = [0.0, 5.0]
        edited.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = f"Edited"
        trace.data_record = f'{profile}-editing-totalratio'
        trace.data_field = 'ratio'
        edited.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateDirectDiffuse()

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = f"Edited SPN1"
        trace.data_record = f'{profile}-editing-spn1ratio'
        trace.data_field = 'ratio'
        edited.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateTotal()
