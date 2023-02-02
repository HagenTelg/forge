import typing
from forge.vis.view.solar import SolarTimeSeries


class EditingAlbedo(SolarTimeSeries):
    class CalculateAlbedo(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
function calc(up, down, zsa) {
    if (!isFinite(up) || !isFinite(down)) {
        return undefined;
    }
    if (up <= 0.0) {
        return undefined;
    }
    if (down < 100.0) {
        return undefined;
    }
    if (isFinite(zsa) && zsa >= 75.0) {
        return undefined;
    }    
    return up / down;
}
    return new GenericOperations.SingleOutput(dataName, calc, 'albedo', 'up', 'down', 'zsa');
})"""

    def __init__(self, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 profile: str = 'radiation', **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Albedo"

        raw = SolarTimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        albedo = SolarTimeSeries.Axis()
        albedo.title = "Upwelling/Downwelling"
        albedo.format_code = '.3f'
        raw.axes.append(albedo)

        trace = SolarTimeSeries.Trace(albedo)
        trace.legend = f"Raw"
        trace.data_record = f'{profile}-raw-albedo'
        trace.data_field = 'albedo'
        raw.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateAlbedo()

        edited = SolarTimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        albedo = SolarTimeSeries.Axis()
        albedo.title = "Upwelling/Downwelling"
        albedo.format_code = '.3f'
        edited.axes.append(albedo)

        trace = SolarTimeSeries.Trace(albedo)
        trace.legend = f"Edited"
        trace.data_record = f'{profile}-editing-albedo'
        trace.data_field = 'albedo'
        edited.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateAlbedo()
