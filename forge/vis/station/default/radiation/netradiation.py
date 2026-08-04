import typing

from forge.vis.view.solar import SolarTimeSeries


class NetRadiation(SolarTimeSeries):
    class CalculateNetRadiation(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append("generic_operations")
            self.script = r"""(function(dataName) {
function net(direct, diffuse, up_sw, down_lw, up_lw, zsa) {
    if (!isFinite(direct) || !isFinite(diffuse) || !isFinite(up_sw) || !isFinite(down_lw) || !isFinite(up_lw) || !isFinite(zsa)) {
        return undefined;
    }
    const u0 = Math.cos(zsa * Math.PI/180.0);

    if (direct > 1500.0) {
        return undefined;
    }
    if (diffuse > 1500 * 0.95 * Math.pow(u0, 1.2) + 50) {
        return undefined;
    }
    const down_sw = direct * u0 + diffuse;
    return (down_sw - up_sw) + (down_lw - up_lw);
}

function netshortwave(direct, diffuse, up_sw, zsa) {
    if (!isFinite(direct) || !isFinite(diffuse) || !isFinite(up_sw) || !isFinite(zsa)) {
        return undefined;
    }
    const u0 = Math.cos(zsa * Math.PI/180.0);

    if (direct > 1500.0) {
        return undefined;
    }
    if (diffuse > 1500 * 0.95 * Math.pow(u0, 1.2) + 50) {
        return undefined;
    }
    const down_sw = direct * u0 + diffuse;
    return down_sw - up_sw;
}

function netlongwave(down_lw, up_lw) {
    if (!isFinite(down_lw) || !isFinite(up_lw)) {
        return undefined;
    }
    return down_lw - up_lw;
}

    return new GenericOperations.ApplyToFields(dataName, {
        'net': (output, direct, diffuse, up_sw, down_lw, up_lw, zsa) => net(direct, diffuse, up_sw, down_lw, up_lw, zsa),
        'netshortwave': (output, direct, diffuse, up_sw, down_lw, up_lw, zsa) => netshortwave(direct, diffuse, up_sw, zsa),
        'netlongwave': (output, direct, diffuse, up_sw, down_lw, up_lw, zsa) => netlongwave(down_lw, up_lw),
    }, 'direct', 'diffuse', 'up_sw', 'down_lw', 'up_lw', 'zsa');
})"""

    def __init__(
        self,
        mode: str,
        latitude: typing.Optional[float] = None,
        longitude: typing.Optional[float] = None,
        **kwargs,
    ):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Net Radiation"

        net_graph = SolarTimeSeries.Graph()
        net_graph.title = "Net Radiation"
        net_graph.contamination = f"{mode}-contamination"
        self.graphs.append(net_graph)

        net_axis = SolarTimeSeries.Axis()
        net_axis.title = "W/m²"
        net_axis.format_code = ".1f"
        net_graph.axes.append(net_axis)

        trace = SolarTimeSeries.Trace(net_axis)
        trace.legend = "Net Radiation"
        trace.data_record = f"{mode}-netradiation"
        trace.data_field = "net"
        net_graph.traces.append(trace)

        trace = SolarTimeSeries.Trace(net_axis)
        trace.legend = "Net Shortwave"
        trace.data_record = f"{mode}-netradiation"
        trace.data_field = "netshortwave"
        net_graph.traces.append(trace)

        trace = SolarTimeSeries.Trace(net_axis)
        trace.legend = "Net Longwave"
        trace.data_record = f"{mode}-netradiation"
        trace.data_field = "netlongwave"
        net_graph.traces.append(trace)

        self.processing[f"{mode}-netradiation"] = self.CalculateNetRadiation()
