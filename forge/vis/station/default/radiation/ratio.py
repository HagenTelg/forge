import typing
from forge.vis.view.solar import SolarTimeSeries


class Ratios(SolarTimeSeries):
    class CalculateDirectDiffuseGlobal(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
    function calc(direct, diffuse, global, zsa) {
        if (!isFinite(direct) || !isFinite(diffuse) || !isFinite(global) || !isFinite(zsa)) {
            return undefined;
        }
        if (zsa >= 93.0) {
            return undefined;
        }
        const u0 = Math.cos(zsa * Math.PI/180.0);
        if (direct <= 0.01 || direct > 1500.0) {
            return undefined;
        }
        if (diffuse <= 0.01 || diffuse > 1500 * 0.95 * u0 ** 1.2 + 50) {
            return undefined;
        }
        if (global <= 0.01 || global > 1500 * 1.5 * u0 ** 1.2 + 100) {
            return undefined;
        }
        const total = direct * u0 + diffuse;
        if (total < 50.0) {
            return undefined;
        }
        return total / global;
    }
        return new GenericOperations.SingleOutput(dataName, calc, 'ratio', 'direct', 'diffuse', 'global', 'zsa');
    })"""

    class CalculateTotalGlobal(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
function calc(total, global, zsa) {
    if (!isFinite(total) || !isFinite(global) || !isFinite(zsa)) {
        return undefined;
    }
    if (zsa >= 93.0) {
        return undefined;
    }
    const u0 = Math.cos(zsa * Math.PI/180.0);
    const upper_limit = 1500 * 1.5 * u0 ** 1.2 + 100;
    if (total <= 50.0 || total > upper_limit) {
        return undefined;
    }
    if (global <= 0.01 || global > upper_limit) {
        return undefined;
    }
    return total / global;
}
    return new GenericOperations.SingleOutput(dataName, calc, 'ratio', 'total', 'global', 'zsa');
})"""

    class CalculateAlbedo(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
function calc(up, down, zsa) {
    if (!isFinite(up) || !isFinite(down) || !isFinite(zsa)) {
        return undefined;
    }
    if (zsa >= 75.0) {
        return undefined;
    } 
    const u0 = Math.cos(zsa * Math.PI/180.0);    
    if (up <= 0.0 || up > 1500 * 1.5 * u0 ** 1.2 + 50) {
        return undefined;
    }
    if (down < 100.0 || down > 1500 * 1.5 * u0 ** 1.2 + 100) {
        return undefined;
    }    
    return up / down;
}
    return new GenericOperations.SingleOutput(dataName, calc, 'albedo', 'up', 'down', 'zsa');
})"""

    class CalculateDiffuseGlobal(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
function calc(diffuse, global, zsa) {
    if (!isFinite(diffuse) || !isFinite(global) || !isFinite(zsa)) {
        return undefined;
    }
    const u0 = Math.cos(zsa * Math.PI/180.0);
    if (diffuse <= 0.01 || diffuse > 1500 * 0.95 * u0 ** 1.2 + 50) {
        return undefined;
    }
    if (global <= 0.01 || global > 1500 * 1.5 * u0 ** 1.2 + 100) {
        return undefined;
    }
    return diffuse / global;
}
    return new GenericOperations.SingleOutput(dataName, calc, 'ratio', 'diffuse', 'global', 'zsa');
})"""

    class CalculatePIRTemperature(SolarTimeSeries.Processing):
        def __init__(self):
            super().__init__()
            self.components.append('generic_operations')
            self.script = r"""(function(dataName) {
function calc(pir, temperature) {
    if (!isFinite(pir) || !isFinite(temperature)) {
        return undefined;
    }
    if (pir < 40.0 || pir > 900.0) {
        return undefined;
    }
    if (temperature < -100.0 || temperature > 150.0) {
        return undefined;
    }
    temperature += 273.15;
    return pir / temperature;
}
    return new GenericOperations.SingleOutput(dataName, calc, 'ratio', 'pir', 'temperature');
})"""

    def __init__(self, mode: str, latitude: typing.Optional[float] = None, longitude: typing.Optional[float] = None,
                 **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Ratios"

        total_global = SolarTimeSeries.Graph()
        total_global.title = "Total/Global"
        total_global.contamination = f'{mode}-contamination'
        self.graphs.append(total_global)

        ratio = SolarTimeSeries.Axis()
        ratio.format_code = '.3f'
        ratio.range = [0.0, 5.0]
        total_global.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "Direct+Diffuse / Global"
        trace.data_record = f'{mode}-totalratio'
        trace.data_field = 'ratio'
        total_global.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateDirectDiffuseGlobal()

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "SPN1 Total/Global"
        trace.data_record = f'{mode}-spn1ratio'
        trace.data_field = 'ratio'
        total_global.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateTotalGlobal()


        albedo = SolarTimeSeries.Graph()
        albedo.title = "Albedo"
        albedo.contamination = f'{mode}-contamination'
        self.graphs.append(albedo)

        ratio = SolarTimeSeries.Axis()
        ratio.format_code = '.3f'
        ratio.range = [0.0, 1.5]
        albedo.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "Upwelling/Downwelling"
        trace.data_record = f'{mode}-albedo'
        trace.data_field = 'albedo'
        albedo.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateAlbedo()


        diffuse_global = SolarTimeSeries.Graph()
        diffuse_global.title = "Diffuse/Global"
        diffuse_global.contamination = f'{mode}-contamination'
        self.graphs.append(diffuse_global)

        ratio = SolarTimeSeries.Axis()
        ratio.format_code = '.3f'
        ratio.range = [0.0, 5.0]
        diffuse_global.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "Diffuse / Global"
        trace.data_record = f'{mode}-diffuseratio'
        trace.data_field = 'ratio'
        diffuse_global.traces.append(trace)
        self.processing[trace.data_record] = self.CalculateDiffuseGlobal()


        pir_temperature = SolarTimeSeries.Graph()
        pir_temperature.title = "Downwelling PIR/Air Temperature"
        pir_temperature.contamination = f'{mode}-contamination'
        self.graphs.append(pir_temperature)

        ratio = SolarTimeSeries.Axis()
        ratio.title = "W/m² / K"
        ratio.format_code = '.3f'
        pir_temperature.axes.append(ratio)

        trace = SolarTimeSeries.Trace(ratio)
        trace.legend = "Longwave/Temperature"
        trace.data_record = f'{mode}-pirdownratio'
        trace.data_field = 'ratio'
        pir_temperature.traces.append(trace)
        self.processing[trace.data_record] = self.CalculatePIRTemperature()


