import typing
from forge.vis.view.solar import SolarTimeSeries
from . import Site


class EditingSolar(SolarTimeSeries):
    _RAW_FORMAT = '{site.name} Raw {parameter}'
    _EDITED_FORMAT = '{site.name} Edited {parameter} ({code}_{site.instrument_code})'

    def __init__(self, latitude: float, longitude: float, profile: str, sites: typing.List[Site], **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Solar"

        raw = SolarTimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        wm2 = SolarTimeSeries.Axis()
        wm2.title = "W/m²"
        wm2.format_code = '.1f'
        raw.axes.append(wm2)

        for site in sites:
            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Downwelling Solar", code='Rdg')
            trace.data_record = f'{profile}-raw-solar'
            trace.data_field = f'Rdg_{site.instrument_code}'
            raw.traces.append(trace)

            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Upwelling Solar", code='Rug')
            trace.data_record = f'{profile}-raw-solar'
            trace.data_field = f'Rug_{site.instrument_code}'
            raw.traces.append(trace)

            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Direct Normal", code='Rdn')
            trace.data_record = f'{profile}-raw-solar'
            trace.data_field = f'Rdn_{site.instrument_code}'
            raw.traces.append(trace)

            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Diffuse", code='Rdf')
            trace.data_record = f'{profile}-raw-solar'
            trace.data_field = f'Rdf_{site.instrument_code}'
            raw.traces.append(trace)

            if site.include_spn1:
                trace = SolarTimeSeries.Trace(wm2)
                trace.legend = self._RAW_FORMAT.format(site=site, parameter="SPN1 Total", code='Rst')
                trace.data_record = f'{profile}-raw-solar'
                trace.data_field = f'Rst_{site.instrument_code}'
                raw.traces.append(trace)
                
                trace = SolarTimeSeries.Trace(wm2)
                trace.legend = self._RAW_FORMAT.format(site=site, parameter="SPN1 Diffuse", code='Rsd')
                trace.data_record = f'{profile}-raw-solar'
                trace.data_field = f'Rsd_{site.instrument_code}'
                raw.traces.append(trace)


        edited = SolarTimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        wm2 = SolarTimeSeries.Axis()
        wm2.title = "W/m²"
        wm2.format_code = '.1f'
        edited.axes.append(wm2)

        for site in sites:
            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Downwelling Solar", code='Rdg')
            trace.data_record = f'{profile}-editing-solar'
            trace.data_field = f'Rdg_{site.instrument_code}'
            edited.traces.append(trace)

            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Upwelling Solar", code='Rug')
            trace.data_record = f'{profile}-editing-solar'
            trace.data_field = f'Rug_{site.instrument_code}'
            edited.traces.append(trace)

            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Direct Normal", code='Rdn')
            trace.data_record = f'{profile}-editing-solar'
            trace.data_field = f'Rdn_{site.instrument_code}'
            edited.traces.append(trace)

            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Diffuse", code='Rdf')
            trace.data_record = f'{profile}-editing-solar'
            trace.data_field = f'Rdf_{site.instrument_code}'
            edited.traces.append(trace)

            if site.include_spn1:
                trace = SolarTimeSeries.Trace(wm2)
                trace.legend = self._EDITED_FORMAT.format(site=site, parameter="SPN1 Total", code='Rst')
                trace.data_record = f'{profile}-editing-solar'
                trace.data_field = f'Rst_{site.instrument_code}'
                edited.traces.append(trace)

                trace = SolarTimeSeries.Trace(wm2)
                trace.legend = self._EDITED_FORMAT.format(site=site, parameter="SPN1 Diffuse", code='Rsd')
                trace.data_record = f'{profile}-editing-solar'
                trace.data_field = f'Rsd_{site.instrument_code}'
                edited.traces.append(trace)


class EditingIR(SolarTimeSeries):
    _RAW_FORMAT = '{site.name} Raw {parameter}'
    _EDITED_FORMAT = '{site.name} Edited {parameter} ({code}_{site.instrument_code})'

    def __init__(self, latitude: float, longitude: float, profile: str, sites: typing.List[Site], **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Infrared"

        raw = SolarTimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        wm2 = SolarTimeSeries.Axis()
        wm2.title = "W/m²"
        wm2.format_code = '.1f'
        raw.axes.append(wm2)

        for site in sites:
            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Downwelling", code='Rdi')
            trace.data_record = f'{profile}-raw-ir'
            trace.data_field = f'Rdi_{site.instrument_code}'
            raw.traces.append(trace)

            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Upwelling", code='Rui')
            trace.data_record = f'{profile}-raw-ir'
            trace.data_field = f'Rui_{site.instrument_code}'
            raw.traces.append(trace)


        edited = SolarTimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        wm2 = SolarTimeSeries.Axis()
        wm2.title = "W/m²"
        wm2.format_code = '.1f'
        edited.axes.append(wm2)

        for site in sites:
            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Downwelling", code='Rdi')
            trace.data_record = f'{profile}-editing-ir'
            trace.data_field = f'Rdi_{site.instrument_code}'
            edited.traces.append(trace)

            trace = SolarTimeSeries.Trace(wm2)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Upwelling", code='Rui')
            trace.data_record = f'{profile}-editing-ir'
            trace.data_field = f'Rui_{site.instrument_code}'
            edited.traces.append(trace)


class EditingPyranometerTemperature(SolarTimeSeries):
    _RAW_FORMAT = '{site.name} Raw {parameter}'
    _EDITED_FORMAT = '{site.name} Edited {parameter} ({code}_{site.instrument_code})'

    def __init__(self, latitude: float, longitude: float, profile: str, sites: typing.List[Site], **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Pyranometer Temperature"

        raw = SolarTimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        T_C = SolarTimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        raw.axes.append(T_C)

        for site in sites:
            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Downwelling PIR Case", code='Tdic')
            trace.data_record = f'{profile}-raw-pyranometertemperature'
            trace.data_field = f'Tdic_{site.instrument_code}'
            raw.traces.append(trace)

            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Downwelling PIR Dome", code='Tdid')
            trace.data_record = f'{profile}-raw-pyranometertemperature'
            trace.data_field = f'Tdid_{site.instrument_code}'
            raw.traces.append(trace)

            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Upwelling PIR Case", code='Tuic')
            trace.data_record = f'{profile}-raw-pyranometertemperature'
            trace.data_field = f'Tdic_{site.instrument_code}'
            raw.traces.append(trace)

            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Upwelling PIR Dome", code='Tuid')
            trace.data_record = f'{profile}-raw-pyranometertemperature'
            trace.data_field = f'Tdid_{site.instrument_code}'
            raw.traces.append(trace)

            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._RAW_FORMAT.format(site=site, parameter="Air Temperature", code='T')
            trace.data_record = f'{profile}-raw-pyranometertemperature'
            trace.data_field = f'T_{site.instrument_code}'
            raw.traces.append(trace)


        edited = SolarTimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        T_C = SolarTimeSeries.Axis()
        T_C.title = "°C"
        T_C.format_code = '.1f'
        edited.axes.append(T_C)

        for site in sites:
            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Downwelling PIR Case", code='Tdic')
            trace.data_record = f'{profile}-editing-pyranometertemperature'
            trace.data_field = f'Tdic_{site.instrument_code}'
            edited.traces.append(trace)

            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Downwelling PIR Dome", code='Tdid')
            trace.data_record = f'{profile}-editing-pyranometertemperature'
            trace.data_field = f'Tdid_{site.instrument_code}'
            edited.traces.append(trace)

            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Upwelling PIR Case", code='Tuic')
            trace.data_record = f'{profile}-editing-pyranometertemperature'
            trace.data_field = f'Tdic_{site.instrument_code}'
            edited.traces.append(trace)

            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Upwelling PIR Dome", code='Tuid')
            trace.data_record = f'{profile}-editing-pyranometertemperature'
            trace.data_field = f'Tdid_{site.instrument_code}'
            edited.traces.append(trace)

            trace = SolarTimeSeries.Trace(T_C)
            trace.legend = self._EDITED_FORMAT.format(site=site, parameter="Air Temperature", code='T')
            trace.data_record = f'{profile}-editing-pyranometertemperature'
            trace.data_field = f'T_{site.instrument_code}'
            edited.traces.append(trace)


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

    def __init__(self, latitude: float, longitude: float, profile: str, sites: typing.List[Site], **kwargs):
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

        for site in sites:
            trace = SolarTimeSeries.Trace(albedo)
            trace.legend = f"{site.name} Raw"
            trace.data_record = f'{profile}-raw-albedo-{site.instrument_code.lower()}'
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

        for site in sites:
            trace = SolarTimeSeries.Trace(albedo)
            trace.legend = f"{site.name} Edited"
            trace.data_record = f'{profile}-editing-albedo-{site.instrument_code.lower()}'
            trace.data_field = 'albedo'
            edited.traces.append(trace)
            self.processing[trace.data_record] = self.CalculateAlbedo()


class EditingTotalRatio(SolarTimeSeries):
    class CalculateSPN1(SolarTimeSeries.Processing):
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

    def __init__(self, latitude: float, longitude: float, profile: str, sites: typing.List[Site], **kwargs):
        super().__init__(latitude, longitude, **kwargs)
        self.title = "Total Ratio"

        raw = SolarTimeSeries.Graph()
        raw.title = "Raw"
        raw.contamination = f'{profile}-raw-contamination'
        self.graphs.append(raw)

        ratio = SolarTimeSeries.Axis()
        ratio.title = "Total/Global"
        ratio.format_code = '.3f'
        raw.axes.append(ratio)

        for site in sites:
            trace = SolarTimeSeries.Trace(ratio)
            trace.legend = f"{site.name} Raw"
            trace.data_record = f'{profile}-raw-totalratio-{site.instrument_code.lower()}'
            trace.data_field = 'ratio'
            raw.traces.append(trace)
            if site.include_spn1:
                self.processing[trace.data_record] = self.CalculateSPN1()
            else:
                self.processing[trace.data_record] = self.CalculateDirectDiffuse()


        edited = SolarTimeSeries.Graph()
        edited.title = "Edited"
        edited.contamination = f'{profile}-editing-contamination'
        self.graphs.append(edited)

        ratio = SolarTimeSeries.Axis()
        ratio.title = "Total/Global"
        ratio.format_code = '.3f'
        edited.axes.append(ratio)

        for site in sites:
            trace = SolarTimeSeries.Trace(ratio)
            trace.legend = f"{site.name} Edited"
            trace.data_record = f'{profile}-editing-totalratio-{site.instrument_code.lower()}'
            trace.data_field = 'ratio'
            edited.traces.append(trace)
            if site.include_spn1:
                self.processing[trace.data_record] = self.CalculateSPN1()
            else:
                self.processing[trace.data_record] = self.CalculateDirectDiffuse()
