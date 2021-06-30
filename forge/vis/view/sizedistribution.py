import typing
from collections import OrderedDict
from starlette.responses import HTMLResponse
from forge.vis.util import package_template
from . import View, Request, Response


class SizeDistribution(View):
    class ScatteringWavelength:
        def __init__(self, wavelength: float, field_name: str):
            self.wavelength = wavelength

            self.measured_color: typing.Optional[str] = None
            self.measured_field: typing.Optional[str] = field_name

            self.calculated_color: typing.Optional[str] = None
            self.calculated_field: typing.Optional[str] = field_name

    def __init__(self):
        super().__init__()
        self.title: typing.Optional[str] = None
        self.scattering_wavelengths: typing.List[SizeDistribution.ScatteringWavelength] = list()
        self.contamination: typing.Optional[str] = None

        self.size_record: typing.Optional[str] = None
        self.measured_record: typing.Optional[str] = None

        blue = self.ScatteringWavelength(450, 'BsB')
        blue.measured_color = '#00f'
        blue.calculated_color = '#007'
        self.scattering_wavelengths.append(blue)

        green = self.ScatteringWavelength(550, 'BsG')
        green.measured_color = '#0f0'
        green.calculated_color = '#070'
        self.scattering_wavelengths.append(green)

        red = self.ScatteringWavelength(700, 'BsR')
        red.measured_color = '#f00'
        red.calculated_color = '#700'
        self.scattering_wavelengths.append(red)

    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('view', 'sizedistribution.html').render_async(
            request=request,
            view=self,
            **kwargs
        ))


class SizeCounts(View):
    class Trace:
        def __init__(self):
            self.legend = ""
            self.color: typing.Optional[str] = None
            self.data_record: typing.Optional[str] = None
            self.data_field: typing.Optional[str] = None
            self.script_incoming_data: typing.Optional[str] = None

    class Processing:
        def __init__(self):
            self.components: typing.List[str] = []
            self.script = str()

    class IntegrateSizeDistribution(Processing):
        def __init__(self, total_concentration: typing.Optional[str] = None,
                     concentration_above: typing.Optional[str] = None,
                     above_diameter: float = 0.014):
            super().__init__()
            self.components = ['size_distribution']
            self.script = r"""(function(dataName) {
const outputConcentrations = new Map();
"""
            if total_concentration:
                self.script += f"outputConcentrations.set('{total_concentration}', true);\n"
            if concentration_above:
                self.script += f"outputConcentrations.set('{concentration_above}', [{above_diameter}, undefined]);\n"
            self.script += r"""
return new SizeDistribution.ConcentrationDispatch(dataName, outputConcentrations);
    })"""

    def __init__(self):
        super().__init__()
        self.title: typing.Optional[str] = None
        self.contamination: typing.Optional[str] = None
        self.traces: typing.List[SizeCounts.Trace] = []
        self.processing: typing.Dict[str, SizeCounts.Processing] = dict()
        self.size_record: typing.Optional[str] = None

    def required_components(self) -> typing.List[str]:
        components = OrderedDict()
        if self.contamination:
            components['contamination'] = True
        for processing in self.processing.values():
            for name in processing.components:
                components[name] = True
        return list(components.keys())

    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('view', 'sizecounts.html').render_async(
            request=request,
            view=self,
            **kwargs
        ))
