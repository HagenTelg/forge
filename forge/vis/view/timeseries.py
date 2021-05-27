import typing
from collections import OrderedDict
from starlette.responses import HTMLResponse
from forge.vis.util import package_template
from . import View, Request, Response


class TimeSeries(View):
    class Axis:
        def __init__(self):
            self.title: typing.Optional[str] = None
            self.range: typing.Optional[typing.Union[int, typing.Tuple[float, float]]] = None
            self.logarithmic = False
            self.format_code: typing.Optional[str] = None

    class Trace:
        def __init__(self, axis: "TimeSeries.Axis"):
            self.axis: TimeSeries.Axis = axis
            self.legend = ""
            self.data_record: typing.Optional[str] = None
            self.data_field: typing.Optional[str] = None
            self.format_code: typing.Optional[str] = None

        def hover_template(self):
            fmt = self.format_code
            if not fmt:
                fmt = self.axis.format_code
            if not fmt:
                return '%{y}'
            else:
                return '%{y:' + fmt + '}'

    class Graph:
        def __init__(self):
            self.title: typing.Optional[str] = None
            self.axes: typing.List[TimeSeries.Axis] = []
            self.traces: typing.List[TimeSeries.Trace] = []

    class Processing:
        def __init__(self):
            self.components: typing.List[str] = []
            self.script = str()

    def __init__(self):
        super().__init__()
        self.title: typing.Optional[str] = None
        self.graphs: typing.List[TimeSeries.Graph] = []
        self.processing: typing.Dict[str, TimeSeries.Processing] = dict()

    @staticmethod
    def _index_code(index, base: str) -> str:
        if index == 0:
            return base
        return base + str(index + 1)

    def axis_code(self, find_axis: "TimeSeries.Axis", base: str = 'y') -> str:
        # Since we can only do overlaying axes last, the numbering is a bit odd
        index = 0
        for graph in self.graphs:
            if len(graph.axes) == 0:
                continue
            if graph.axes[0] == find_axis:
                return self._index_code(index, base)
            index += 1
        for graph in self.graphs:
            for axis_index in range(1, len(graph.axes)):
                if graph.axes[axis_index] == find_axis:
                    return self._index_code(index, base)
                index += 1
        raise KeyError

    def graph_code(self, find_graph: "TimeSeries.Graph", base: str = 'x') -> str:
        index = 0
        for graph in self.graphs:
            if graph == find_graph:
                return self._index_code(index, base)
            index += 1
        raise KeyError

    def required_components(self) -> typing.List[str]:
        components = OrderedDict()
        for processing in self.processing.values():
            for name in processing.components:
                components[name] = True
        return list(components.keys())

    async def __call__(self, request: Request, **kwargs) -> Response:
        return HTMLResponse(await package_template('view', 'timeseries.html').render_async(
            request=request,
            view=self,
            **kwargs
        ))
