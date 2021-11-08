import typing
from starlette.requests import Request
from starlette.responses import Response, FileResponse
from forge.vis.util import package_data
from . import Export, ExportList


class ExampleExport(Export):
    async def __call__(self) -> typing.Optional[Export.Result]:
        return Export.DirectResult(package_data('static', 'example', 'timeseries.csv'),
                                   client_name='timeseries.csv', media_type='text/csv')


class ExampleExportList(ExportList):
    def __init__(self):
        super().__init__()

        self.exports.append(self.Entry('example-key-1', "First export example"))
        self.exports.append(self.Entry('example-key-2', "Second export example"))
