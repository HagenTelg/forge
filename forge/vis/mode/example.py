from . import Request, Response
from .viewlist import ViewList, Realtime, Editing
from .acquisition import Acquisition


class _ExampleViewList(ViewList):
    def __init__(self, suffix=''):
        super().__init__('example-basic'+suffix, "Example View List")

        self.views.append(ViewList.Entry('example-timeseries-1', "First View"))
        self.views.append(ViewList.Entry('example-timeseries-2', "Second View"))
        self.views.append(ViewList.Entry('example-timeseries-3', "Third View"))

        for i in range(4, 10):
            self.views.append(ViewList.Entry(f'example-timeseries-{i}', f"View {i}"))


example_view_list = _ExampleViewList()
example_view_list2 = _ExampleViewList('2')
example_view_list3 = _ExampleViewList('3')


class _ExampleEditing(Editing):
    def __init__(self):
        super().__init__('example-editing', "Example Editing")

        self.views.append(ViewList.Entry('example-timeseries-1', "First Editing"))
        self.views.append(ViewList.Entry('example-timeseries-2', "Second Editing"))


example_editing = _ExampleEditing()


class _ExampleSolar(Editing):
    def __init__(self):
        super().__init__('example-solar', "Example Solar")

        self.views.append(ViewList.Entry('example-solartimeseries-1', "First Solar"))
        self.views.append(ViewList.Entry('example-solartimeseries-2', "Second Solar"))
        self.views.append(ViewList.Entry('example-solarposition', "Solar Position"))


example_solar = _ExampleSolar()


class _ExampleRealtime(Realtime):
    def __init__(self):
        super().__init__('example-realtime', "Example Realtime")

        self.views.append(ViewList.Entry('example-realtime-1', "First View"))
        self.views.append(ViewList.Entry('example-realtime-2', "Second View"))


example_realtime = _ExampleRealtime()


class _ExampleAcquisition(Acquisition):
    def __init__(self):
        super().__init__('example-acquisition', "Example Acquisition")

        self.summary_instrument.append(Acquisition.SummaryInstrument('example-instrument', 'example_neph'))
        self.summary_instrument.append(Acquisition.SummaryInstrument('example-instrument', 'example_neph', 'S11'))

        item = Acquisition.SummaryStatic('example-static')
        item.priority = -1000
        self.summary_static.append(item)

        self.display_instrument.append(Acquisition.DisplayInstrument('example-instrument', 'example_neph'))
        self.display_instrument.append(Acquisition.DisplayInstrument('example-instrument', 'example_neph', 'S11'))
        self.display_static.append(Acquisition.DisplayStatic('example-spancheck'))

    async def __call__(self, request: Request, **kwargs) -> Response:
        return await super().__call__(
            request,
            socket_url=request.url_for('acquisition_example_socket', station=kwargs.get('station', 'nil')),
            **kwargs)


example_acquisition = _ExampleAcquisition()
