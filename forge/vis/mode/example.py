from .viewlist import ViewList, Editing


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
