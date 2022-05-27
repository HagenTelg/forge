import typing
from forge.vis.view.timeseries import TimeSeries


class FilterStatus(TimeSeries):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "PMEL Filter Carousel Status"

        status = TimeSeries.Graph()
        self.graphs.append(status)

        hPa = TimeSeries.Axis()
        hPa.title = "hPa"
        hPa.format_code = '.1f'
        status.axes.append(hPa)

        filter_index = TimeSeries.Axis()
        filter_index.title = "Active Index"
        filter_index.range = [0, 9]
        status.axes.append(filter_index)

        active = TimeSeries.Trace(filter_index)
        active.legend = "Active Filter"
        active.data_record = f'{mode}-filterstatus'
        active.data_field = 'Fn'
        status.traces.append(active)

        for i in range(8):
            dP = TimeSeries.Trace(hPa)
            dP.legend = f"Pd_P{i+1}"
            dP.data_record = f'{mode}-filterstatus'
            dP.data_field = f'Pd{i+1}'
            status.traces.append(dP)


class SecondFilterStatus(FilterStatus):
    def __init__(self, mode: str, **kwargs):
        super().__init__(**kwargs)
        self.title = "SCRIPPS Filter Carousel Status"

        for g in self.graphs:
            for t in g.traces:
                t.data_record += '2'
