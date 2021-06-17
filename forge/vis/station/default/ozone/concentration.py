import typing
from forge.vis.view.timeseries import TimeSeries


class OzoneConcentration(TimeSeries):
    def __init__(self, mode: str):
        super().__init__()
        self.title = "Ozone Concentration"

        concentration = TimeSeries.Graph()
        concentration.contamination = f'{mode}-contamination'
        self.graphs.append(concentration)

        ppb = TimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        concentration.axes.append(ppb)

        ozone = TimeSeries.Trace(ppb)
        ozone.legend = "Ozone"
        ozone.data_record = f'{mode}-ozone'
        ozone.data_field = 'ozone'
        concentration.traces.append(ozone)
