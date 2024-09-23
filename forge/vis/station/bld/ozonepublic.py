import typing
from forge.vis.view.timeseries import PublicTimeSeries


class PublicOzoneConcentration(PublicTimeSeries):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.title = "Ozone Concentration"

        concentration = PublicTimeSeries.Graph()
        self.graphs.append(concentration)

        ppb = PublicTimeSeries.Axis()
        ppb.title = "ppb"
        ppb.format_code = '.2f'
        concentration.axes.append(ppb)

        ozone = PublicTimeSeries.Trace(ppb)
        ozone.legend = "Ozone"
        ozone.data_record = f'public-realtime-ozone'
        ozone.data_field = 'ozone'
        concentration.traces.append(ozone)

    @property
    def height(self):
        return None
